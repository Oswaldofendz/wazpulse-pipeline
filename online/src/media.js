import {store} from './store.js';
import {network, telegram, ServiceError} from './io.js';
import {visualBrief, visualHtml, visualDimensions, xPublication} from './publication.js';
import {createStorage} from './storage.js';
import {socialKit, socialText, socialHtml, socialDimensions} from './social.js';
import {trump} from './portraits.js';
import {shareXUrl,xButtons} from './share-x.js';
import {CHART_PIXEL} from './market-graph.js';

const limit = value => /^([0-6])$/.test(String(value)) ? Number(value) : 3;
const enabled = env => env.PIPELINE_ENABLED === 'true' && env.MEDIA_ENABLED === 'true';
export function createMedia(env, {fetcher = fetch, now = Date.now, render, generate} = {}) {
  const db = store(env.DB), bot = telegram(env, network(fetcher));
  const storage = createStorage(env, {fetcher, now});
  const getJob = id => db.first('SELECT * FROM visual_jobs WHERE story_id=?', id);

  async function enqueue(id) {
    if (!enabled(env)) return false;
    const inserted = await db.run("INSERT OR IGNORE INTO visual_jobs(story_id,created_at) SELECT id,? FROM stories WHERE id=? AND state='approved'", now(), id);
    return Boolean(inserted.meta?.changes);
  }

  async function status(id) {
    const job = await getJob(id);
    if (!job) return null;
    const assets = await db.all('SELECT position,filename,delivery FROM visual_assets WHERE story_id=? ORDER BY position', id);
    return {id, state: job.state, error: job.error, assets, createdAt: job.created_at};
  }

  async function generateImage(brief) {
    if(brief.art?.mediaKind==='market-chart')return CHART_PIXEL;
    if(brief.art?.mediaKind==='official-portraits')return trump;
    if (generate) return generate(brief);
    if (!env.AI) throw new ServiceError('ai-binding');
    const response = await env.AI.run(brief.model, {prompt: brief.prompt, steps: brief.steps});
    const data = response?.image;
    if (typeof data !== 'string' || !/^[A-Za-z0-9+/=]+$/.test(data) || data.length > 2_400_000 || data.length < 100) throw new ServiceError('ai-image');
    return data;
  }

  async function renderImage(html, dimensions) {
    if (render) return render(html, dimensions);
    if (!env.BROWSER) throw new ServiceError('browser-binding');
    const {launch} = await import('@cloudflare/playwright');
    const browser = await launch(env.BROWSER, {keep_alive: 10_000});
    try {
      const page = await browser.newPage({viewport: dimensions, deviceScaleFactor: 1});
      await page.route('**/*', route => route.request().url().startsWith('data:') || route.request().url() === 'about:blank' ? route.continue() : route.abort());
      await page.setContent(html, {waitUntil: 'load', timeout: 15_000});
      const fits = await page.evaluate(async () => {
        await document.fonts.ready;
        await Promise.all([...document.images].map(img => img.decode()));
        return document.documentElement.dataset.fits === 'true';
      });
      if (!fits) throw new ServiceError('layout-overflow');
      return await page.screenshot({type: 'png'});
    } finally { await browser.close(); }
  }

  async function deliverAsset(asset, brief) {
    // Claim before the external send. An ambiguous response needs an explicit
    // owner retry; automatic runs never resend an uncertain Telegram document.
    const claimed = await db.first("UPDATE visual_assets SET delivery='sending' WHERE story_id=? AND position=? AND delivery='new' RETURNING *", asset.story_id, asset.position);
    if (!claimed) return false;
    try {
      const dims = visualDimensions(brief, asset.position);
      const caption = asset.position === 0 ? '🖼 Portada para X · ' + dims.width + ' × ' + dims.height + ' · Ilustración con IA\n' + brief.title :
        '📚 Carrusel · ' + asset.position + '/5 · 1080 × 1350 · Ilustración con IA';
      const result = await bot.imageDocument(new Uint8Array(claimed.png), asset.filename, caption);
      if (!result?.document?.file_id || !Number.isSafeInteger(result.message_id)) throw new ServiceError('telegram-image');
      // Telegram retains the original PNG; keep its file ID and remove only
      // this temporary upload buffer, never the source or historical content.
      await db.run("UPDATE visual_assets SET delivery='sent',file_id=?,message_id=?,png=NULL WHERE story_id=? AND position=?", result.document.file_id, String(result.message_id), asset.story_id, asset.position);
      return true;
    } catch {
      await db.run("UPDATE visual_assets SET delivery='uncertain' WHERE story_id=? AND position=? AND delivery='sending'", asset.story_id, asset.position);
      return false;
    }
  }

  async function step() {
    if (!enabled(env)) return {state: 'disabled'};
    const owner = crypto.randomUUID(), instant = now();
    if (!await db.lock('visual', owner, instant, 90_000)) return {state: 'busy'};
    let job, stage = 'queue';
    try {
      job = await db.first("SELECT * FROM visual_jobs WHERE state IN ('queued','rendering') AND lease_until<=? ORDER BY CASE WHEN state='rendering' THEN 0 ELSE 1 END,created_at LIMIT 1", instant);
      if (!job) return {state: 'idle'};
      const story = await db.first("SELECT * FROM stories WHERE id=? AND state='approved'", job.story_id);
      if (!story) throw new ServiceError('approval-required');
      if (job.state === 'queued') {
        stage = 'generation';
        if (!await storage.allow()) return {state: 'storage-paused', id: job.story_id};
        if (!env.AI && !generate || !env.BROWSER && !render) throw new ServiceError('media-binding');
        const day = new Date(instant).toISOString().slice(0, 10);
        await db.run('INSERT OR IGNORE INTO visual_budget(day,used) VALUES(?,0)', day);
        if (!await db.first('UPDATE visual_budget SET used=used+1 WHERE day=? AND used<? RETURNING used', day, limit(env.DAILY_VISUAL_LIMIT))) {
          return {state: 'daily-limit', id: job.story_id};
        }
        // Count attempts before AI. A crash cannot run up the free quota.
        await db.run('UPDATE visual_jobs SET attempts=attempts+1,lease_until=? WHERE story_id=?', instant + 120_000, job.story_id);
        if (job.attempts >= 2) throw new ServiceError('generation-attempts');
        const priorJobs = await db.all('SELECT manifest FROM visual_jobs WHERE manifest IS NOT NULL ORDER BY created_at DESC,story_id DESC LIMIT 3');
        const recent = priorJobs.flatMap(row => {try {return [JSON.parse(row.manifest)];} catch {return [];}});
        const brief = visualBrief(story, {recent}), image = await generateImage(brief);
        // HTML validation rejects malformed/oversize image payloads, too.
        visualHtml(brief, image);
        await db.run("UPDATE visual_jobs SET state='rendering',manifest=?,image=?,lease_until=0,error=NULL WHERE story_id=?", JSON.stringify(brief), image, job.story_id);
        return {state: 'image-created', id: job.story_id};
      }
      const brief = JSON.parse(job.manifest);
      const assets = await db.all('SELECT * FROM visual_assets WHERE story_id=? ORDER BY position', job.story_id);
      if (assets.some(x => ['sending','uncertain'].includes(x.delivery))) {
        await db.run("UPDATE visual_jobs SET state='blocked',error='delivery-uncertain' WHERE story_id=?", job.story_id);
        return {state: 'delivery-needs-review', id: job.story_id};
      }
      for (let position = 0; position <= 5; position++) {
        let asset = assets.find(x => x.position === position);
        if (asset?.delivery === 'sent') continue;
        stage = 'render-' + position;
        if (!asset) {
          if (!await storage.allow(1_900_000)) return {state: 'storage-paused', id: job.story_id};
          const last = await db.first("SELECT value FROM runtime WHERE key='visual_last_browser'");
          if (last && instant - JSON.parse(last.value) < 25_000) return {state: 'browser-cooldown', id: job.story_id};
          await db.set('visual_last_browser', instant);
          const html = visualHtml(brief, job.image, position - 1);
          const png = new Uint8Array(await renderImage(html, visualDimensions(brief, position)));
          if (png.length < 8 || png.length > 1_900_000 || ![137,80,78,71,13,10,26,10].every((n, i) => png[i] === n)) throw new ServiceError('png-size-or-format');
          const filename = 'WaCapital-' + story.id.slice(0, 8) + (position === 0 ? '-X.png' : '-carrusel-' + position + '.png');
          await db.run('INSERT INTO visual_assets(story_id,position,filename,png) VALUES(?,?,?,?)', story.id, position, filename, png.buffer.slice(png.byteOffset, png.byteOffset + png.byteLength));
          asset = await db.first('SELECT * FROM visual_assets WHERE story_id=? AND position=?', story.id, position);
        }
        stage = 'delivery-' + position;
        const delivered = await deliverAsset(asset, brief);
        if (delivered) await db.run('UPDATE visual_jobs SET error=NULL,lease_until=0 WHERE story_id=?', story.id);
        else await db.run("UPDATE visual_jobs SET state='blocked',error='delivery-uncertain' WHERE story_id=?", story.id);
        return {state: delivered ? 'asset-delivered' : 'delivery-needs-review', id: job.story_id, position};
      }
      stage = 'final-review';
      await storage.delivered(story.id);
      // A lost response here must not create duplicate approval messages.
      await db.run("UPDATE visual_jobs SET state='review',error='review-sending' WHERE story_id=? AND state='rendering'", story.id);
      const result = await bot.send('✅ Material visual preparado\n\n' + brief.x.text +
        '\n\nX: ' + (brief.x.ready === false ? 'el texto completo necesita edición.' : brief.x.weightedLength + '/280 caracteres ponderados.') + '\nPortada y 5 láminas enviadas como PNG originales. Revisa imagen, nombres, cifras y fuente.\n\nAprobar aquí confirma esta versión; no publica en redes.',
      {inline_keyboard: [[{text: '✅ Aprobar versión final', callback_data: 'visualok:' + story.id},
        {text: '❌ Descartar imágenes', callback_data: 'visualno:' + story.id}]]});
      if (!Number.isSafeInteger(result?.message_id)) throw new ServiceError('telegram-review');
      await db.run('UPDATE visual_jobs SET review_message=?,error=NULL WHERE story_id=?', String(result.message_id), story.id);
      return {state: 'awaiting-visual-approval', id: story.id};
    } catch (error) {
      const detail = String(error?.message || '');
      const hint = /429|rate.limit|too many/i.test(detail) ? 'rate-limit' : /time limit exceeded|daily.*limit/i.test(detail) ? 'daily-quota' :
        /timeout|timed out/i.test(detail) ? 'timeout' : /403|unauthor/i.test(detail) ? 'access' : 'unavailable';
      const category = error instanceof ServiceError ? error.service : stage + '-' + hint;
      if (job) {
        // Browser capacity/rate failures resume later using the saved AI image.
        // Bound the retry count; permanent layout or format failures stop.
        const prior = /^retry:([^:]+):(\d+)$/.exec(job.error || '');
        const attempt = prior?.[1] === stage ? Number(prior[2]) + 1 : 1;
        if (stage.startsWith('render-') && !(error instanceof ServiceError) && attempt <= 3) {
          await db.run("UPDATE visual_jobs SET state='rendering',error=?,lease_until=? WHERE story_id=?", 'retry:' + stage + ':' + attempt, instant + 120_000 * attempt, job.story_id);
          return {state: 'render-deferred', id: job.story_id, category};
        }
        await db.run("UPDATE visual_jobs SET state='blocked',error=?,lease_until=0 WHERE story_id=?", category, job.story_id);
        const reason = category === 'layout-overflow' ? 'El texto necesita un ajuste para caber en el diseño.' :
          category === 'png-size-or-format' ? 'La imagen no cumple el formato de entrega.' : 'El servicio de imágenes no pudo completar esta preparación.';
        await bot.send('⚠️ No se completaron las imágenes. El texto sigue disponible.\n' + reason + '\nReferencia: ' + job.story_id + '\nNo se ha activado ningún servicio de pago.').catch(() => {});
      }
      return {state: 'blocked', id: job?.story_id, stage, category};
    } finally { await db.unlock('visual', owner); }
  }

  async function review(id, messageId, approved) {
    const row = await db.first("UPDATE visual_jobs SET state=?,approved_at=? WHERE story_id=? AND state='review' AND review_message=? RETURNING *", approved ? 'approved' : 'rejected', approved ? now() : null, id, String(messageId));
    if (!row) return false;
    if (approved) {
      await prepareX(id);
    } else await bot.send('Imágenes descartadas. No se ha publicado nada.');
    return true;
  }

  async function prepareX(id) {
    const job = await getJob(id);
    if (job?.state !== 'approved') return false;
    const story = await db.first("SELECT * FROM stories WHERE id=? AND state='approved'", id);
    const cover = await db.first("SELECT * FROM visual_assets WHERE story_id=? AND position=0 AND delivery='sent' AND length(file_id)>0", id);
    if (!story || !cover) return false;
    const x = xPublication(story), brief = JSON.parse(job.manifest), ref = id.slice(0,8);
    const shareUrl=await shareXUrl(env,story,cover,'l',now());
    // Re-send the exact approved cover by file_id, never another story's latest
    // file or the source website's preview. This does not generate another image.
    await bot.imageDocument(cover.file_id, cover.filename,
      '🖼 PORTADA PARA ESTE BORRADOR · ' + ref + '\n' + brief.title + '\nGuarda este PNG y adjúntalo en X.');
    await bot.send('📦 Borrador X · ' + ref + '\n\n' + x.text +
      '\n\n' + (x.ready ? 'Texto completo: ' + x.weightedLength + '/280 caracteres ponderados.' : 'Este texto necesita edición antes de abrir X; no se ha cortado.') +
      (shareUrl?'\nComparte texto y portada desde el móvil. Elige X y comprueba que recibió ambos antes de publicar. La opción de solo texto requiere adjuntar el PNG manualmente.':'\n1. Guarda la portada PNG anterior, referencia ' + ref + '.\n2. Abre el borrador y adjunta ese archivo con el botón de fotos de X.\n3. Revisa texto e imagen antes de publicar.\nEl botón precarga solo texto; no adjunta imágenes ni publica.') +
      '\n\nFuente conservada para revisión: ' + story.source_url + '\nRecuperar este conjunto: /publicar ' + id,
      xButtons(x,shareUrl));
    return true;
  }

  async function resend(id) {
    const job = await getJob(id);
    if (!job || job.state === 'rejected') return false;
    const brief = JSON.parse(job.manifest);
    const assets = await db.all('SELECT * FROM visual_assets WHERE story_id=? ORDER BY position', id);
    for (const asset of assets) {
      if (asset.file_id) await bot.imageDocument(asset.file_id, asset.filename, 'WaCapital · Copia original · Ilustración con IA');
      else if (asset.png && ['sending', 'uncertain'].includes(asset.delivery)) {
        await db.run("UPDATE visual_assets SET delivery='new' WHERE story_id=? AND position=?", id, asset.position);
        await deliverAsset(asset, brief);
      }
    }
    const uncertain = await db.first("SELECT COUNT(*) AS n FROM visual_assets WHERE story_id=? AND delivery IN ('sending','uncertain')", id);
    if (job.state === 'blocked' && job.error === 'delivery-uncertain' && uncertain.n === 0) await db.run("UPDATE visual_jobs SET state='rendering',error=NULL,lease_until=0 WHERE story_id=?", id);
    if (['review', 'blocked'].includes(job.state) && !job.review_message && assets.length === 6 && assets.every(x => x.delivery === 'sent')) await db.run("UPDATE visual_jobs SET state='rendering',error=NULL WHERE story_id=?", id);
    return assets.length > 0;
  }
  async function retry(id) {
    const row = await getJob(id);
    if (!row || row.state !== 'blocked' || !row.image || row.error === 'layout-overflow' || row.error === 'png-size-or-format') return false;
    await db.run("UPDATE visual_jobs SET state='rendering',error=NULL,lease_until=0 WHERE story_id=? AND state='blocked'", id);
    return true;
  }
  async function preview(id, position = 0, {reuseOnly=false, platform=null} = {}) {
    if (!enabled(env)) return {state: 'disabled'};
    const owner = crypto.randomUUID(), instant = now();
    if (!await db.lock('visual', owner, instant, 90_000)) return {state: 'busy'};
    try {
      const story = await db.first("SELECT * FROM stories WHERE id=? AND state='approved'", id);
      if (!story) return {state: 'approval-required'};
      const last = await db.first("SELECT value FROM runtime WHERE key='visual_last_browser'");
      if (last && instant - JSON.parse(last.value) < 25_000) return {state: 'browser-cooldown'};
      // Preview has its own cache. It never resets a delivered job, deletes an
      // old image, or changes the approval of a previous version.
      const previewBrief = visualBrief(story);
      const promptHash = [...new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(previewBrief.prompt)))].slice(0,8).map(n => n.toString(16).padStart(2,'0')).join('');
      let key = 'visual_preview:' + previewBrief.style + ':' + id + ':' + promptHash;
      const cached = await db.first('SELECT value FROM runtime WHERE key=?', key);
      let saved = cached ? JSON.parse(cached.value) : null;
      if (!saved && reuseOnly) {
        const prior=await db.first('SELECT key,value FROM runtime WHERE key GLOB ? ORDER BY rowid DESC LIMIT 1','visual_preview:impact-v3:'+id+':*');
        if(prior){key=prior.key;saved=JSON.parse(prior.value);}
        else {
          const job=await getJob(id);
          if(job?.image){saved={image:job.image};key=null;}
        }
        if(!saved) return {state:'image-unavailable'};
      }
      if (!saved) {
        if (!await storage.allow()) return {state: 'storage-paused'};
        const day = new Date(instant).toISOString().slice(0, 10);
        if(!['official-portraits','market-chart'].includes(previewBrief.art?.mediaKind)) {
        await db.run('INSERT OR IGNORE INTO visual_budget(day,used) VALUES(?,0)', day);
        if (!await db.first('UPDATE visual_budget SET used=used+1 WHERE day=? AND used<? RETURNING used', day, limit(env.DAILY_VISUAL_LIMIT))) return {state: 'daily-limit'};
        }
        const brief = previewBrief;
        saved = {brief, image: await generateImage(brief)};
        visualHtml(brief, saved.image);
        await db.set(key, saved);
        await storage.rememberPreview(key);
      }
      else {
        if(key) await storage.rememberPreview(key);
        // Typography-only corrections reuse the illustration, even at the daily cap.
        saved = {...saved, brief: previewBrief};
      }
      await db.set('visual_last_browser', instant);
      const html=platform?socialHtml(socialKit(story),saved.image,position-1,platform):visualHtml(saved.brief,saved.image,position-1);
      const dimensions=platform?socialDimensions(platform):visualDimensions(saved.brief,position);
      const png = new Uint8Array(await renderImage(html,dimensions));
      if (png.length < 8 || png.length > 1_900_000 || ![137,80,78,71,13,10,26,10].every((n,i) => png[i] === n)) throw new ServiceError('png-size-or-format');
      return {state: 'preview', png};
    } finally { await db.unlock('visual', owner); }
  }

  async function social(id) {
    if(!enabled(env)) return null;
    const story=await db.first("SELECT * FROM stories WHERE id=? AND state='approved'",id);
    return story?socialKit(story):null;
  }
  async function sendSocial(id) {
    const kit=await social(id);
    if(!kit) return false;
    await bot.document(socialText(kit),'WaCapital-'+id.slice(0,8)+'-redes-borrador.txt');
    return true;
  }
  async function deliverPreview(id,platform='x',position=0) {
    if(!enabled(env))return {state:'disabled'};
    if(!['x','instagram','tiktok'].includes(platform)||!Number.isInteger(position)||position<0||position>(platform==='x'?0:4))return {state:'invalid-selection'};
    const kit=await social(id);
    if(!kit)return {state:'approval-required'};
    const key='social_delivery:v1:'+id+':'+platform+':'+position;
    const prior=await db.first('SELECT value FROM runtime WHERE key=?',key);
    if(prior)return {state:JSON.parse(prior.value).state==='sent'?'already-delivered':'delivery-needs-review'};
    const result=await preview(id,platform==='x'?0:position+1,{reuseOnly:true,platform:platform==='x'?null:platform});
    if(result.state!=='preview')return result;
    const claimed=await db.run('INSERT OR IGNORE INTO runtime(key,value) VALUES(?,?)',key,JSON.stringify({state:'sending',at:now()}));
    if(!claimed.meta?.changes)return {state:'delivery-needs-review'};
    try {
      const ref=id.slice(0,8), stamp=kit.source.publishedAt.slice(0,10);
      const sent=await bot.imageDocument(result.png,'WaCapital-'+ref+'-'+platform+'-'+(position+1)+'-borrador.png',
        '🧪 PRUEBA · '+platform.toUpperCase()+' · '+(position+1)+(platform==='x'?'':'/5')+' · '+ref+'\n'+kit.title+'\nFecha original: '+stamp+'. Diseño actualizado; requiere revisión.');
      if(!sent?.document?.file_id)throw new ServiceError('telegram-image');
      await db.set(key,{state:'sent',at:now(),fileId:sent.document.file_id});
      if(platform==='x')await bot.send('🧪 PRUEBA DE FORMATO · '+ref+'\nNoticia del '+stamp+'; se reutiliza una ilustración guardada, no es una alerta nueva.\n\n'+kit.x.text+
        '\n\nGuarda el PNG de esta misma referencia y adjúntalo en X. El botón solo precarga texto. Revisa el material antes de publicar.\nFuente: '+kit.source.url,
        kit.x.composeUrl?{inline_keyboard:[[{text:'✍️ Revisar texto en X · adjuntar PNG',url:kit.x.composeUrl}]]}:undefined);
      return {state:'delivered',platform,position,reference:ref};
    } catch (error) {
      const category=error instanceof ServiceError?error.service:'delivery';
      const status=error instanceof ServiceError?error.status:0;
      await db.set(key,{state:'uncertain',at:now(),category,status});
      return {state:'delivery-needs-review',category,status};
    }
  }
  return {enqueue, step, status, review, resend, retry, preview, prepareX, social, sendSocial, deliverPreview};
}
