import {store} from './store.js';
import {network, telegram, ServiceError} from './io.js';
import {visualBrief, visualHtml, visualDimensions, selectedXText, xDraftOptions, xPublication} from './publication.js';
import {socialKit, socialHtml, socialDimensions} from './social.js';
import {createImages} from './image-providers.js';
import {createStorage} from './storage.js';
import {shareXUrl,xButtons} from './share-x.js';
import {shareTikTokUrl} from './share-tiktok.js';
import {FAST_EDITORIAL_PROVIDER} from './fast-editorial.js';

const platforms={x:'Twitter/X',i:'Instagram',t:'TikTok'};
const names={x:'x',i:'instagram',t:'tiktok'};
const codes={x:'x',instagram:'i',tiktok:'t'};
const xCopyKey = id => 'channel_x_copy:' + id;
const jobWithCopy = "SELECT j.*, (SELECT value FROM runtime WHERE key='channel_x_copy:'||j.story_id AND j.platform='x') AS x_copy FROM channel_jobs j";
const xForJob = (story, job) => xPublication(story, selectedXText(job.x_copy));
export function newsCard(story) {
  const e=JSON.parse(story.editorial);
  const date=new Date(story.published_at).toLocaleDateString('es-ES',{timeZone:'UTC',day:'2-digit',month:'short'});
  if (e.provider === FAST_EDITORIAL_PROVIDER) {
    const time = new Date(story.published_at).toISOString().slice(11, 16);
    return ['🚨 ÚLTIMA HORA · ' + story.source, e.headline, date + ' · ' + time + ' UTC'].join('\n\n');
  }
  return ['📰 '+e.headline,story.source+' · '+date+(e.marketSnapshot?' · '+new Date(e.marketSnapshot.at).toISOString().slice(11,16)+' UTC':'')].join('\n\n');
}
export function newsButtons(id, sourceUrl) {
  return {inline_keyboard:[[{text:'Aprobar para Twitter/X',callback_data:'pickx:'+id}],
    [{text:'Aprobar para TikTok',callback_data:'pickt:'+id},{text:'Aprobar para Instagram',callback_data:'picki:'+id}],
    ...(sourceUrl ? [[{text:'Ver fuente',url:sourceUrl}]] : []),
    [{text:'Descartar',callback_data:'reject:'+id}]]};
}
function draftText(story,platform,xCopy=null) {
  const kit=socialKit(story);
  const choices=xCopy ? (xCopy.selected ? xCopy.options.filter(option => option.code === xCopy.selected) : xCopy.options) : [];
  const xText=choices.length ? choices.map(option => {
    const x=xPublication(story,option.text);
    return option.label+'\n'+x.text+'\n'+x.weightedLength+'/280 caracteres ponderados';
  }).join('\n\n') : kit.x.text+'\n\n'+kit.x.weightedLength+'/280 caracteres ponderados';
  const body=platform==='x' ? xText+'\n\n'+(choices.length>1?'Elige el enfoque para preparar una portada.':'Una portada.') :
    kit[platform].caption+'\n\nCinco láminas:\n'+kit.slides.map((s,i)=>(i+1)+'. '+s.title+'\n'+s.body).join('\n\n');
  const text=['📝 Borrador · '+platforms[codes[platform]],body,
    ...(platform==='x'?['Fuente: '+story.source_url]:[]),
    'Fecha original: '+kit.source.publishedAt.slice(0,10),
    platform==='tiktok'?'Formato: carrusel vertical 1080 × 1920; no vídeo.':platform==='instagram'?'Formato: 1080 × 1350.':'',
    JSON.parse(story.editorial).marketSnapshot?'Base: cotizaciones guardadas con hora de corte. Revisa precios y fecha antes de publicar.':'Base: titular y extracto. Revisa la fuente. No publica en redes.'].filter(Boolean).join('\n\n');
  // Telegram's limit is explicit; preserve complete copy in the on-demand file
  // rather than silently cutting a sentence to fit a chat bubble.
  return text.length<=3900?text:null;
}
export function createChannels(env,{fetcher=fetch,now=Date.now,render,generate}={}) {
  const db=store(env.DB),bot=telegram(env,network(fetcher));
  const images=createImages(env,{fetcher,now,generate}),storage=createStorage(env,{fetcher,now});
  const getJob=(id,p)=>db.first(jobWithCopy+' WHERE j.story_id=? AND j.platform=?',id,p);
  const enabled=()=>env.MEDIA_ENABLED==='true'&&env.PIPELINE_ENABLED==='true';
  async function edit(job,text,buttons) {
    if(!job.draft_message)return false;
    // Editing an existing message is safe to repeat; sending an upload is not.
    try {await bot.edit(job.draft_message,text,buttons);return true;}
    catch{return false;}
  }
  async function editCoverButtons(cover, buttons) {
    if (!cover?.message_id) return true;
    try { await bot.buttons(cover.message_id, buttons); return true; }
    catch { return false; }
  }
  async function showReview(job,story) {
    const x=job.platform==='x'?xForJob(story,job):null;
    const approveLabel=x?'Aprobar y compartir en X':job.platform==='tiktok'?'Aprobar y compartir en TikTok':'Aprobar versión final';
    const approval={text:approveLabel,callback_data:'ok'+codes[job.platform]+':'+job.story_id};
    const success=await edit(job,'✅ '+platforms[codes[job.platform]]+' · Listo para revisar\n\n'+(job.platform==='x'?x.text+'\n\n'+x.weightedLength+'/280 caracteres ponderados.':socialKit(story)[job.platform].caption)+
      (x?'\n\nRevisa el texto y la portada. Pulsa «Aprobar y compartir en X» para habilitar los botones de compartir.':'\n\nRevisa el grupo de cinco láminas recibido. Aprobar confirma el material; no publica.'),
      {inline_keyboard:[[approval],[{text:'Descartar',callback_data:'cancel'+codes[job.platform]+':'+job.story_id}]]});
    const cover=x?await db.first("SELECT message_id FROM channel_assets WHERE story_id=? AND platform='x' AND position=0 AND length(file_id)>0",job.story_id):null;
    const imageSuccess=await editCoverButtons(cover,{inline_keyboard:[[approval]]});
    const saved=await db.run("UPDATE channel_jobs SET error=? WHERE story_id=? AND platform=? AND state='review'",
      success&&imageSuccess?null:'review-edit-pending',job.story_id,job.platform);
    if(x&&!saved.meta?.changes)await refreshX(job.story_id);
  }
  async function refreshX(id) {
    const job=await getJob(id,'x');
    if(job?.state!=='approved'||job.delivery!=='sent')return false;
    const story=await db.first("SELECT * FROM stories WHERE id=? AND state='approved'",id);
    const cover=await db.first("SELECT file_id,filename,message_id FROM channel_assets WHERE story_id=? AND platform='x' AND position=0 AND length(file_id)>0",id);
    if(!story||!cover)return false;
    const x=xForJob(story,job),shareUrl=await shareXUrl(env,story,cover,'c',now(),selectedXText(job.x_copy));
    const buttons=xButtons(x,shareUrl);
    const textSuccess=await edit(job,'✅ Twitter/X · Material aprobado\n\n'+x.text+'\n\n'+x.weightedLength+'/280 · '+
      (shareUrl?'Comparte texto y portada desde el móvil; revisa que X reciba ambos.':'Adjunta la portada recibida al abrir X.')+
      '\n\nFuente: '+story.source_url+'\nNo se ha publicado en redes.',buttons);
    const imageSuccess=await editCoverButtons(cover,buttons);
    const complete=textSuccess&&imageSuccess;
    const attempt=Number(/^share-edit-pending:(\d+)$/.exec(job.error||'')?.[1]||0)+1;
    await db.run("UPDATE channel_jobs SET error=?,lease_until=? WHERE story_id=? AND platform='x' AND state='approved'",
      complete?null:attempt<3?'share-edit-pending:'+attempt:'share-edit-failed',complete?0:now()+60_000,id);
    return complete;
  }
  async function refreshTikTok(id) {
    const job = await getJob(id, 'tiktok');
    if (job?.state !== 'approved' || job.delivery !== 'sent') return false;
    const story = await db.first("SELECT * FROM stories WHERE id=? AND state='approved'", id);
    if (!story) return false;
    const url = await shareTikTokUrl(env, id, now());
    const caption = socialKit(story).tiktok.caption;
    const success = await edit(job, '✅ TikTok · Carrusel aprobado\n\n' + caption +
      '\n\n' + (url ? 'Comparte las cinco láminas y la descripción desde el botón. Con la cuenta conectada, podrás enviarlas como borrador a TikTok y terminar allí.' : 'Puedes cargar las cinco láminas del grupo anterior en TikTok.'), {
      inline_keyboard: url ? [[{text: 'Compartir en TikTok', url}], [{text: 'Renovar enlace', callback_data: 'okt:' + id}]] : []
    });
    const attempt=Number(/^share-edit-pending:(\d+)$/.exec(job.error||'')?.[1]||0)+1;
    await db.run("UPDATE channel_jobs SET error=?,lease_until=? WHERE story_id=? AND platform='tiktok' AND state='approved'",
      success?null:attempt<3?'share-edit-pending:'+attempt:'share-edit-failed',success?0:now()+60_000,id);
    return success;
  }
  async function pick(id,code,messageId) {
    const platform=names[code];
    if(!platform)return false;
    const story=await db.review(id,messageId,true,now())||await db.first("SELECT * FROM stories WHERE id=? AND state='approved' AND message_id=?",id,String(messageId));
    if(!story)return false;
    const inserted=await db.run('INSERT OR IGNORE INTO channel_jobs(story_id,platform,created_at) VALUES(?,?,?)',id,platform,now());
    if(!inserted.meta?.changes)return true;
    await db.run("UPDATE channel_jobs SET delivery='sending' WHERE story_id=? AND platform=?",id,platform);
    try {
      const choices=platform==='x'?xDraftOptions(story):[];
      const xCopy=choices.length?{options:choices,selected:null}:null;
      if(xCopy)await db.set(xCopyKey(id),xCopy);
      const full=draftText(story,platform,xCopy);
      const prepare=platform==='x'?choices.map(option => [{
        text: choices.length===1?'Preparar portada':'Portada · '+option.label, callback_data:'gen'+option.code+':'+id
      }]):[[{text:'Preparar 5 láminas',callback_data:'gen'+code+':'+id}]];
      const keyboard={inline_keyboard:[...(enabled()?prepare:[]),
        [{text:'Ver texto completo y fuente',callback_data:'info'+code+':'+id}],
        [{text:'Cancelar este formato',callback_data:'cancel'+code+':'+id}]]};
      const sent=await bot.send(full||('📝 Borrador · '+platforms[code]+'\n\n'+JSON.parse(story.editorial).headline+'\n\nEl texto completo está disponible en el botón inferior; supera el tamaño de un mensaje.'),keyboard);
      if(!Number.isSafeInteger(sent?.message_id))throw new ServiceError('telegram-draft');
      await db.run("UPDATE channel_jobs SET draft_message=?,delivery='sent' WHERE story_id=? AND platform=?",String(sent.message_id),id,platform);
    }catch {
      await db.run("UPDATE channel_jobs SET state='blocked',delivery='uncertain',error='draft-uncertain' WHERE story_id=? AND platform=?",id,platform);
    }
    return true;
  }
  async function action(action,id,messageId) {
    if(!/^(gen[qh]|(?:gen|ok|cancel|info)[xit])$/.test(action))return false;
    const verb=action.slice(0,-1),code=action.at(-1),platform=names[code]||'x',job=await getJob(id,platform);
    if(!job)return false;
    if(job.draft_message!==String(messageId)) {
      if(action!=='okx'||!['review','approved'].includes(job.state)||job.delivery!=='sent')return false;
      const cover=await db.first("SELECT 1 AS valid FROM channel_assets WHERE story_id=? AND platform='x' AND position=0 AND message_id=? AND length(file_id)>0",id,String(messageId));
      if(!cover)return false;
    }
    const story=await db.first("SELECT * FROM stories WHERE id=? AND state='approved'",id);
    if(!story)return false;
    if(verb==='gen'&&job.state==='draft'&&enabled()) {
      const queue=env.DB.prepare("UPDATE channel_jobs SET state='queued' WHERE story_id=? AND platform=? AND state='draft'").bind(id,platform);
      let queued;
      if(platform==='x'&&job.x_copy) {
        const draft=JSON.parse(job.x_copy);
        const option=draft.options.find(item=>item.code===code);
        if(!option||!xPublication(story,option.text).ready)return false;
        const selected=env.DB.prepare("UPDATE runtime SET value=? WHERE key=? AND EXISTS(SELECT 1 FROM channel_jobs WHERE story_id=? AND platform='x' AND state='draft')")
          .bind(JSON.stringify({...draft,selected:code}),xCopyKey(id),id);
        [,queued]=await env.DB.batch([selected,queue]);
      } else {
        if(platform==='x'&&(code!=='x'||!xPublication(story).ready))return false;
        queued=await queue.run();
      }
      if(!queued.meta?.changes)return false;
      await edit(job,'⏳ Preparando '+platforms[codes[platform]]+'\n\n'+JSON.parse(story.editorial).headline+'\n\nRecibirás '+(platform==='x'?'una portada.':'las cinco láminas en un solo grupo.')+' No publica en redes.');
    } else if(verb==='ok'&&['review','approved'].includes(job.state)&&job.delivery==='sent') {
      await db.run("UPDATE channel_jobs SET state='approved',approved_at=?,error=? WHERE story_id=? AND platform=? AND state='review'",now(),platform==='x'?'share-edit-pending:0':null,id,platform);
      if(platform==='x')await refreshX(id);
      else if(platform==='tiktok')await refreshTikTok(id);
      else await edit(job,'✅ '+platforms[code]+' · Material aprobado\n\n'+socialKit(story)[platform].caption+'\n\nFuente: '+story.source_url+'\nNo se ha publicado en redes.');
    } else if(verb==='cancel'&&['draft','review'].includes(job.state)) {
      await db.run("UPDATE channel_jobs SET state='rejected' WHERE story_id=? AND platform=?",id,platform);
      await edit(job,'Formato descartado · '+platforms[code]+'.');
      if(platform==='x') {
        const cover=await db.first("SELECT message_id FROM channel_assets WHERE story_id=? AND platform='x' AND position=0",id);
        await editCoverButtons(cover,{inline_keyboard:[]});
      }
    } else if(verb==='info') {
      // This download is requested explicitly, never sent as part of approval.
      const kit=socialKit(story);
      const text=platform==='x'?draftText(story,platform,job.x_copy?JSON.parse(job.x_copy):null):[kit[platform].caption,...kit.slides.map((s,i)=>(i+1)+'. '+s.title+'\n'+s.body),
        ...(platform==='tiktok'?['Guion opcional (no incluye vídeo)',...kit.video.beats.map(b=>b.role+': '+b.text)]:[]),
        'Fuente: '+story.source_url,'Fecha: '+kit.source.publishedAt].join('\n\n');
      const key='channel_info:'+id+':'+platform+':'+messageId;
      const claimed=await db.run('INSERT OR IGNORE INTO runtime(key,value) VALUES(?,?)',key,JSON.stringify({state:'sending',at:now()}));
      if(claimed.meta?.changes) {
        try {await bot.document(text,'WaCapital-'+id.slice(0,8)+'-'+platform+'.txt');await db.set(key,{state:'sent',at:now()});}
        catch {await db.set(key,{state:'uncertain',at:now()});}
      }
    }
    return true;
  }
  async function renderPng(html,dimensions) {
    if(render)return new Uint8Array(await render(html,dimensions));
    const {launch}=await import('@cloudflare/playwright');
    const browser=await launch(env.BROWSER,{keep_alive:10_000});
    try {
      const page=await browser.newPage({viewport:dimensions,deviceScaleFactor:1});
      await page.route('**/*',r=>r.request().url().startsWith('data:')||r.request().url()==='about:blank'?r.continue():r.abort());
      await page.setContent(html,{waitUntil:'load',timeout:15_000});
      const fits=await page.evaluate(async()=>{await document.fonts.ready;await Promise.all([...document.images].map(i=>i.decode()));return document.documentElement.dataset.fits==='true';});
      if(!fits)throw new ServiceError('layout-overflow');
      return new Uint8Array(await page.screenshot({type:'png'}));
    }finally{await browser.close();}
  }
  async function step() {
    if(!enabled())return {state:'disabled'};
    const owner=crypto.randomUUID(),instant=now();
    if(!await db.lock('visual',owner,instant,90_000))return {state:'busy'};
    let job;
    try {
      job=await db.first(jobWithCopy+" WHERE j.lease_until<=? AND (j.state IN ('queued','rendering','sending') OR (j.state='review' AND j.error='review-edit-pending') OR (j.platform IN ('x','tiktok') AND j.state='approved' AND j.error GLOB 'share-edit-pending:*')) ORDER BY j.created_at LIMIT 1",instant);
      if(!job)return {state:'idle'};
      const story=await db.first("SELECT * FROM stories WHERE id=? AND state='approved'",job.story_id);
      if(!story)throw new ServiceError('approval-required');
      if(job.state==='approved')return {state:await (job.platform==='x'?refreshX(job.story_id):refreshTikTok(job.story_id))?'approved':'share-edit-pending'};
      if(job.state==='review'){await showReview(job,story);return {state:'review'};}
      if(job.state==='sending')throw new ServiceError('delivery-uncertain');
      if(!await storage.allow())return {state:'storage-paused'};
      const prior=await db.all("SELECT manifest FROM channel_images WHERE state='ready' ORDER BY created_at DESC LIMIT 2");
      const result=await images.next(job.story_id,visualBrief(story,{recent:prior.map(r=>JSON.parse(r.manifest))}));
      if(result.state==='blocked')throw new ServiceError(result.reason);
      if(result.state==='daily-limit'&&job.error!=='quota-notice') {
        const edited=await edit(job,'⏳ '+platforms[codes[job.platform]]+' · Sin cupo de imagen disponible\n\n'+
          (result.reason==='horde-subject-restricted'?'Esta noticia cripto no puede usar el respaldo por sus condiciones de uso. ':'')+
          'El borrador está guardado. Se comprobará el cupo de Cloudflare al reiniciarse el día UTC.');
        if(edited)await db.run("UPDATE channel_jobs SET error='quota-notice' WHERE story_id=? AND platform=?",job.story_id,job.platform);
      }
      if(result.state==='daily-limit')await db.run('UPDATE channel_jobs SET lease_until=? WHERE story_id=? AND platform=?',Date.parse(new Date(instant+86_400_000).toISOString().slice(0,10)+'T00:00:00Z'),job.story_id,job.platform);
      else if(result.state==='pending')await db.run('UPDATE channel_jobs SET lease_until=? WHERE story_id=? AND platform=?',instant+60_000,job.story_id,job.platform);
      if(result.provider==='horde'&&job.error!=='horde-queue-notice') {
        const edited=await edit(job,'⏳ '+platforms[codes[job.platform]]+' · Esperando imagen\n\nSe agotó el cupo de Cloudflare. El respaldo gratuito está en cola; puede tardar varios minutos.'+
          '\nSe detendrá tras 45 minutos de espera. El borrador sigue guardado.');
        if(edited)await db.run("UPDATE channel_jobs SET error='horde-queue-notice' WHERE story_id=? AND platform=?",job.story_id,job.platform);
      }
      if(result.state!=='ready')return result;
      const count=job.platform==='x'?1:5;
      const assets=await db.all('SELECT * FROM channel_assets WHERE story_id=? AND platform=? ORDER BY position',job.story_id,job.platform);
      if(assets.length<count) {
        const last=await db.first("SELECT value FROM runtime WHERE key='visual_last_browser'");
        if(last&&instant-JSON.parse(last.value)<25_000)return {state:'browser-cooldown'};
        if(!await storage.allow(1_900_000))return {state:'storage-paused'};
        const position=assets.length;
        await db.set('visual_last_browser',instant);
        const html=job.platform==='x'?visualHtml(result.brief,result.image):socialHtml(socialKit(story),result.image,position,job.platform);
        const dims=job.platform==='x'?visualDimensions(result.brief,0):socialDimensions(job.platform);
        const png=await renderPng(html,dims);
        if(png.length>1_900_000||![137,80,78,71,13,10,26,10].every((n,i)=>png[i]===n))throw new ServiceError('png-size-or-format');
        await db.run('INSERT INTO channel_assets(story_id,platform,position,filename,png) VALUES(?,?,?,?,?)',job.story_id,job.platform,position,'WaCapital-'+job.story_id.slice(0,8)+'-'+job.platform+'-'+(position+1)+'.png',png.buffer.slice(png.byteOffset,png.byteOffset+png.byteLength));
        await db.run("UPDATE channel_jobs SET state='rendering',error=NULL WHERE story_id=? AND platform=?",job.story_id,job.platform);
        return {state:'rendered',platform:job.platform,position};
      }
      await db.run("UPDATE channel_jobs SET state='sending',delivery='sending' WHERE story_id=? AND platform=?",job.story_id,job.platform);
      const kind=result.brief.art?.mediaKind==='market-chart'?'Gráfico con datos · Corte '+new Date(result.brief.marketSnapshot.at).toISOString().slice(11,16)+' UTC':result.brief.art?.mediaKind==='official-portraits'?'Montaje editorial · Retratos oficiales':'Ilustración con IA';
      const x=job.platform==='x'?xForJob(story,job):null,caption=x?'🖼 Twitter/X · Borrador\n\n'+x.text+'\n\n'+x.weightedLength+'/280 · '+kind:
        platforms[codes[job.platform]]+' · 5 láminas · Borrador\n'+JSON.parse(story.editorial).headline+'\n'+kind+' · '+job.story_id.slice(0,8);
      const sent=job.platform==='x'?[await bot.imageDocument(new Uint8Array(assets[0].png),assets[0].filename,caption)]:
        await bot.album(assets.map(a=>({...a,png:new Uint8Array(a.png)})),caption);
      if(!Array.isArray(sent)||sent.length!==count||sent.some(s=>!s.document?.file_id||!Number.isSafeInteger(s.message_id)))throw new ServiceError('delivery-uncertain');
      // Commit the entire confirmed group together. Never release uncertain buffers.
      await env.DB.batch(assets.map((a,i)=>env.DB.prepare('UPDATE channel_assets SET file_id=?,message_id=?,png=NULL WHERE story_id=? AND platform=? AND position=?').bind(sent[i].document.file_id,String(sent[i].message_id),job.story_id,job.platform,a.position)));
      await db.run("UPDATE channel_jobs SET state='review',delivery='sent',error='review-edit-pending' WHERE story_id=? AND platform=?",job.story_id,job.platform);
      await showReview(job,story);
      return {state:'review',platform:job.platform};
    }catch(error) {
      if(job) {
        const current=await getJob(job.story_id,job.platform),category=error instanceof ServiceError?error.service:'preparation-unavailable';
        // Browser network errors retry the saved illustration at most twice.
        const attempt=Number(/^render-retry:(\d+)$/.exec(current.error||'')?.[1]||0)+1;
        if(current.state!=='sending'&&!(error instanceof ServiceError)&&attempt<=2) {
          await db.run('UPDATE channel_jobs SET error=? WHERE story_id=? AND platform=?','render-retry:'+attempt,job.story_id,job.platform);
          return {state:'render-deferred'};
        }
        await db.run("UPDATE channel_jobs SET state='blocked',error=?,delivery=? WHERE story_id=? AND platform=?",category,current.state==='sending'?'uncertain':current.delivery,job.story_id,job.platform);
        await edit(job,'⚠️ '+platforms[codes[job.platform]]+' · Preparación pendiente de revisión\n\n'+
          (current.state==='sending'?'La entrega no se pudo confirmar. No se reenviará automáticamente.':'No se pudo completar la imagen. El borrador y la fuente siguen guardados.')+
          '\nReferencia: '+job.story_id.slice(0,8));
      }
      return {state:'blocked'};
    }finally{await db.unlock('visual',owner);}
  }
  return {pick,action,step,refreshX,refreshTikTok};
}
