import {store} from './store.js';
import {network, telegram, ServiceError} from './io.js';
import {visualBrief, visualHtml, visualDimensions, xPublication} from './publication.js';
import {socialKit, socialHtml, socialDimensions} from './social.js';
import {createImages} from './image-providers.js';
import {createStorage} from './storage.js';
import {shareXUrl,xButtons} from './share-x.js';

const platforms={x:'Twitter/X',i:'Instagram',t:'TikTok'};
const names={x:'x',i:'instagram',t:'tiktok'};
const codes={x:'x',instagram:'i',tiktok:'t'};
export function newsCard(story) {
  const e=JSON.parse(story.editorial);
  const date=new Date(story.published_at).toLocaleDateString('es-ES',{timeZone:'UTC',day:'2-digit',month:'short'});
  return ['📰 '+e.headline,story.source+' · '+date+(e.marketSnapshot?' · '+new Date(e.marketSnapshot.at).toISOString().slice(11,16)+' UTC':'')].join('\n\n');
}
export function newsButtons(id) {
  return {inline_keyboard:[[{text:'Aprobar para Twitter/X',callback_data:'pickx:'+id}],
    [{text:'Aprobar para TikTok',callback_data:'pickt:'+id},{text:'Aprobar para Instagram',callback_data:'picki:'+id}],
    [{text:'Descartar',callback_data:'reject:'+id}]]};
}
function draftText(story,platform) {
  const kit=socialKit(story);
  const body=platform==='x' ? kit.x.text+'\n\n'+kit.x.weightedLength+'/280 caracteres ponderados · Una portada.' :
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
  const getJob=(id,p)=>db.first('SELECT * FROM channel_jobs WHERE story_id=? AND platform=?',id,p);
  const enabled=()=>env.MEDIA_ENABLED==='true'&&env.PIPELINE_ENABLED==='true';
  async function edit(job,text,buttons) {
    if(!job.draft_message)return false;
    // Editing an existing message is safe to repeat; sending an upload is not.
    try {await bot.edit(job.draft_message,text,buttons);return true;}
    catch{return false;}
  }
  async function showReview(job,story) {
    const x=xPublication(story);
    const success=await edit(job,'✅ '+platforms[codes[job.platform]]+' · Listo para revisar\n\n'+(job.platform==='x'?x.text+'\n\n'+x.weightedLength+'/280 caracteres ponderados.':socialKit(story)[job.platform].caption)+
      '\n\nRevisa '+(job.platform==='x'?'la portada recibida.':'el grupo de cinco láminas recibido.')+' Aprobar confirma el material; no publica.',
      {inline_keyboard:[[{text:'Aprobar versión final',callback_data:'ok'+codes[job.platform]+':'+job.story_id},{text:'Descartar',callback_data:'cancel'+codes[job.platform]+':'+job.story_id}]]});
    if(success)await db.run("UPDATE channel_jobs SET error=NULL WHERE story_id=? AND platform=? AND state='review'",job.story_id,job.platform);
  }
  async function refreshX(id) {
    const job=await getJob(id,'x');
    if(job?.state!=='approved'||job.delivery!=='sent')return false;
    const story=await db.first("SELECT * FROM stories WHERE id=? AND state='approved'",id);
    const cover=await db.first("SELECT file_id,filename FROM channel_assets WHERE story_id=? AND platform='x' AND position=0 AND length(file_id)>0",id);
    if(!story||!cover)return false;
    const x=xPublication(story),shareUrl=await shareXUrl(env,story,cover,'c',now());
    return edit(job,'✅ Twitter/X · Material aprobado\n\n'+x.text+'\n\n'+x.weightedLength+'/280 · '+
      (shareUrl?'Comparte texto y portada desde el móvil; revisa que X reciba ambos.':'Adjunta la portada recibida al abrir X.')+
      '\n\nFuente: '+story.source_url+'\nNo se ha publicado en redes.',xButtons(x,shareUrl));
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
      const x=xPublication(story),full=draftText(story,platform);
      const canGenerate=enabled()&&(platform!=='x'||x.ready);
      const keyboard={inline_keyboard:[...(canGenerate?[ [{text:platform==='x'?'Preparar portada':'Preparar 5 láminas',callback_data:'gen'+code+':'+id}] ]:[]),
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
    const match=/^(gen|ok|cancel|info)([xit])$/.exec(action);
    if(!match)return false;
    const [,verb,code]=match,platform=names[code],job=await getJob(id,platform);
    if(!job||job.draft_message!==String(messageId))return false;
    const story=await db.first("SELECT * FROM stories WHERE id=? AND state='approved'",id);
    if(!story)return false;
    if(verb==='gen'&&job.state==='draft'&&enabled()) {
      if(platform==='x'&&!xPublication(story).ready)return false;
      await db.run("UPDATE channel_jobs SET state='queued' WHERE story_id=? AND platform=? AND state='draft'",id,platform);
      await edit(job,'⏳ Preparando '+platforms[code]+'\n\n'+JSON.parse(story.editorial).headline+'\n\nRecibirás '+(platform==='x'?'una portada.':'las cinco láminas en un solo grupo.')+' No publica en redes.');
    } else if(verb==='ok'&&['review','approved'].includes(job.state)&&job.delivery==='sent') {
      await db.run("UPDATE channel_jobs SET state='approved',approved_at=? WHERE story_id=? AND platform=? AND state='review'",now(),id,platform);
      if(platform==='x')await refreshX(id);
      else await edit(job,'✅ '+platforms[code]+' · Material aprobado\n\n'+socialKit(story)[platform].caption+'\n\nFuente: '+story.source_url+'\nNo se ha publicado en redes.');
    } else if(verb==='cancel'&&['draft','review'].includes(job.state)) {
      await db.run("UPDATE channel_jobs SET state='rejected' WHERE story_id=? AND platform=?",id,platform);
      await edit(job,'Formato descartado · '+platforms[code]+'.');
    } else if(verb==='info') {
      // This download is requested explicitly, never sent as part of approval.
      const kit=socialKit(story);
      const text=platform==='x'?draftText(story,platform):[kit[platform].caption,...kit.slides.map((s,i)=>(i+1)+'. '+s.title+'\n'+s.body),
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
      job=await db.first("SELECT * FROM channel_jobs WHERE lease_until<=? AND (state IN ('queued','rendering','sending') OR (state='review' AND error='review-edit-pending')) ORDER BY created_at LIMIT 1",instant);
      if(!job)return {state:'idle'};
      const story=await db.first("SELECT * FROM stories WHERE id=? AND state='approved'",job.story_id);
      if(!story)throw new ServiceError('approval-required');
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
      const x=xPublication(story),caption=job.platform==='x'?'🖼 Twitter/X · Borrador\n\n'+x.text+'\n\n'+x.weightedLength+'/280 · '+kind:
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
  return {pick,action,step,refreshX};
}
