import {digest} from './domain.js';
import {store} from './store.js';
import {network,telegram} from './io.js';
import {imageBytes} from './share-x.js';
import {socialKitV2,socialDimensions} from './social.js';
import {visualBrief} from './publication.js';
import {createStorage} from './storage.js';

export function socialRevisions(env,{fetcher=fetch,now=Date.now}={}) {
  const db=store(env.DB);
  async function create(parentId,input) {
    if(!input||typeof input!=='object'||Array.isArray(input))return {error:'Invalid revision',status:400};
    const platform=input?.platform||'tiktok';
    if(!['tiktok','instagram'].includes(platform)||typeof input.title!=='string'||input.title.length>90||!input.title.trim()||
      typeof input.caption!=='string'||input.caption.length>3500||typeof input.image!=='string'||input.image.length>2_400_000||
      input.image.length<100||!/^[A-Za-z0-9+/=]+$/.test(input.image)||!Array.isArray(input.slides)||input.slides.length<3||input.slides.length>5) return {error:'Invalid revision',status:400};
    if(input.slides.some(s=>!s||typeof s.title!=='string'||typeof s.body!=='string'||s.title.length>150||s.body.length>260))return {error:'Invalid slide',status:400};
    const parent=await db.first("SELECT * FROM stories WHERE id=? AND state='approved'",parentId);
    if(!parent)return {error:'Original approval required',status:409};
    const e={...JSON.parse(parent.editorial),headline:input.title,socialVersion:2,socialTitle:input.title,socialSlides:input.slides,socialCaption:input.caption};
    const id=(await digest(parentId+'|social-v2|'+platform+'|'+JSON.stringify(e)+'|'+await digest(input.image))).slice(0,32);
    const existing=await db.first('SELECT state,delivery,draft_message FROM channel_jobs WHERE story_id=? AND platform=?',id,platform);
    if(existing)return {id,...existing,created:false};
    const story={...parent,id,editorial:JSON.stringify(e)},kit=socialKitV2(story);
    if(!kit.quality.ready)return {error:'More source detail required',status:409};
    if(!await createStorage(env,{fetcher,now}).allow(12_000_000))return {error:'Storage paused',status:409};
    const instant=now(),brief=visualBrief(story),snapshot=JSON.stringify(kit);
    const statements=[
      env.DB.prepare("INSERT OR IGNORE INTO stories(id,event_key,title,source,source_url,published_at,priority,snippet,assets,state,notification,editorial,created_at,approved_at) SELECT ?,?,title,source,source_url,published_at,priority,snippet,assets,'approved','sent',?,?,? FROM stories WHERE id=? AND state='approved'").bind(id,parent.event_key+':social-revision:'+id,story.editorial,instant,instant,parentId),
      env.DB.prepare("INSERT OR IGNORE INTO channel_jobs(story_id,platform,state,delivery,created_at) VALUES(?,?,'draft','sending',?)").bind(id,platform,instant),
      env.DB.prepare('INSERT OR IGNORE INTO runtime(key,value) VALUES(?,?)').bind('channel_social_kit:'+id,snapshot),
      env.DB.prepare('INSERT OR IGNORE INTO runtime(key,value) VALUES(?,?)').bind('social_revision:'+id,JSON.stringify({parentId,version:2,platform,createdAt:instant})),
      env.DB.prepare("INSERT OR IGNORE INTO channel_images(story_id,state,manifest,image,provider,created_at,updated_at) VALUES(?,'ready',?,?,'editorial-revision',?,?)").bind(id,JSON.stringify(brief),input.image,instant,instant)
    ];
    const results=await env.DB.batch(statements);
    if(!results[1].meta?.changes)return {id,created:false,state:'existing'};
    try {
      const message=await telegram(env,network(fetcher)).send('🛠 '+(platform==='tiktok'?'TikTok':'Instagram')+' · Versión corregida\n\n'+kit[platform].caption+
        '\n\nPreparando '+kit.slides.length+' láminas nuevas. Revisarás esta versión antes de enviarla.');
      if(!Number.isSafeInteger(message?.message_id))throw Error('Missing message');
      await db.run("UPDATE channel_jobs SET draft_message=?,delivery='sent' WHERE story_id=? AND platform=?",String(message.message_id),id,platform);
      return {id,state:'draft',created:true,files:kit.slides.length};
    }catch{
      await db.run("UPDATE channel_jobs SET state='blocked',delivery='uncertain',error='revision-draft-uncertain' WHERE story_id=? AND platform=?",id,platform);
      return {id,state:'blocked',created:true};
    }
  }
  async function asset(id,position,request) {
    const revision=await db.first('SELECT value FROM runtime WHERE key=?','social_revision:'+id);
    if(!revision)return {error:'Revision not found',status:404};
    const {platform}=JSON.parse(revision.value);
    const job=await db.first("SELECT * FROM channel_jobs WHERE story_id=? AND platform=? AND state='draft' AND delivery='sent'",id,platform);
    const saved=await db.first('SELECT value FROM runtime WHERE key=?','channel_social_kit:'+id);
    if(!job||!saved)return {error:'Revision is no longer editable',status:409};
    const kit=JSON.parse(saved.value);
    if(!Number.isInteger(position)||position<0||position>=kit.slides.length)return {error:'Invalid position',status:400};
    const png=await imageBytes(new Response(request.body,{headers:request.headers}));
    const view=new DataView(png.buffer,png.byteOffset,png.byteLength),dims=socialDimensions(platform);
    if(png.length<24||view.getUint32(16)!==dims.width||view.getUint32(20)!==dims.height)return {error:'Incorrect image dimensions',status:400};
    const hash=await crypto.subtle.digest('SHA-256',png),signature=Array.from(new Uint8Array(hash),n=>n.toString(16).padStart(2,'0')).join('');
    const key='social_revision_asset:'+id+':'+position;
    const prior=await db.first('SELECT value FROM runtime WHERE key=?',key);
    if(prior)return JSON.parse(prior.value)===signature?{id,position,stored:true,sha256:signature}:{error:'Image already fixed',status:409};
    await env.DB.batch([
      env.DB.prepare('INSERT OR IGNORE INTO channel_assets(story_id,platform,position,filename,png) VALUES(?,?,?,?,?)').bind(id,platform,position,'WaCapital-'+id.slice(0,8)+'-'+platform+'-'+(position+1)+'.png',png.buffer.slice(png.byteOffset,png.byteOffset+png.byteLength)),
      env.DB.prepare('INSERT OR IGNORE INTO runtime(key,value) VALUES(?,?)').bind(key,JSON.stringify(signature))
    ]);
    const committed=await db.first('SELECT value FROM runtime WHERE key=?',key);
    if(!committed||JSON.parse(committed.value)!==signature)return {error:'Image already fixed',status:409};
    return {id,position,stored:true,sha256:signature};
  }
  async function ready(id) {
    const row=await db.first('SELECT value FROM runtime WHERE key=?','social_revision:'+id);
    if(!row)return {error:'Revision not found',status:404};
    const {platform}=JSON.parse(row.value),saved=await db.first('SELECT value FROM runtime WHERE key=?','channel_social_kit:'+id);
    const kit=JSON.parse(saved.value),assets=await db.all('SELECT position,length(png) AS bytes FROM channel_assets WHERE story_id=? AND platform=? ORDER BY position',id,platform);
    const job=await db.first('SELECT state,delivery FROM channel_jobs WHERE story_id=? AND platform=?',id,platform);
    if(job.state!=='draft')return {id,state:job.state,delivery:job.delivery};
    if(job.delivery!=='sent'||assets.length!==kit.slides.length||assets.some((a,i)=>a.position!==i||!a.bytes))return {error:'Revision incomplete',status:409};
    await db.run("UPDATE channel_jobs SET state='rendering',lease_until=0,error=NULL WHERE story_id=? AND platform=? AND state='draft' AND delivery='sent'",id,platform);
    return {id,state:'rendering',files:assets.length};
  }
  return {create,asset,ready};
}
