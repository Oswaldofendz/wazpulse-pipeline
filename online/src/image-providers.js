import {store} from './store.js';
import {readBounded, ServiceError} from './io.js';
import {trump} from './portraits.js';
import {CHART_PIXEL} from './market-graph.js';
import {backupImage,configuredImageBackups,imageLimit} from './image-backups.js';
import {Buffer} from 'node:buffer';

const HORDE = 'https://aihorde.net/api/v2';
const headers = {'Content-Type': 'application/json', apikey: '0000000000', 'Client-Agent': 'WaCapital:1.0:https://github.com/Oswaldofendz/wazpulse-pipeline'};
const validImage = image => {
  if(typeof image!=='string'||image.length<100||image.length>2400000||!/^[A-Za-z0-9+/=]+$/.test(image))return false;
  const bytes=Buffer.from(image.slice(0,32),'base64');
  return bytes[0]===255&&bytes[1]===216&&bytes[2]===255||[137,80,78,71,13,10,26,10].every((v,i)=>bytes[i]===v)||bytes.toString('ascii',0,4)==='RIFF'&&bytes.toString('ascii',8,12)==='WEBP';
};
export const quotaError = error => error?.status === 429 || /\b(?:429|quota|neurons|daily.*limit|quota.*exceeded)\b/i.test(String(error?.message || ''));
// Horde's published terms restrict cryptocurrency/blockchain uses. Restrict the
// integration to non-crypto subjects, including when crypto appears in extract.
export const hordeEligible = brief => !/\b(?:bitcoin|btc|ethereum|eth|crypto\w*|cripto\w*|blockchain|nfts?|binance|coinbase|stablecoins?|defi|altcoins?|solana|dogecoin|token\w*)\b/i.test(JSON.stringify({title:brief.sourceTitle||'',headline:brief.title,extract:brief.sourceExtract||''}));

// Every submission is claimed before contacting a provider. An uncertain call
// never repeats that provider; another provider or local artwork can finish it.
export function createImages(env, {fetcher = fetch, now = Date.now, generate} = {}) {
  const db = store(env.DB);
  async function ready(id,brief,image,provider,instant){
    await db.run("UPDATE channel_images SET state='ready',manifest=?,image=?,provider=?,error=NULL,updated_at=? WHERE story_id=?",JSON.stringify(brief),image,provider,instant,id);
    return {state:'ready',image,brief,provider};
  }
  async function local(id,brief,reason,instant){
    if(env.EDITORIAL_FALLBACK_ENABLED==='false')return {state:'daily-limit',reason};
    const fallback={...brief,art:{...brief.art,mediaKind:'editorial-vector'},fallbackReason:reason};
    return ready(id,fallback,CHART_PIXEL,'editorial-local',instant);
  }
  async function external(id,brief,day,instant){
    for(const provider of configuredImageBackups(env)){
      const circuit=await db.first('SELECT value FROM runtime WHERE key=?','image_circuit:'+day+':'+provider.id);
      if(circuit&&JSON.parse(circuit.value).until>instant)continue;
      const key='image_attempt:'+id+':'+provider.id;
      if(!(await db.run('INSERT OR IGNORE INTO runtime(key,value) VALUES(?,?)',key,JSON.stringify({state:'claimed',at:instant}))).meta?.changes)continue;
      await db.run('INSERT OR IGNORE INTO image_provider_budget(day,provider,used) VALUES(?,?,0)',day,provider.id);
      const reserved=await db.first('UPDATE image_provider_budget SET used=used+1 WHERE day=? AND provider=? AND used<? RETURNING used',day,provider.id,imageLimit(env[provider.limit]));
      if(!reserved){await db.set(key,{state:'skipped',reason:'daily-cap',at:instant});continue;}
      await db.run("UPDATE channel_images SET state='calling',provider=?,updated_at=? WHERE story_id=?",provider.id,instant,id);
      try{
        const image=await backupImage(provider,brief,env,fetcher);await db.set(key,{state:'ready',at:instant});
        return await ready(id,brief,image,provider.id,instant);
      }catch(error){
        const status=Number.isInteger(error?.status)?error.status:0;
        await db.set(key,{state:status?'failed':'uncertain',status,reason:error instanceof ServiceError?error.service:'network',at:instant});
        await db.set('image_circuit:'+day+':'+provider.id,{until:[401,402,403,429].includes(status)?Date.parse(day+'T00:00:00Z')+86400000:instant+300000,status});
        await db.run("UPDATE channel_images SET state='fallback',error=?,updated_at=? WHERE story_id=?",provider.id+'-unavailable',instant,id);
      }
    }
    return null;
  }
  async function json(path, options = {}) {
    const response = await fetcher(HORDE + path, {...options, headers, redirect:'manual', signal:AbortSignal.timeout(15_000)});
    if (!response.ok) throw new ServiceError('horde-http', response.status);
    return JSON.parse(await readBounded(response, 2_500_000));
  }
  async function next(id, brief) {
    const instant = now(), day = new Date(instant).toISOString().slice(0,10);
    await db.run('INSERT OR IGNORE INTO channel_images(story_id,manifest,created_at,updated_at) VALUES(?,?,?,?)',id,JSON.stringify(brief),instant,instant);
    let row = await db.first('SELECT * FROM channel_images WHERE story_id=?',id);
    if(row.state==='new'&&brief.art?.priceMove&&!brief.marketSnapshot&&brief.art.mediaKind!=='official-portraits'&&env.EDITORIAL_FALLBACK_ENABLED!=='false')return local(id,brief,'source-price-illustration',instant);
    if(brief.art?.mediaKind==='market-chart'&&row.state==='new') {
      await db.run("UPDATE channel_images SET state='ready',image=?,provider='data-chart',updated_at=? WHERE story_id=?",CHART_PIXEL,instant,id);
      return {state:'ready',image:CHART_PIXEL,brief,provider:'data-chart'};
    }
    if(brief.art?.mediaKind==='official-portraits'&&row.state==='new') {
      await db.run("UPDATE channel_images SET state='ready',image=?,provider='official-portraits',updated_at=? WHERE story_id=?",trump,instant,id);
      return {state:'ready',image:trump,brief,provider:'official-portraits'};
    }
    if(row.state==='ready'&&!row.image) {
      await db.run("UPDATE channel_images SET state='new',provider=NULL,request_id=NULL,updated_at=? WHERE story_id=?",instant,id);
      await db.run("DELETE FROM runtime WHERE key=?",'retention_channel:'+id);
      row={...row,state:'new'};
    }
    if(row.state==='ready')return {state:'ready',image:row.image,brief:JSON.parse(row.manifest),provider:row.provider};
    if(row.state==='blocked')return {state:'blocked',reason:row.error||'submission-uncertain'};
    if(row.state==='calling'){
      await db.set('image_attempt:'+id+':'+row.provider,{state:'uncertain',at:instant});
      await db.run("UPDATE channel_images SET state='fallback',error='submission-uncertain',updated_at=? WHERE story_id=?",instant,id);row={...row,state:'fallback'};
    }
    const saved = JSON.parse(row.manifest);
    if(row.state==='new') {
      await db.run('INSERT OR IGNORE INTO visual_budget(day,used) VALUES(?,0)',day);
      const exhausted=await db.first('SELECT value FROM runtime WHERE key=?','cloudflare_exhausted:'+day);
      const cap=imageLimit(env.DAILY_VISUAL_LIMIT,3);
      const reserved=!exhausted&&await db.first('UPDATE visual_budget SET used=used+1 WHERE day=? AND used<? RETURNING used',day,cap);
      if(reserved) {
        await db.run("UPDATE channel_images SET state='calling',provider='cloudflare',updated_at=? WHERE story_id=?",instant,id);
        let timer;
        try {
          if(!env.AI&&!generate)throw new ServiceError('ai-binding');
          const task=generate?generate(saved):Promise.resolve(env.AI.run(saved.model,{prompt:saved.prompt,steps:saved.steps})).then(result=>result.image);
          const image=await Promise.race([task,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new ServiceError('cloudflare-timeout')),15000);})]);
          if(!validImage(image))throw new ServiceError('ai-image');
          await db.run("UPDATE channel_images SET state='ready',image=?,error=NULL,updated_at=? WHERE story_id=?",image,instant,id);
          return {state:'ready',image,brief:saved,provider:'cloudflare'};
        } catch(error) {
          if(quotaError(error))await db.set('cloudflare_exhausted:'+day,true);
          await db.set('image_attempt:'+id+':cloudflare',{state:'failed',reason:quotaError(error)?'provider-quota':'provider-unavailable',status:Number.isInteger(error?.status)?error.status:0,at:instant});
          await db.run("UPDATE channel_images SET state='fallback',error=?,updated_at=? WHERE story_id=?",quotaError(error)?'cloudflare-quota':'cloudflare-unavailable',instant,id);
          return {state:'pending'};
        }finally{clearTimeout(timer);}
      }
      await db.run("UPDATE channel_images SET state='fallback',updated_at=? WHERE story_id=?",instant,id);
      row = {...row,state:'fallback'};
    }
    if(row.state==='fallback') {
      const alternative=await external(id,saved,day,instant);if(alternative)return alternative;
      if(env.HORDE_ENABLED!=='true')return local(id,saved,'ai-providers-unavailable',instant);
      if(!hordeEligible(saved))return local(id,saved,'horde-subject-restricted',instant);
      const cap=/^[0-3]$/.test(String(env.DAILY_HORDE_LIMIT))?Number(env.DAILY_HORDE_LIMIT):2;
      await db.run("INSERT OR IGNORE INTO image_provider_budget(day,provider,used) VALUES(?,'horde',0)",day);
      if(!(await db.run('INSERT OR IGNORE INTO runtime(key,value) VALUES(?,?)','image_attempt:'+id+':horde',JSON.stringify({state:'calling',at:instant}))).meta?.changes)return local(id,saved,'horde-submission-uncertain',instant);
      if(!await db.first("UPDATE image_provider_budget SET used=used+1 WHERE day=? AND provider='horde' AND used<? RETURNING used",day,cap))return local(id,saved,'ai-daily-cap',instant);
      await db.run("UPDATE channel_images SET state='calling',provider='horde',updated_at=?,error='submission-uncertain' WHERE story_id=?",instant,id);
      try {
        const result=await json('/generate/async',{method:'POST',body:JSON.stringify({prompt:saved.prompt,params:{width:512,height:512,steps:4,n:1,sampler_name:'k_euler',cfg_scale:1},models:['Flux.1-Schnell fp8 (Compact)'],nsfw:false,censor_nsfw:true,r2:false,shared:true,trusted_workers:true,allow_downgrade:true})});
        if(typeof result.id!=='string'||! /^[a-f0-9-]{36}$/.test(result.id))throw new ServiceError('horde-id');
        await db.run("UPDATE channel_images SET state='waiting',request_id=?,error=NULL,updated_at=? WHERE story_id=?",result.id,instant,id);
        return {state:'pending'};
      } catch {
        await db.set('image_attempt:'+id+':horde',{state:'uncertain',at:instant});
        return local(id,saved,'horde-submission-uncertain',instant);
      }
    }
    if(row.state==='waiting') {
      const wait=/^\d{2,3}$/.test(String(env.IMAGE_BACKUP_WAIT_SECONDS))?Math.max(60,Math.min(600,Number(env.IMAGE_BACKUP_WAIT_SECONDS))):120;
      if(instant-row.updated_at>=wait*1000) {
        await json('/generate/status/'+row.request_id,{method:'DELETE'}).catch(()=>{});
        return local(id,saved,'horde-timeout',instant);
      }
      // Poll failures never start another generation and never notify repeatedly.
      let check;
      try {check=await json('/generate/check/'+row.request_id);}catch{return {state:'pending'};}
      if(check.faulted||check.is_possible===false) {
        return local(id,saved,'horde-unavailable',instant);
      }
      if(!check.done)return {state:'pending',provider:'horde',waitSeconds:Number.isFinite(check.wait_time)?check.wait_time:null};
      let result;
      try {result=await json('/generate/status/'+row.request_id);}catch{return {state:'pending'};}
      const image=result.generations?.[0]?.img;
      if(result.generations?.[0]?.censored||!validImage(image)) {
        return local(id,saved,'horde-image',instant);
      }
      await db.run("UPDATE channel_images SET state='ready',image=?,error=NULL,updated_at=? WHERE story_id=?",image,instant,id);
      return {state:'ready',image,brief:saved,provider:'horde'};
    }
    return {state:'pending'};
  }
  return {next};
}
