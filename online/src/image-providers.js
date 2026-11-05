import {store} from './store.js';
import {readBounded, ServiceError} from './io.js';
import {trump} from './portraits.js';
import {CHART_PIXEL} from './market-graph.js';

const HORDE = 'https://aihorde.net/api/v2';
const headers = {'Content-Type': 'application/json', apikey: '0000000000', 'Client-Agent': 'WaCapital:1.0:https://github.com/Oswaldofendz/wazpulse-pipeline'};
const validImage = image => typeof image === 'string' && image.length >= 100 && image.length <= 2_400_000 && /^[A-Za-z0-9+/=]+$/.test(image);
export const quotaError = error => error?.status === 429 || /\b(?:429|quota|neurons|daily.*limit|quota.*exceeded)\b/i.test(String(error?.message || ''));
// Horde's published terms restrict cryptocurrency/blockchain uses. Restrict the
// integration to non-crypto subjects, including when crypto appears in extract.
export const hordeEligible = brief => !/\b(?:bitcoin|btc|ethereum|eth|crypto\w*|cripto\w*|blockchain|nfts?|binance|coinbase|stablecoins?|defi|altcoins?|solana|dogecoin|token\w*)\b/i.test(JSON.stringify({title:brief.sourceTitle||'',headline:brief.title,extract:brief.sourceExtract||''}));

// All submissions are persisted before contacting a provider. An uncertain
// submission is blocked rather than blindly repeated on the next cron tick.
export function createImages(env, {fetcher = fetch, now = Date.now, generate} = {}) {
  const db = store(env.DB);
  async function json(path, options = {}) {
    const response = await fetcher(HORDE + path, {...options, headers, redirect:'manual', signal:AbortSignal.timeout(15_000)});
    if (!response.ok) throw new ServiceError('horde-http', response.status);
    return JSON.parse(await readBounded(response, 2_500_000));
  }
  async function next(id, brief) {
    const instant = now(), day = new Date(instant).toISOString().slice(0,10);
    await db.run('INSERT OR IGNORE INTO channel_images(story_id,manifest,created_at,updated_at) VALUES(?,?,?,?)',id,JSON.stringify(brief),instant,instant);
    let row = await db.first('SELECT * FROM channel_images WHERE story_id=?',id);
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
    if(row.state==='blocked'||row.state==='calling')return {state:'blocked',reason:row.error||'submission-uncertain'};
    const saved = JSON.parse(row.manifest);
    if(row.state==='new') {
      await db.run('INSERT OR IGNORE INTO visual_budget(day,used) VALUES(?,0)',day);
      const exhausted=await db.first('SELECT value FROM runtime WHERE key=?','cloudflare_exhausted:'+day);
      const cap=/^[0-6]$/.test(String(env.DAILY_VISUAL_LIMIT))?Number(env.DAILY_VISUAL_LIMIT):3;
      const reserved=!exhausted&&await db.first('UPDATE visual_budget SET used=used+1 WHERE day=? AND used<? RETURNING used',day,cap);
      if(reserved) {
        await db.run("UPDATE channel_images SET state='calling',provider='cloudflare',updated_at=? WHERE story_id=?",instant,id);
        try {
          if(!env.AI&&!generate)throw new ServiceError('ai-binding');
          const image = generate ? await generate(saved) : (await env.AI.run(saved.model,{prompt:saved.prompt,steps:saved.steps})).image;
          if(!validImage(image))throw new ServiceError('ai-image');
          await db.run("UPDATE channel_images SET state='ready',image=?,error=NULL,updated_at=? WHERE story_id=?",image,instant,id);
          return {state:'ready',image,brief:saved,provider:'cloudflare'};
        } catch(error) {
          if(!quotaError(error)) {
            await db.run("UPDATE channel_images SET state='blocked',error='cloudflare-unavailable',updated_at=? WHERE story_id=?",instant,id);
            return {state:'blocked',reason:'cloudflare-unavailable'};
          }
          await db.set('cloudflare_exhausted:'+day,true);
          await db.run("UPDATE channel_images SET state='fallback',updated_at=? WHERE story_id=?",instant,id);
          return {state:'pending'};
        }
      }
      await db.run("UPDATE channel_images SET state='fallback',updated_at=? WHERE story_id=?",instant,id);
      row = {...row,state:'fallback'};
    }
    if(row.state==='fallback') {
      if(env.HORDE_ENABLED!=='true')return {state:'daily-limit'};
      if(!hordeEligible(saved))return {state:'daily-limit',reason:'horde-subject-restricted'};
      const cap=/^[0-3]$/.test(String(env.DAILY_HORDE_LIMIT))?Number(env.DAILY_HORDE_LIMIT):2;
      await db.run("INSERT OR IGNORE INTO image_provider_budget(day,provider,used) VALUES(?,'horde',0)",day);
      if(!await db.first("UPDATE image_provider_budget SET used=used+1 WHERE day=? AND provider='horde' AND used<? RETURNING used",day,cap))return {state:'daily-limit'};
      await db.run("UPDATE channel_images SET state='calling',provider='horde',updated_at=?,error='submission-uncertain' WHERE story_id=?",instant,id);
      try {
        const result=await json('/generate/async',{method:'POST',body:JSON.stringify({prompt:saved.prompt,params:{width:512,height:512,steps:4,n:1,sampler_name:'k_euler',cfg_scale:1},models:['Flux.1-Schnell fp8 (Compact)'],nsfw:false,censor_nsfw:true,r2:false,shared:true,trusted_workers:true,allow_downgrade:true})});
        if(typeof result.id!=='string'||! /^[a-f0-9-]{36}$/.test(result.id))throw new ServiceError('horde-id');
        await db.run("UPDATE channel_images SET state='waiting',request_id=?,error=NULL,updated_at=? WHERE story_id=?",result.id,instant,id);
        return {state:'pending'};
      } catch {
        await db.run("UPDATE channel_images SET state='blocked',error='horde-submission-uncertain' WHERE story_id=?",id);
        return {state:'blocked',reason:'horde-submission-uncertain'};
      }
    }
    if(row.state==='waiting') {
      if(instant-row.updated_at>45*60_000) {
        await db.run("UPDATE channel_images SET state='blocked',error='horde-timeout' WHERE story_id=?",id);
        await json('/generate/status/'+row.request_id,{method:'DELETE'}).catch(()=>{});
        return {state:'blocked',reason:'horde-timeout'};
      }
      // Poll failures never start another generation and never notify repeatedly.
      let check;
      try {check=await json('/generate/check/'+row.request_id);}catch{return {state:'pending'};}
      if(check.faulted||check.is_possible===false) {
        await db.run("UPDATE channel_images SET state='blocked',error='horde-unavailable' WHERE story_id=?",id);
        return {state:'blocked',reason:'horde-unavailable'};
      }
      if(!check.done)return {state:'pending',provider:'horde',waitSeconds:Number.isFinite(check.wait_time)?check.wait_time:null};
      let result;
      try {result=await json('/generate/status/'+row.request_id);}catch{return {state:'pending'};}
      const image=result.generations?.[0]?.img;
      if(result.generations?.[0]?.censored||!validImage(image)) {
        await db.run("UPDATE channel_images SET state='blocked',error='horde-image' WHERE story_id=?",id);
        return {state:'blocked',reason:'horde-image'};
      }
      await db.run("UPDATE channel_images SET state='ready',image=?,error=NULL,updated_at=? WHERE story_id=?",image,instant,id);
      return {state:'ready',image,brief:saved,provider:'horde'};
    }
    return {state:'pending'};
  }
  return {next};
}
