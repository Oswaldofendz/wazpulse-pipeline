import {isOwner, minimalUpdate, digest} from './domain.js';
import {readBounded, network, telegram} from './io.js';
import {store} from './store.js';
import {createPipeline} from './pipeline.js';
import {createMedia} from './media.js';
import {createStorage} from './storage.js';
import {createChannels} from './channels.js';
import {shareXRequest} from './share-x.js';
import {createFast} from './fast.js';

const json = (data, status = 200) => Response.json(data, {status, headers: {'Cache-Control': 'no-store'}});
const active = env => env.PIPELINE_ENABLED === 'true';
async function secretMatches(actual, expected) {
  // Compare fixed-length digests; never log either secret or a Telegram URL.
  if (!actual || !expected) return false;
  const [a, b] = await Promise.all([digest(actual), digest(expected)]);
  let mismatch = 0;
  for (let i = 0; i < a.length; i++) mismatch |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return mismatch === 0;
}

export default {
  async fetch(request, env, ctx) {
    const path = new URL(request.url).pathname;
    if(path==='/share/x'||path.startsWith('/share/x/'))return shareXRequest(request,env);
    if (path === '/health' && request.method === 'GET') {
      return json({service: 'wacapital-online', version: '0.4.0', enabled: active(env), databaseBound: Boolean(env.DB), mediaEnabled: env.MEDIA_ENABLED === 'true'});
    }
    if (path === '/telegram/webhook' && request.method === 'POST') {
      if (!await secretMatches(request.headers.get('X-Telegram-Bot-Api-Secret-Token'), env.TELEGRAM_WEBHOOK_SECRET)) return json({error: 'Unauthorized'}, 401);
      if (!env.DB || !active(env)) return json({error: 'Pipeline paused'}, 503);
      let update;
      try { update = minimalUpdate(JSON.parse(await readBounded(request, 16_000))); }
      catch { return json({error: 'Invalid update'}, 400); }
      if (!update || !isOwner(update, env)) return json({ok: true});
      try {
        const db = store(env.DB);
        const queued = await db.first("SELECT COUNT(*) AS count FROM inbox WHERE state IN ('new','processing')");
        if (queued.count >= 100) return json({error: 'Inbox full'}, 503);
        await db.queueUpdate(update, Date.now());
        ctx.waitUntil(createPipeline(env).drainInbox(1));
        return json({ok: true});
      } catch { return json({error: 'Queue unavailable'}, 503); }
    }
    if (path.startsWith('/admin/')) {
      if (!await secretMatches(request.headers.get('Authorization'), env.ADMIN_TOKEN ? 'Bearer ' + env.ADMIN_TOKEN : null)) return json({error: 'Unauthorized'}, 401);
      if (!env.DB) return json({error: 'Database not configured'}, 503);
      try {
        const pipeline = createPipeline(env);
        if (path === '/admin/status' && request.method === 'GET') return json(await pipeline.status());
        if(path==='/admin/fast-status'&&request.method==='GET')return json(await createFast(env).status());
        if(path==='/admin/fast-tick'&&request.method==='POST')return json(await createFast(env).tick());
        if(path==='/admin/fast-probe'&&request.method==='POST')return json(await createFast(env).probeCrypto());
        if(path==='/admin/share-x-status'&&request.method==='GET') {
          const row=await store(env.DB).first("SELECT value FROM runtime WHERE key='share_x_last_error'");
          return json({lastError:row?JSON.parse(row.value):null});
        }
        if (path === '/admin/tick' && request.method === 'POST') return json(await pipeline.tick());
        if (path === '/admin/media-step' && request.method === 'POST') return json(await createMedia(env).step());
        if (path === '/admin/channel-step' && request.method === 'POST') return json(await createChannels(env).step());
        if (path === '/admin/channel-status' && request.method === 'GET') {
          const db=store(env.DB);
          return json({jobs:await db.all('SELECT story_id,platform,state,delivery,error FROM channel_jobs ORDER BY created_at DESC LIMIT 10'),
            images:await db.all('SELECT story_id,state,provider,error FROM channel_images ORDER BY created_at DESC LIMIT 10'),
            budget:await db.all('SELECT * FROM image_provider_budget ORDER BY day DESC LIMIT 4')});
        }
        if (path === '/admin/storage-status' && request.method === 'GET') return json(await createStorage(env).status());
        if (path === '/admin/storage-plan' && request.method === 'GET') return json(await createStorage(env).plan());
        if (path === '/admin/storage-cleanup' && request.method === 'POST') return json(await createStorage(env).cleanup());
        if (path === '/admin/media-status' && request.method === 'GET') {
          const db = store(env.DB);
          return json({enabled: env.MEDIA_ENABLED === 'true', dailyLimit: env.DAILY_VISUAL_LIMIT || '3',
            budget: await db.all('SELECT * FROM visual_budget ORDER BY day DESC LIMIT 2'),
            jobs: await db.all('SELECT story_id,state,error,attempts,created_at FROM visual_jobs ORDER BY created_at DESC LIMIT 10'),
            assets: await db.all('SELECT story_id,position,filename,delivery FROM visual_assets ORDER BY rowid DESC LIMIT 60')});
        }
        const previewPath = path.match(/^\/admin\/media\/([a-f0-9]{32})\/preview\/([0-5])$/);
        const socialPath=path.match(/^\/admin\/media\/([a-f0-9]{32})\/social-(preview|deliver)\/(instagram|tiktok)\/([1-5])$/);
        if(socialPath&&request.method==='POST'){
          const [,id,action,platform,n]=socialPath,media=createMedia(env);
          if(action==='deliver')return json(await media.deliverPreview(id,platform,Number(n)-1));
          const result=await media.preview(id,Number(n),{reuseOnly:true,platform});
          return result.state==='preview'?new Response(result.png,{headers:{'Content-Type':'image/png','Cache-Control':'private, no-store'}}):json({state:result.state},409);
        }
        if (previewPath && request.method === 'POST') {
          const result = await createMedia(env).preview(previewPath[1], Number(previewPath[2]));
          if (result.state !== 'preview') return json({state: result.state}, 409);
          return new Response(result.png, {headers: {'Content-Type': 'image/png', 'Cache-Control': 'private, no-store'}});
        }
        const mediaPath = path.match(/^\/admin\/media\/([a-f0-9]{32})(?:\/(enqueue|manifest|retry|x-package|test-send|social-pack|social-send|[0-5]))?$/);
        if (mediaPath) {
          const [, id, action] = mediaPath, db = store(env.DB);
          if (action === 'enqueue' && request.method === 'POST') return json({queued: await createMedia(env).enqueue(id)});
          if (action === 'retry' && request.method === 'POST') return json({resumed: await createMedia(env).retry(id)});
          if (action === 'x-package' && request.method === 'POST') return json({prepared: await createMedia(env).prepareX(id)});
          if(action==='test-send'&&request.method==='POST')return json(await createMedia(env).deliverPreview(id));
          if(action==='social-pack'&&request.method==='GET'){
            const kit=await createMedia(env).social(id);
            return kit?json(kit):json({error:'Approval required'},409);
          }
          if(action==='social-send'&&request.method==='POST')return json({prepared:await createMedia(env).sendSocial(id)});
          if (!action && request.method === 'GET') return json(await createMedia(env).status(id));
          if (action === 'manifest' && request.method === 'GET') {
            const row = await db.first('SELECT manifest FROM visual_jobs WHERE story_id=?', id);
            return row?.manifest ? json(JSON.parse(row.manifest)) : json({error: 'Not ready'}, 404);
          }
          if (/^[0-5]$/.test(action || '') && request.method === 'GET') {
            const row = await db.first('SELECT png,file_id,filename FROM visual_assets WHERE story_id=? AND position=?', id, Number(action));
            if (!row) return json({error: 'Not found'}, 404);
            let bytes;
            if (row.png) bytes = new Uint8Array(row.png);
            else if (row.file_id) {
              const target = await telegram(env, network()).download(row.file_id);
              const response = await fetch(target, {redirect: 'manual', signal: AbortSignal.timeout(15_000)});
              if (!response.ok || Number(response.headers.get('content-length') || 0) > 1_900_000) throw new Error('Image unavailable');
              bytes = new Uint8Array(await response.arrayBuffer());
            }
            if (!bytes || bytes.length > 1_900_000) throw new Error('Image unavailable');
            return new Response(bytes, {headers: {'Content-Type': 'image/png', 'Cache-Control': 'private, no-store', 'Content-Disposition': 'attachment; filename="' + row.filename + '"'}});
          }
        }
        if (path === '/admin/telegram-info' && request.method === 'GET') {
          if (!env.TELEGRAM_BOT_TOKEN) return json({configured: false});
          const info = await telegram(env, network()).info();
          return json({configured: true, username: /^[A-Za-z0-9_]{5,32}$/.test(info.username || '') ? info.username : null});
        }
        if (path === '/admin/telegram-connect' && request.method === 'POST') {
          if (!active(env)) return json({error: 'Pipeline paused'}, 409);
          const origin = new URL(env.WORKER_ORIGIN || 'https://invalid.local');
          if (!env.TELEGRAM_BOT_TOKEN || !env.TELEGRAM_CHAT_ID || !env.TELEGRAM_WEBHOOK_SECRET ||
              origin.protocol !== 'https:' || !origin.hostname.endsWith('.workers.dev') ||
              origin.origin !== new URL(request.url).origin || origin.username || origin.password ||
              origin.search || origin.hash || origin.pathname !== '/' ||
              (!/^\d+$/.test(env.TELEGRAM_CHAT_ID) && !/^\d+$/.test(env.TELEGRAM_APPROVER_ID || ''))) {
            return json({error: 'Telegram configuration incomplete'}, 400);
          }
          const connected = await telegram(env, network()).connect(new URL('/telegram/webhook', origin).href);
          return json({connected: connected === true, pendingUpdatesPreserved: true});
        }
      } catch { return json({error: 'Service unavailable'}, 503); }
    }
    return json({error: 'Not found'}, 404);
  },
  async scheduled(event, env, ctx) {
    if (active(env) && env.DB) {
      // Fast observation is independent of Render and media work. Redaction
      // drains more often but keeps the same shared hard daily call budget.
      const minute = new Date(event.scheduledTime).getUTCMinutes();
      if(env.FAST_ENABLED==='true') {
        ctx.waitUntil((async()=>{
          const pulse=await createFast(env).tick();
          // Sending a new urgent alert takes precedence over background jobs.
          if(pulse.state!=='ok'||pulse.prices?.id||pulse.feeds?.some(f=>f.attempts>0))return;
          const pipeline=createPipeline(env);if(await pipeline.drainInbox(1))return;
          if(minute===1){await createStorage(env).cleanup();return;}
          if(minute%2===1){await pipeline.tick({collect:minute%10===3,inbox:false});return;}
          if(env.MEDIA_ENABLED==='true') {
            const result=await createChannels(env).step();
            if(result.state==='idle')await createMedia(env).step();
          }
        })());
        return;
      }
      if (minute === 1) ctx.waitUntil(createStorage(env).cleanup());
      else if (env.FAST_ENABLED!=='true'&&minute % 10 === 3) ctx.waitUntil(createPipeline(env).tick());
      else if (env.MEDIA_ENABLED === 'true') ctx.waitUntil((async()=>{
        const result=await createChannels(env).step();
        if(result.state==='idle')await createMedia(env).step();
      })());
    }
  }
};
