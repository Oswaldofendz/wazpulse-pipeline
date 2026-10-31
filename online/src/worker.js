import {isOwner, minimalUpdate, digest} from './domain.js';
import {readBounded} from './io.js';
import {store} from './store.js';
import {createPipeline} from './pipeline.js';

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
    if (path === '/health' && request.method === 'GET') {
      return json({service: 'wacapital-online', version: '0.1.0', enabled: active(env), databaseBound: Boolean(env.DB)});
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
        if (path === '/admin/tick' && request.method === 'POST') return json(await pipeline.tick());
      } catch { return json({error: 'Service unavailable'}, 503); }
    }
    return json({error: 'Not found'}, 404);
  },
  async scheduled(_event, env, ctx) {
    if (active(env) && env.DB) ctx.waitUntil(createPipeline(env).tick());
  }
};
