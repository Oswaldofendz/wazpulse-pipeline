import {store} from './store.js';
import {network, telegram} from './io.js';

const DAY = 86_400_000;
export const STORAGE_LIMIT_BYTES = 100_000_000;
const PREVIEW_AGE = 7 * DAY, IMAGE_AGE = 30 * DAY;
// Clocks are separate from cached images, so legacy caches can receive a
// conservative first-observed timestamp without rewriting megabytes of JSON.
const previewClock = 'retention_preview:';
const imageClock = 'retention_image:';
const complete = alias => `(SELECT COUNT(*) FROM visual_assets a WHERE a.story_id=${alias}.story_id AND a.delivery='sent' AND length(a.file_id)>0 AND a.png IS NULL)=6`;
const born = alias => `CASE WHEN json_valid(${alias}.value) THEN CASE WHEN json_type(${alias}.value)='integer' THEN CAST(${alias}.value AS INTEGER) ELSE NULL END ELSE NULL END`;
const channelsComplete = alias => `EXISTS(SELECT 1 FROM channel_jobs j WHERE j.story_id=${alias}.story_id) AND NOT EXISTS(SELECT 1 FROM channel_jobs j WHERE j.story_id=${alias}.story_id AND (j.state NOT IN ('review','approved','rejected') OR j.delivery!='sent' OR (SELECT COUNT(*) FROM channel_assets a WHERE a.story_id=j.story_id AND a.platform=j.platform AND length(a.file_id)>0 AND a.png IS NULL)!=CASE WHEN j.platform='x' THEN 1 ELSE COALESCE((SELECT CASE WHEN json_valid(value) THEN json_array_length(value,'$.slides') END FROM runtime WHERE key='channel_social_kit:'||j.story_id),5) END))`;

export function createStorage(env, {fetcher = fetch, now = Date.now} = {}) {
  const db = store(env.DB);
  async function size() {
    const result = await env.DB.prepare('SELECT 1 AS storage_probe').bind().all();
    const bytes = result.meta?.size_after;
    return Number.isSafeInteger(bytes) && bytes > 0 ? bytes : null;
  }
  async function plan() {
    const instant = now();
    const previews = await db.first(`SELECT COUNT(*) AS n FROM runtime r JOIN runtime c ON c.key=?||r.key WHERE r.key GLOB 'visual_preview:*' AND ${born('c')}>0 AND ${born('c')}<=?`, previewClock, instant - PREVIEW_AGE);
    const images = await db.first(`SELECT COUNT(*) AS n FROM visual_jobs j JOIN runtime c ON c.key=?||j.story_id WHERE j.image IS NOT NULL AND j.state IN ('review','approved','rejected') AND ${complete('j')} AND ${born('c')}>0 AND ${born('c')}<=?`, imageClock, instant - IMAGE_AGE);
    return {expiredPreviews: previews.n, deliveredBackgroundsToRelease: images.n};
  }
  async function status() {
    const bytes = await size();
    const last = await db.first("SELECT value FROM runtime WHERE key='storage_last_cleanup'");
    return {bytes, limitBytes: STORAGE_LIMIT_BYTES, guardReserveBytes: 2_400_000, generationPaused: bytes === null || bytes + 2_400_000 >= STORAGE_LIMIT_BYTES,
      previewRetentionDays: 7, deliveredBackgroundRetentionDays: 30,
      lastCleanup: last ? JSON.parse(last.value) : null, ...await plan()};
  }
  async function allow(reserveBytes = 2_400_000) {
    let bytes = null;
    try { bytes = await size(); } catch { /* Fail closed for new image work. */ }
    if (bytes !== null && bytes + reserveBytes < STORAGE_LIMIT_BYTES) return true;
    const last = await db.first("SELECT value FROM runtime WHERE key='storage_last_alert'");
    const previous = last ? JSON.parse(last.value) : 0;
    if (now() - previous >= DAY) {
      // Mark before notification to avoid repeated messages on lost responses.
      await db.set('storage_last_alert', now());
      if (env.TELEGRAM_BOT_TOKEN && env.TELEGRAM_CHAT_ID) await telegram(env, network(fetcher)).send(
        '🧹 Protección de almacenamiento\nLa creación de nuevas imágenes está pausada ' +
        (bytes === null ? 'porque no se pudo medir el espacio disponible.' : 'para mantener margen dentro del plan gratuito.') +
        '\nLos textos, las aprobaciones y los archivos entregados se conservan. La limpieza programada seguirá comprobando el espacio. No se ha activado un plan de pago.'
      ).catch(() => {});
    }
    return false;
  }
  async function delivered(id) {
    // Clock starts only after all six originals have confirmed file IDs.
    return db.run(`INSERT OR IGNORE INTO runtime(key,value) SELECT ?||j.story_id,? FROM visual_jobs j WHERE j.story_id=? AND ${complete('j')}`, imageClock, JSON.stringify(now()), id);
  }
  async function rememberPreview(key) {
    return db.run('INSERT OR IGNORE INTO runtime(key,value) VALUES(?,?)', previewClock + key, JSON.stringify(now()));
  }
  async function cleanup() {
    if (env.PIPELINE_ENABLED !== 'true') return {state: 'disabled'};
    const owner = crypto.randomUUID(), instant = now();
    if (!await db.lock('visual', owner, instant, 90_000)) return {state: 'busy'};
    try {
      // TikTok fetches JPEG copies for at most two hours. Preserve upload IDs
      // and approval history; release only expired temporary image bytes.
      await db.run('DELETE FROM tiktok_upload_assets WHERE story_id IN (SELECT story_id FROM tiktok_uploads WHERE media_expires<=?)',instant);
      // No deletion based on the publication date: previews may have been
      // rendered much later. Legacy clocks start today, never retroactively.
      await db.run(`INSERT OR IGNORE INTO runtime(key,value) SELECT ?||r.key,? FROM runtime r WHERE r.key GLOB 'visual_preview:*' AND NOT EXISTS(SELECT 1 FROM runtime c WHERE c.key=?||r.key) LIMIT 10`, previewClock, JSON.stringify(instant), previewClock);
      await db.run(`INSERT OR IGNORE INTO runtime(key,value) SELECT ?||j.story_id,? FROM visual_jobs j WHERE j.image IS NOT NULL AND j.state IN ('review','approved','rejected') AND ${complete('j')} AND NOT EXISTS(SELECT 1 FROM runtime c WHERE c.key=?||j.story_id) LIMIT 10`, imageClock, JSON.stringify(instant), imageClock);
      const deleted = await db.run(`DELETE FROM runtime WHERE key IN (SELECT r.key FROM runtime r JOIN runtime c ON c.key=?||r.key WHERE r.key GLOB 'visual_preview:*' AND ${born('c')}>0 AND ${born('c')}<=? LIMIT 10)`, previewClock, instant - PREVIEW_AGE);
      const released = await db.run(`UPDATE visual_jobs SET image=NULL WHERE story_id IN (SELECT j.story_id FROM visual_jobs j JOIN runtime c ON c.key=?||j.story_id WHERE j.image IS NOT NULL AND j.state IN ('review','approved','rejected') AND ${complete('j')} AND ${born('c')}>0 AND ${born('c')}<=? LIMIT 10)`, imageClock, instant - IMAGE_AGE);
      await db.run(`INSERT OR IGNORE INTO runtime(key,value) SELECT 'retention_channel:'||i.story_id,? FROM channel_images i WHERE i.image IS NOT NULL AND ${channelsComplete('i')} LIMIT 10`,JSON.stringify(instant));
      const channelReleased=await db.run(`UPDATE channel_images SET image=NULL WHERE story_id IN (SELECT i.story_id FROM channel_images i JOIN runtime c ON c.key='retention_channel:'||i.story_id WHERE i.image IS NOT NULL AND ${channelsComplete('i')} AND ${born('c')}>0 AND ${born('c')}<=? LIMIT 10)`,instant-IMAGE_AGE);
      await db.run(`DELETE FROM runtime WHERE key IN (SELECT c.key FROM runtime c WHERE substr(c.key,1,?)=? AND NOT EXISTS(SELECT 1 FROM runtime r WHERE r.key=substr(c.key,?)) LIMIT 10)`, previewClock.length, previewClock, previewClock.length + 1);
      const result = {at: new Date(instant).toISOString(), state: 'ok',
        deletedPreviews: deleted.meta?.changes || 0, releasedBackgrounds: released.meta?.changes || 0, releasedChannelBackgrounds:channelReleased.meta?.changes||0,
        bytes: await size(), limitBytes: STORAGE_LIMIT_BYTES};
      await db.set('storage_last_cleanup', result);
      return result;
    } finally { await db.unlock('visual', owner); }
  }
  return {status, plan, allow, delivered, rememberPreview, cleanup};
}
