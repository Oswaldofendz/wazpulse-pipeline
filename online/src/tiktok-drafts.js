import {digest} from './domain.js';
import {network, readBounded, telegram} from './io.js';
import {store} from './store.js';
import {createStorage} from './storage.js';
import {imageBytes, matchesShareSignature, shareHeaders, shareOrigin, shareSignature} from './share-x.js';
import {tiktokAccess, tiktokAccount, tiktokConfigured} from './tiktok-auth.js';
import {Buffer} from 'node:buffer';
import {tiktokCaption} from './social.js';

const HOUR = 3_600_000;
const configured = env => tiktokConfigured(env) && env.TIKTOK_UPLOAD_ENABLED === 'true' && env.TIKTOK_MEDIA_VERIFIED === 'true';
const fingerprint = carousel => digest(JSON.stringify([carousel.assets.map(a => [a.position,a.file_id,a.filename]),carousel.text]));
const mediaPath = '/tiktok/media/';
const publicState = row => ({state:row.state, error:row.error || null});
const safeCode = value => /^[a-z_]{1,80}$/.test(value || '') ? value : 'unavailable';

export async function tiktokDraftStatus(env, id, now=Date.now(), carousel) {
  if (!configured(env)) return {state:'setup_required'};
  const account = await tiktokAccount(env);
  if (!account || account.refreshing || account.refreshExpires <= now) return {state:'connection_required'};
  const row = await store(env.DB).first('SELECT state,error,account_id,fingerprint FROM tiktok_uploads WHERE story_id=?', id);
  if (row && carousel && row.fingerprint !== await fingerprint(carousel)) return {state:'material_changed'};
  return {account:account.displayName, ...(row ? row.account_id === account.openId ? publicState(row) : {state:'account_changed'} : {state:'ready'})};
}

async function downloadPng(env, fileId, fetcher) {
  const url = await telegram(env, network(fetcher)).download(fileId);
  return imageBytes(await fetcher(url, {redirect:'manual', signal:AbortSignal.timeout(15_000)}));
}
// TikTok accepts JPEG/WebP, not the PNG originals delivered in Telegram.
// Re-encode those exact approved pixels without generating a new illustration.
export async function tiktokJpegs(env, assets, fetcher, launchOverride) {
  if (!env.BROWSER && !launchOverride) throw Error('Browser unavailable');
  const launch = launchOverride || (await import('@cloudflare/playwright')).launch;
  const browser = await launch(env.BROWSER, {keep_alive:10_000});
  try {
    const page = await browser.newPage({viewport:{width:1080,height:1920}, deviceScaleFactor:1});
    await page.route('**/*', route => route.abort());
    const result = [];
    for (const asset of assets) {
      const png = await downloadPng(env, asset.file_id, fetcher);
      const view = new DataView(png.buffer,png.byteOffset,png.byteLength);
      if (png.length < 24 || view.getUint32(16) !== 1080 || view.getUint32(20) !== 1920) throw Error('Unexpected slide dimensions');
      await page.setContent('<html><meta http-equiv="Content-Security-Policy" content="default-src \'none\'; img-src data:; style-src \'unsafe-inline\'"><style>html,body{margin:0;width:1080px;height:1920px;overflow:hidden}img{width:1080px;height:1920px}</style><img src="data:image/png;base64,'+Buffer.from(png).toString('base64')+'"></html>');
      await page.locator('img').evaluate(img => img.decode());
      result.push(new Uint8Array(await page.screenshot({type:'jpeg',quality:95})));
    }
    return result;
  } finally { await browser.close(); }
}

async function mediaUrl(env, row, position) {
  const sig = await shareSignature(env, ['wacapital-tiktok-media-v1', row.story_id, row.fingerprint, position, row.media_expires]);
  return shareOrigin(env)+mediaPath+[row.story_id,position,row.media_expires,sig+'.jpg'].join('/');
}

export async function tiktokMediaRequest(request, env, {now=Date.now}={}) {
  const pathname = new URL(request.url).pathname;
  // TikTok's URL-prefix ownership check uses the exact text file provided by
  // its developer portal. No arbitrary file path or HTML may be configured.
  if (request.method === 'GET' && /^[\w.-]{1,120}\.txt$/.test(env.TIKTOK_VERIFICATION_FILENAME || '') &&
      pathname === mediaPath+env.TIKTOK_VERIFICATION_FILENAME && typeof env.TIKTOK_VERIFICATION_CONTENT === 'string' &&
      env.TIKTOK_VERIFICATION_CONTENT.length>0 && env.TIKTOK_VERIFICATION_CONTENT.length<=4096) {
    return new Response(env.TIKTOK_VERIFICATION_CONTENT,{headers:{...shareHeaders,'Content-Type':'text/plain; charset=utf-8'}});
  }
  const match = pathname.match(/^\/tiktok\/media\/([a-f0-9]{32})\/([0-4])\/(\d{13})\/([a-f0-9]{64})\.jpg$/);
  if (!match || !['GET','HEAD'].includes(request.method) || !env.ADMIN_TOKEN || !env.DB) return new Response('No encontrado',{status:404,headers:shareHeaders});
  const [,id,position,expiry,sig] = match, expires = Number(expiry), db = store(env.DB);
  if (expires <= now() || expires > now()+2*HOUR) return new Response('Enlace caducado',{status:401,headers:shareHeaders});
  const row = await db.first("SELECT u.* FROM tiktok_uploads u JOIN stories s ON s.id=u.story_id JOIN channel_jobs j ON j.story_id=s.id AND j.platform='tiktok' WHERE u.story_id=? AND s.state='approved' AND j.state='approved' AND j.delivery='sent'",id);
  if (!row || row.media_expires !== expires || !['submitting','processing','inbox','published','uncertain'].includes(row.state) ||
      !matchesShareSignature(await shareSignature(env,['wacapital-tiktok-media-v1',id,row.fingerprint,Number(position),expires]),sig)) return new Response('Enlace no válido',{status:401,headers:shareHeaders});
  const currentAssets = await db.all("SELECT position,file_id,filename FROM channel_assets WHERE story_id=? AND platform='tiktok' ORDER BY position",id);
  const current = await db.first('SELECT editorial,source_url FROM stories WHERE id=?',id);
  if (await fingerprint({assets:currentAssets,text:tiktokCaption(current)}) !== row.fingerprint) return new Response('Material modificado',{status:401,headers:shareHeaders});
  const asset = await db.first('SELECT jpeg FROM tiktok_upload_assets WHERE story_id=? AND position=?',id,Number(position));
  if (!asset) return new Response('No encontrado',{status:404,headers:shareHeaders});
  const bytes = new Uint8Array(asset.jpeg);
  return new Response(request.method==='HEAD'?null:bytes,{headers:{...shareHeaders,'Content-Type':'image/jpeg','Content-Length':String(bytes.length)}});
}

export async function submitTikTokDraft(env, id, carousel, {fetcher=fetch, now=Date.now, convert=tiktokJpegs, allowStorage}={}) {
  if (!configured(env)) return {state:'setup_required'};
  const db = store(env.DB), existing = await db.first('SELECT * FROM tiktok_uploads WHERE story_id=?',id);
  if (existing) return existing.fingerprint === await fingerprint(carousel) ? publicState(existing) : {state:'material_changed'};
  let account;
  try { account = await tiktokAccess(env,{fetcher,now}); }
  catch (error) { return {state:error.message==='TikTok account busy'?'busy':'connection_required'}; }
  if (carousel.text.length > 4000) return {state:'caption_too_long'};
  const instant = now(), hash = await fingerprint(carousel);
  // An atomic durable claim survives double taps, concurrent pages and timeouts.
  // No retry path re-enters the initializer for this story.
  const claimed = await db.first(`INSERT OR IGNORE INTO tiktok_uploads(story_id,account_id,fingerprint,state,created_at,updated_at,media_expires)
    SELECT ?,?,?,'preparing',?,?,? WHERE (SELECT COUNT(*) FROM tiktok_uploads WHERE created_at>?)<5 RETURNING *`,
    id,account.openId,hash,instant,instant,instant+2*HOUR,instant-24*HOUR);
  if (!claimed) {
    const row = await db.first('SELECT * FROM tiktok_uploads WHERE story_id=?',id);
    return row ? publicState(row) : {state:'daily_limit'};
  }
  let initializing = false;
  try {
    await db.run('DELETE FROM tiktok_upload_assets WHERE story_id IN (SELECT story_id FROM tiktok_uploads WHERE media_expires<=?)',now());
    const allowed = allowStorage ? await allowStorage() : await createStorage(env,{fetcher,now}).allow(10_000_000);
    if (!allowed) throw Error('Storage unavailable');
    const bytes = await convert(env,carousel.assets,fetcher);
    if (bytes.length !== 5 || bytes.some(b => b.length<4 || b.length>1_900_000 || b[0]!==255 || b[1]!==216 || b.at(-2)!==255 || b.at(-1)!==217)) throw Error('Invalid JPEG');
    await env.DB.batch(bytes.map((b,position) => env.DB.prepare('INSERT INTO tiktok_upload_assets(story_id,position,jpeg) VALUES(?,?,?)').bind(id,position,b.buffer.slice(b.byteOffset,b.byteOffset+b.byteLength))));
    // Approval and the snapshot must still match after image preparation.
    const current = await db.first("SELECT s.editorial FROM stories s JOIN channel_jobs j ON j.story_id=s.id AND j.platform='tiktok' WHERE s.id=? AND s.state='approved' AND j.state='approved' AND j.delivery='sent'",id);
    const assets = await db.all("SELECT position,file_id,filename FROM channel_assets WHERE story_id=? AND platform='tiktok' ORDER BY position",id);
    if (!current || current.editorial !== carousel.story.editorial || await fingerprint({assets,text:carousel.text}) !== hash) throw Error('Approval changed');
    await db.run("UPDATE tiktok_uploads SET state='submitting',updated_at=? WHERE story_id=?",now(),id);
    const images = await Promise.all(bytes.map((_,position) => mediaUrl(env,claimed,position)));
    initializing = true;
    const response = await fetcher('https://open.tiktokapis.com/v2/post/publish/content/init/', {
      method:'POST',redirect:'manual',signal:AbortSignal.timeout(20_000),
      headers:{Authorization:'Bearer '+account.accessToken,'Content-Type':'application/json; charset=UTF-8'},
      body:JSON.stringify({post_mode:'MEDIA_UPLOAD',media_type:'PHOTO',post_info:{description:carousel.text},
        source_info:{source:'PULL_FROM_URL',photo_cover_index:0,photo_images:images},is_aigc:carousel.isAigc})
    });
    const result = JSON.parse(await readBounded(response,20_000));
    if (response.ok && result.error?.code === 'ok' && typeof result.data?.publish_id === 'string' && result.data.publish_id.length<=128) {
      await db.run("UPDATE tiktok_uploads SET state='processing',publish_id=?,updated_at=? WHERE story_id=?",result.data.publish_id,now(),id);
      return {state:'processing'};
    }
    // Only a documented explicit rejection is a known failure. An ambiguous
    // response must never trigger another upload, even after a worker restart.
    const known = response.status>=400 && response.status<500 && result.error?.code && result.error.code!=='ok';
    await db.run('UPDATE tiktok_uploads SET state=?,error=?,updated_at=? WHERE story_id=?',known?'failed':'uncertain',known?safeCode(result.error.code):'upload_unconfirmed',now(),id);
    return {state:known?'failed':'uncertain',error:known?safeCode(result.error.code):'upload_unconfirmed'};
  } catch {
    const state = initializing?'uncertain':'failed';
    await db.run('UPDATE tiktok_uploads SET state=?,error=?,updated_at=? WHERE story_id=?',state,initializing?'upload_unconfirmed':'preparation_failed',now(),id);
    return {state,error:initializing?'upload_unconfirmed':'preparation_failed'};
  }
}

export async function pollTikTokDraft(env, id, {fetcher=fetch,now=Date.now}={}) {
  if (!configured(env)) return {state:'setup_required'};
  const db = store(env.DB);
  let row = await db.first('SELECT * FROM tiktok_uploads WHERE story_id=?',id);
  if (!row) return {state:'ready'};
  if (['preparing','submitting'].includes(row.state) && now()-row.updated_at>120_000) {
    await db.run("UPDATE tiktok_uploads SET state='uncertain',error='upload_unconfirmed' WHERE story_id=? AND state IN ('preparing','submitting') AND updated_at<=?",id,now()-120_000);
    row = await db.first('SELECT * FROM tiktok_uploads WHERE story_id=?',id);
  }
  if (!row.publish_id || !['processing','inbox'].includes(row.state)) return publicState(row);
  const claim = await db.first("UPDATE tiktok_uploads SET checked_at=? WHERE story_id=? AND checked_at<=? RETURNING story_id",now(),id,now()-5000);
  if (!claim) return publicState(row);
  try {
    const account = await tiktokAccess(env,{fetcher,now});
    if (account.openId !== row.account_id) return {state:'account_changed'};
    const response = await fetcher('https://open.tiktokapis.com/v2/post/publish/status/fetch/',{
      method:'POST',redirect:'manual',signal:AbortSignal.timeout(10_000),
      headers:{Authorization:'Bearer '+account.accessToken,'Content-Type':'application/json'},body:JSON.stringify({publish_id:row.publish_id})
    });
    const result = JSON.parse(await readBounded(response,20_000));
    if (!response.ok || result.error?.code!=='ok') return {...publicState(row),checkUnavailable:true};
    const state = {PROCESSING_UPLOAD:'processing',PROCESSING_DOWNLOAD:'processing',SEND_TO_USER_INBOX:'inbox',PUBLISH_COMPLETE:'published',FAILED:'failed'}[result.data?.status];
    if (!state) return {...publicState(row),checkUnavailable:true};
    const error = state==='failed'?safeCode(result.data.fail_reason):null;
    await db.run('UPDATE tiktok_uploads SET state=?,error=?,updated_at=? WHERE story_id=?',state,error,now(),id);
    return {state,error};
  } catch { return {...publicState(row),checkUnavailable:true}; }
}
