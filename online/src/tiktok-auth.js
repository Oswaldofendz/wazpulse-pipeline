import {digest} from './domain.js';
import {readBounded} from './io.js';
import {store} from './store.js';
import {matchesShareSignature, shareHeaders, shareOrigin, shareSignature} from './share-x.js';

const encoder = new TextEncoder();
const accountKey = 'tiktok_account_v1';
const cookieName = '__Host-wacapital-tiktok';
const TTL = 15 * 60_000;
const scopes = ['user.info.basic', 'video.upload'];
const random = () => [...crypto.getRandomValues(new Uint8Array(32))].map(n => n.toString(16).padStart(2, '0')).join('');
const callback = env => shareOrigin(env) + '/tiktok/callback';
export const tiktokConfigured = env => Boolean(env.TIKTOK_CLIENT_KEY && env.TIKTOK_CLIENT_SECRET && env.ADMIN_TOKEN && shareOrigin(env));

// Tokens stay encrypted in D1. The existing random admin secret supplies key
// material, with a separate context from the share-link signing key.
async function key(env) {
  const bytes = await crypto.subtle.digest('SHA-256', encoder.encode('wacapital-tiktok-token-v1\n' + env.ADMIN_TOKEN));
  return crypto.subtle.importKey('raw', bytes, 'AES-GCM', false, ['encrypt', 'decrypt']);
}
async function save(env, account) {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const bound = {...account, clientKeyHash:await digest(env.TIKTOK_CLIENT_KEY)};
  const bytes = await crypto.subtle.encrypt({name:'AES-GCM', iv, additionalData:encoder.encode(accountKey)}, await key(env), encoder.encode(JSON.stringify(bound)));
  const hex = value => [...new Uint8Array(value)].map(n => n.toString(16).padStart(2, '0')).join('');
  await store(env.DB).set(accountKey, {iv:hex(iv), data:hex(bytes)});
}
export async function tiktokAccount(env) {
  if (!tiktokConfigured(env)) return null;
  const row = await store(env.DB).first('SELECT value FROM runtime WHERE key=?', accountKey);
  if (!row) return null;
  try {
    const value = JSON.parse(row.value);
    const bytes = hex => new Uint8Array(hex.match(/../g).map(n => parseInt(n, 16)));
    const account = JSON.parse(new TextDecoder().decode(await crypto.subtle.decrypt({name:'AES-GCM', iv:bytes(value.iv), additionalData:encoder.encode(accountKey)}, await key(env), bytes(value.data))));
    return account.clientKeyHash === await digest(env.TIKTOK_CLIENT_KEY) ? account : null;
  } catch { return null; }
}

async function tokenRequest(env, fields, fetcher) {
  const response = await fetcher('https://open.tiktokapis.com/v2/oauth/token/', {
    method:'POST', redirect:'manual', signal:AbortSignal.timeout(15_000),
    headers:{'Content-Type':'application/x-www-form-urlencoded'},
    body:new URLSearchParams({client_key:env.TIKTOK_CLIENT_KEY, client_secret:env.TIKTOK_CLIENT_SECRET, ...fields}).toString()
  });
  const data = JSON.parse(await readBounded(response, 20_000));
  if (!response.ok || typeof data.error === 'string' || !data.access_token || !data.refresh_token || !data.open_id ||
      !Number.isFinite(data.expires_in) || data.expires_in <= 0 || !Number.isFinite(data.refresh_expires_in) || data.refresh_expires_in <= 0 ||
      scopes.some(scope => !String(data.scope).split(',').includes(scope))) throw Error('TikTok authorization unavailable');
  return data;
}
function tokens(data, now, displayName) {
  return {accessToken:data.access_token, refreshToken:data.refresh_token, openId:data.open_id,
    expires:now + data.expires_in * 1000, refreshExpires:now + data.refresh_expires_in * 1000, displayName, refreshing:false};
}
export async function tiktokAccess(env, {fetcher=fetch, now=Date.now}={}) {
  const db = store(env.DB), owner = random();
  if (!await db.lock('tiktok-account', owner, now(), 30_000)) throw Error('TikTok account busy');
  try {
    const account = await tiktokAccount(env);
    if (!account || account.refreshing || account.refreshExpires <= now()) throw Error('Reconnect TikTok');
    if (account.expires > now() + 60_000) return account;
    // A rotated refresh token cannot safely be retried after a lost response.
    await save(env, {...account, refreshing:true});
    const data = await tokenRequest(env, {grant_type:'refresh_token', refresh_token:account.refreshToken}, fetcher);
    if (data.open_id !== account.openId) throw Error('TikTok account changed');
    const next = tokens(data, now(), account.displayName);
    await save(env, next);
    return next;
  } finally { await db.unlock('tiktok-account', owner); }
}

export async function tiktokConnectUrl(env, now=Date.now()) {
  if (!tiktokConfigured(env)) return null;
  const expires = now + TTL;
  const sig = await shareSignature(env, ['wacapital-tiktok-connect-v1', expires]);
  return shareOrigin(env) + '/tiktok/connect#' + expires + '.' + sig;
}
const htmlHeaders = {...shareHeaders, 'Content-Type':'text/html; charset=utf-8',
  'Content-Security-Policy':"default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"};
const html = body => '<!doctype html><html lang="es"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>WaCapital · Conectar TikTok</title><style>body{background:#090f17;color:#eef5fa;font:17px/1.5 system-ui;max-width:560px;padding:30px 20px;margin:auto}button{font:inherit;background:#80ead0;border:0;border-radius:12px;padding:16px;width:100%}p{overflow-wrap:anywhere}</style><h1>Conectar TikTok</h1>' + body + '</html>';
const connectPage = html('<p>Conecta la cuenta de WaCapital para recibir las cinco láminas y la descripción en la bandeja de entrada de TikTok. Podrás revisarlas en la app antes de publicar.</p><p>Comprueba que estás autorizando la cuenta correcta de TikTok.</p><button id="connect">Continuar con TikTok</button><p id="status" role="status"></p><script src="/tiktok/connect.js" defer></script>');
const connectClient = `const button=document.getElementById('connect');button.onclick=async()=>{button.disabled=true;try{const [expires,sig]=location.hash.slice(1).split('.');const r=await fetch('/tiktok/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({expires:Number(expires),sig}),redirect:'error'});if(!r.ok)throw Error();const data=await r.json();location.assign(data.url);}catch{document.getElementById('status').textContent='No se pudo conectar. Solicita un enlace nuevo con /conectartiktok en Telegram.';button.disabled=false;}};`;

export async function tiktokAuthRequest(request, env, {fetcher=fetch, now=Date.now}={}) {
  const url = new URL(request.url), db = store(env.DB);
  if (url.pathname === '/tiktok/connect' && request.method === 'GET') return new Response(env.TIKTOK_MODE==='sandbox'
    ? connectPage.replace('<h1>Conectar TikTok</h1>','<h1>Conectar TikTok</h1><p><strong>Conexión de prueba · Sandbox</strong></p>') : connectPage, {headers:htmlHeaders});
  if (url.pathname === '/tiktok/connect.js' && request.method === 'GET') return new Response(connectClient, {headers:{...shareHeaders, 'Content-Type':'text/javascript; charset=utf-8'}});
  if (!tiktokConfigured(env)) return new Response('La conexión de TikTok aún no está configurada.', {status:503, headers:shareHeaders});
  if (url.pathname === '/tiktok/start' && request.method === 'POST') {
    try {
      if (request.headers.get('Origin') !== shareOrigin(env)) throw Error();
      const {expires, sig} = JSON.parse(await readBounded(request, 1024));
      if (!Number.isSafeInteger(expires) || expires <= now() || expires > now() + TTL || !/^[a-f0-9]{64}$/.test(sig || '') ||
          !matchesShareSignature(await shareSignature(env, ['wacapital-tiktok-connect-v1', expires]), sig)) throw Error();
      const state = random(), cookie = random();
      await db.set('tiktok_oauth_session', {state:await digest(state), cookie:await digest(cookie), expires:now()+TTL});
      const target = new URL('https://www.tiktok.com/v2/auth/authorize/');
      target.search = new URLSearchParams({client_key:env.TIKTOK_CLIENT_KEY, response_type:'code', scope:scopes.join(','), redirect_uri:callback(env), state, disable_auto_auth:'1'}).toString();
      return Response.json({url:target.href}, {headers:{...shareHeaders, 'Set-Cookie':cookieName+'='+cookie+'; Secure; HttpOnly; SameSite=Lax; Path=/; Max-Age=900'}});
    } catch { return new Response('Enlace no válido o caducado', {status:401, headers:shareHeaders}); }
  }
  if (url.pathname === '/tiktok/callback' && request.method === 'GET') {
    let owner;
    const headers = {...htmlHeaders, 'Set-Cookie':cookieName+'=; Secure; HttpOnly; SameSite=Lax; Path=/; Max-Age=0'};
    try {
      const state = url.searchParams.get('state'), code = url.searchParams.get('code');
      const cookie = request.headers.get('Cookie')?.split(';').map(s => s.trim()).find(s => s.startsWith(cookieName+'='))?.slice(cookieName.length+1);
      if (!/^[a-f0-9]{64}$/.test(state || '') || !/^[a-f0-9]{64}$/.test(cookie || '')) throw Error();
      const session = await db.first("DELETE FROM runtime WHERE key='tiktok_oauth_session' AND json_extract(value,'$.state')=? AND json_extract(value,'$.cookie')=? AND json_extract(value,'$.expires')>? RETURNING value", await digest(state), await digest(cookie), now());
      if (!session || !code || code.length > 4096 || url.searchParams.has('error')) throw Error();
      owner = random();
      if (!await db.lock('tiktok-account', owner, now(), 30_000)) throw Error();
      const data = await tokenRequest(env, {grant_type:'authorization_code', code, redirect_uri:callback(env)}, fetcher);
      const response = await fetcher('https://open.tiktokapis.com/v2/user/info/?fields=open_id,display_name', {
        headers:{Authorization:'Bearer '+data.access_token}, redirect:'manual', signal:AbortSignal.timeout(15_000)
      });
      const profile = JSON.parse(await readBounded(response, 20_000));
      if (!response.ok || profile.error?.code !== 'ok' || profile.data?.user?.open_id !== data.open_id || !profile.data.user.display_name) throw Error();
      await save(env, tokens(data, now(), String(profile.data.user.display_name).slice(0, 100)));
      return new Response(html('<p>Cuenta conectada. Vuelve al carrusel aprobado en Telegram y pulsa «Compartir en TikTok».</p><p>Al enviar el borrador, abre TikTok → Bandeja de entrada y toca la notificación para continuar.</p>'), {headers});
    } catch { return new Response(html('<p>No se completó la conexión. Vuelve a Telegram y solicita /conectartiktok para intentarlo de nuevo.</p>'), {status:400, headers}); }
    finally { if (owner) await db.unlock('tiktok-account', owner); }
  }
  return new Response('No encontrado', {status:404, headers:shareHeaders});
}
