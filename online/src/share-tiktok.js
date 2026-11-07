import {store} from './store.js';
import {network, readBounded, telegram} from './io.js';
import {tiktokCaption} from './social.js';
import {imageBytes, matchesShareSignature, shareHeaders, shareOrigin, shareSignature, WEEK} from './share-x.js';
import {pollTikTokDraft, submitTikTokDraft, tiktokDraftStatus} from './tiktok-drafts.js';

const path = '/share/tiktok';
const pageHeaders = {
  ...shareHeaders,
  'Content-Type': 'text/html; charset=utf-8',
  'Permissions-Policy': 'web-share=(self)',
  'Content-Security-Policy': "default-src 'none'; script-src 'self'; style-src 'unsafe-inline'; img-src blob:; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"
};

async function approvedCarousel(env, id) {
  const db = store(env.DB);
  const story = await db.first(`
    SELECT s.*, (SELECT manifest FROM channel_images WHERE story_id=s.id) AS image_manifest
    FROM stories s JOIN channel_jobs j ON j.story_id = s.id
    WHERE s.id = ? AND s.state = 'approved' AND j.platform = 'tiktok'
      AND j.state = 'approved' AND j.delivery = 'sent'
  `, id);
  if (!story) return null;
  const assets = await db.all(`
    SELECT position, file_id, filename FROM channel_assets
    WHERE story_id = ? AND platform = 'tiktok' ORDER BY position LIMIT 6
  `, id);
  if (assets.length !== 5 || assets.some((asset, position) => asset.position !== position ||
      !asset.file_id || !/^[\w-]{1,120}\.png$/i.test(asset.filename))) return null;
  const kind = story.image_manifest ? JSON.parse(story.image_manifest).art?.mediaKind : null;
  return {story, assets, text: tiktokCaption(story), isAigc:!['market-chart','official-portraits'].includes(kind)};
}

function sign(env, id, expires, carousel) {
  const files = carousel.assets.map(({position, file_id, filename}) => [position, file_id, filename]);
  return shareSignature(env, ['wacapital-share-tiktok-v1', id, expires, JSON.stringify(files), carousel.text]);
}

export async function shareTikTokUrl(env, id, now = Date.now()) {
  const origin = shareOrigin(env);
  if (!origin || !env.ADMIN_TOKEN) return null;
  const carousel = await approvedCarousel(env, id);
  if (!carousel) return null;
  const expires = now + WEEK;
  const sig = await sign(env, id, expires, carousel);
  return origin + path + '#' + [id, expires, sig].join('.');
}

async function authorize(request, env, now) {
  const origin = shareOrigin(env);
  const caller = request.headers.get('Origin');
  if (!env.DB || !env.ADMIN_TOKEN || !origin || (caller && caller !== origin)) return null;
  let payload;
  try { payload = JSON.parse(await readBounded(request, 1024)); }
  catch { return null; }
  const {id, expires, sig, position} = payload || {};
  if (!/^[a-f0-9]{32}$/.test(id || '') || !/^[a-f0-9]{64}$/.test(sig || '') ||
      !Number.isSafeInteger(expires) || expires <= now || expires > now + WEEK) return null;
  const carousel = await approvedCarousel(env, id);
  if (!carousel || !matchesShareSignature(await sign(env, id, expires, carousel), sig)) return null;
  return {id, carousel, position};
}

export async function shareTikTokRequest(request, env, {fetcher = fetch, now = Date.now, convert, allowStorage} = {}) {
  const route = new URL(request.url).pathname;
  if (route === path && request.method === 'GET') return new Response(page, {headers: pageHeaders});
  if (route === path + '/app.js' && request.method === 'GET') {
    return new Response(client, {headers: {...shareHeaders, 'Content-Type': 'text/javascript; charset=utf-8'}});
  }
  if (!['/data','/image','/draft','/draft-status'].map(suffix => path+suffix).includes(route) || request.method !== 'POST') {
    return new Response('No encontrado', {status: 404, headers: shareHeaders});
  }
  try {
    const result = await authorize(request, env, now());
    if (!result) return new Response('Enlace no válido o caducado', {status: 401, headers: shareHeaders});
    const {id, carousel, position} = result;
    if (route.endsWith('/data')) {
      return Response.json({text: carousel.text, filenames: carousel.assets.map(asset => asset.filename),
        tiktok:await tiktokDraftStatus(env,id,now(),carousel)}, {headers: shareHeaders});
    }
    if (route.endsWith('/draft')) return Response.json(await submitTikTokDraft(env,id,carousel,{fetcher,now,convert,allowStorage}),{headers:shareHeaders});
    if (route.endsWith('/draft-status')) return Response.json(await pollTikTokDraft(env,id,{fetcher,now}),{headers:shareHeaders});
    if (!Number.isInteger(position) || position < 0 || position >= carousel.assets.length) {
      return new Response('Lámina no válida', {status: 400, headers: shareHeaders});
    }
    const url = await telegram(env, network(fetcher)).download(carousel.assets[position].file_id);
    const response = await fetcher(url, {redirect: 'manual', signal: AbortSignal.timeout(15_000)});
    const bytes = await imageBytes(response);
    return new Response(bytes, {headers: {...shareHeaders, 'Content-Type': 'image/png'}});
  } catch {
    return new Response('No se pudo cargar el carrusel. Inténtalo de nuevo.', {status: 503, headers: shareHeaders});
  }
}

const page = `<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>WaCapital · Preparar TikTok</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#090f17;color:#eef5fa;font:16px/1.5 system-ui,sans-serif}main{max-width:610px;margin:auto;padding:28px 20px 48px}.brand{color:#80ead0;font-size:.85rem;font-weight:800;letter-spacing:.15em}h1{line-height:1.16;margin:14px 0;font-size:1.85rem}p{color:#c1cdda}button,a.button{display:block;width:100%;min-height:48px;margin:10px 0;padding:12px;border:1px solid #3e556b;border-radius:12px;background:#152638;color:#eef5fa;text-decoration:none;text-align:center;font:inherit;font-weight:650;cursor:pointer}button.primary{border:0;background:#80ead0;color:#0b211d}button:disabled{opacity:.5;cursor:wait}#text{white-space:pre-wrap;overflow-wrap:anywhere;padding:16px;border:1px solid #2b4355;border-radius:12px;background:#142231}#slides{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:9px}img{display:block;width:100%;aspect-ratio:9/16;border-radius:9px;background:#142231}figure{margin:0;text-align:center;font-size:.88rem}figure a{color:#80ead0;display:block;padding:8px 0}details{margin-top:19px}summary{font-weight:650;cursor:pointer}.small{font-size:.875rem}[hidden]{display:none!important}
</style><script src="/share/tiktok/app.js" defer></script></head><body><main>
<div class="brand">WACAPITAL</div><h1>Tu carrusel de TikTok</h1>
<p id="lead">Prepara las cinco láminas y la descripción para terminar el post en TikTok.</p>
<div id="status" role="status" aria-live="polite">Cargando el carrusel aprobado…</div><button id="retry" hidden>Volver a cargar</button>
<section id="material" hidden>
<section id="direct" hidden><p id="account"></p><button class="primary" id="draft">Enviar borrador a TikTok</button><button id="check" hidden>Comprobar envío</button><a class="button" id="openTikTok" href="https://www.tiktok.com/" target="_blank" rel="noreferrer noopener" hidden>Abrir TikTok · ver Bandeja de entrada</a></section>
<p id="connection" class="small"></p>
<button id="share" disabled>Compartir desde el móvil</button>
<button id="copy">Copiar descripción</button><div id="text"></div>
<h2>Láminas en orden</h2><div id="slides"></div>
<details id="help"><summary>Guardar y cargar en TikTok</summary>
<p>Si TikTok no aparece, comparte solo las láminas y elige guardarlas en Fotos cuando el móvil lo ofrezca. También puedes guardar cada una desde la vista de arriba.</p>
<button id="photos" disabled>Compartir solo las láminas</button>
<p>En TikTok pulsa <strong>+</strong>, carga las cinco imágenes en orden y pega la descripción copiada. Si Telegram no permite compartir, abre esta página en Safari o Chrome.</p>
</details><p class="small">Enlace privado de revisión. Caduca en siete días.</p></section>
</main></body></html>`;

const client = String.raw`'use strict';
const get = id => document.getElementById(id);
const status = get('status');
const [id, expiry, sig] = location.hash.slice(1).split('.');
const access = {id, expires: Number(expiry), sig};
let files = [], urls = [], data;
let checking = null;
let checks = 0;

function draftState(result) {
  const state = result.state;
  if (data) data.tiktok = {...data.tiktok,...result};
  get('lead').textContent = ['setup_required','connection_required'].includes(state)
    ? 'Comparte las cinco láminas y el texto desde tu móvil. Revisa el orden y la descripción en TikTok.'
    : 'Envía las cinco láminas y la descripción a TikTok para terminar el post en la app.';
  get('direct').hidden = ['setup_required','connection_required'].includes(state);
  get('account').textContent = result.account ? 'Cuenta conectada: ' + result.account : get('account').textContent;
  get('draft').hidden = state !== 'ready';
  get('check').hidden = !['busy','preparing','submitting','processing','inbox'].includes(state);
  get('openTikTok').hidden = !['inbox','published','uncertain'].includes(state);
  const messages = {
    setup_required:'La carga automática está pendiente de activar la conexión oficial con TikTok. Puedes compartir desde el móvil o guardar las láminas.',
    connection_required:'Falta conectar o renovar la cuenta. Envía /conectartiktok al bot de Telegram.',
    ready:'Las cinco láminas y la descripción se enviarán como borrador. En TikTok, abre la notificación de la Bandeja de entrada para editar y publicar.',
    busy:'La conexión de TikTok está atendiendo otro envío. Comprueba el estado en unos segundos.',
    preparing:'Preparando las cinco láminas para TikTok. El envío ya está en curso.',
    submitting:'Enviando el borrador a TikTok…',
    processing:'TikTok está procesando las cinco láminas. Puedes comprobar el envío sin cargarlo otra vez.',
    inbox:'TikTok confirmó la entrega. Abre la app → Bandeja de entrada → notificación del borrador para continuar con las láminas y la descripción cargadas.',
    published:'TikTok indica que este material ya se publicó desde su app. Puedes revisar la publicación en tu cuenta.',
    uncertain:'No se pudo confirmar el envío. Revisa la Bandeja de entrada de TikTok antes de cargarlo manualmente.',
    failed:'TikTok no pudo preparar este borrador. Puedes usar las láminas y la descripción de abajo.',
    account_changed:'Este borrador corresponde a otra conexión de TikTok. Comprueba la cuenta antes de continuar.',
    material_changed:'Este material cambió después del envío. Comprueba el borrador anterior en TikTok antes de cargar una nueva versión.',
    daily_limit:'Se alcanzó el límite de cinco envíos en 24 horas. Puedes usar la carga desde el móvil.',
    caption_too_long:'La descripción supera el límite de TikTok. Revisa el texto antes de cargarlo.'
  };
  get('connection').textContent = messages[state] || 'Comprueba el estado del borrador en TikTok.';
  if (['busy','preparing','submitting','processing','inbox','published','uncertain','failed','account_changed','material_changed'].includes(state)) {
    status.textContent = state==='inbox' ? 'Borrador recibido en TikTok.' : state==='published' ? 'Publicación confirmada por TikTok.' : 'Estado del envío: ' + ({busy:'esperando',preparing:'preparando',submitting:'enviando',processing:'procesando',uncertain:'pendiente de comprobar',failed:'interrumpido',account_changed:'comprueba la cuenta',material_changed:'material modificado'}[state]);
  }
  if (result.error === 'url_ownership_unverified') get('connection').textContent = 'TikTok pide verificar el dominio de las imágenes. Mientras se completa, puedes usar la carga desde el móvil.';
  if (result.error === 'spam_risk_too_many_pending_share') get('connection').textContent = 'TikTok tiene demasiados borradores pendientes. Termina uno desde la Bandeja de entrada antes de enviar otro.';
  if (result.error === 'access_token_invalid' || result.error === 'scope_not_authorized') get('connection').textContent = 'TikTok pide renovar el permiso. Envía /conectartiktok al bot de Telegram.';
  clearTimeout(checking);
  if (['busy','preparing','submitting','processing'].includes(state) && !document.hidden && checks<12) checking = setTimeout(checkDraft, 10_000);
}
async function checkDraft() {
  checks++;
  get('check').disabled = true;
  try { draftState(await (await loadResource('draft-status')).json()); }
  catch { get('connection').textContent = 'No se pudo comprobar. Vuelve a pulsar Comprobar envío.'; }
  finally { get('check').disabled = false; }
}

async function loadResource(name, extra = {}) {
  const response = await fetch('/share/tiktok/' + name, {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({...access, ...extra}), cache: 'no-store', credentials: 'omit', redirect: 'error'
  });
  if (!response.ok) throw Error(response.status === 401
    ? 'Enlace caducado o material pendiente de aprobación. Recupera el botón en Telegram.'
    : 'No se pudo cargar el carrusel. Revisa la conexión y vuelve a intentarlo.');
  return response;
}

function canShare(payload) {
  try { return Boolean(navigator.share && navigator.canShare && navigator.canShare(payload)); }
  catch { return false; }
}
function enableSharing() {
  const ready = files.length === 5 && canShare({files});
  get('share').disabled = get('photos').disabled = !ready;
  return ready;
}

async function load() {
  get('retry').hidden = true;
  get('material').hidden = true;
  get('slides').replaceChildren();
  urls.forEach(url => URL.revokeObjectURL(url));
  files = [];
  urls = [];
  data = null;
  clearTimeout(checking);
  enableSharing();
  status.textContent = 'Cargando el carrusel aprobado…';
  try {
    data = await (await loadResource('data')).json();
    draftState(data.tiktok);
    get('text').textContent = data.text;
    get('material').hidden = false;
    files = await Promise.all(data.filenames.map(async (filename, position) => {
      const blob = await (await loadResource('image', {position})).blob();
      return new File([blob], filename, {type: 'image/png'});
    }));
    for (const [index, file] of files.entries()) {
      const url = URL.createObjectURL(file);
      urls.push(url);
      const figure = document.createElement('figure');
      const image = document.createElement('img');
      image.src = url;
      image.alt = 'Lámina ' + (index + 1) + ' de 5';
      const save = document.createElement('a');
      save.href = url;
      save.download = file.name;
      save.textContent = 'Guardar ' + (index + 1);
      figure.append(image, save);
      get('slides').append(figure);
    }
    if (enableSharing()) status.textContent = 'Cinco láminas listas. Abre el menú y elige TikTok si está disponible.';
    else {
      status.textContent = 'Guarda las láminas aquí o abre esta página en Safari o Chrome para compartirlas juntas.';
      get('help').open = true;
    }
    draftState(data.tiktok);
  } catch (error) {
    status.textContent = error.message;
    get('retry').hidden = false;
  }
}

async function share(withText) {
  if (files.length !== 5 || !canShare({files})) return;
  const combined = {files, text: data.text};
  const payload = withText && canShare(combined) ? combined : {files};
  try {
    // Start sharing during the tap, after the images have finished loading.
    const pending = navigator.share(payload);
    get('share').disabled = get('photos').disabled = true;
    await pending;
    status.textContent = 'En TikTok, comprueba las cinco láminas y pega la descripción si quedó vacía.';
  } catch (error) {
    status.textContent = error.name === 'AbortError' ? 'Puedes volver a compartir cuando quieras.' : 'Usa Safari, Chrome o la opción de guardar las láminas.';
    get('help').open = true;
  } finally { enableSharing(); }
}

get('share').addEventListener('click', () => share(true));
get('draft').addEventListener('click', async () => {
  get('draft').disabled = true;
  get('connection').textContent = 'Preparando y enviando el borrador a TikTok…';
  try { draftState(await (await loadResource('draft')).json()); }
  catch {
    get('draft').hidden = true;
    get('check').hidden = false;
    get('connection').textContent = 'La conexión se interrumpió. Pulsa Comprobar envío para conocer el resultado.';
  } finally { get('draft').disabled = false; }
});
get('check').addEventListener('click', checkDraft);
document.addEventListener('visibilitychange', () => { if (document.hidden) clearTimeout(checking); });
get('photos').addEventListener('click', () => share(false));
get('retry').addEventListener('click', load);
get('copy').addEventListener('click', async () => {
  try {
    await navigator.clipboard.writeText(data.text);
    status.textContent = 'Descripción copiada para pegar en TikTok.';
  } catch {
    const range = document.createRange();
    range.selectNodeContents(get('text'));
    const selection = window.getSelection();
    selection.removeAllRanges();
    selection.addRange(range);
    status.textContent = 'Mantén pulsado el texto seleccionado y elige Copiar.';
  }
});
load();
`;
