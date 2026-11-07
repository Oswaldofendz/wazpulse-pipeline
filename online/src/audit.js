const headers = {
  'Cache-Control': 'no-store',
  'Referrer-Policy': 'no-referrer',
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options': 'DENY',
  'X-Robots-Tag': 'noindex, nofollow',
  'Content-Security-Policy': "default-src 'none'; script-src 'self'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"
};

export function auditRequest(request) {
  if (request.method !== 'GET') return new Response('No encontrado', {status: 404, headers});
  const script = new URL(request.url).pathname.endsWith('/app.js');
  return new Response(script ? client : page, {headers: {
    ...headers, 'Content-Type': script ? 'text/javascript; charset=utf-8' : 'text/html; charset=utf-8'
  }});
}

const page = `<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>WaCapital · Auditor de redes</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#0b111a;color:#f0f5fa;font:16px/1.5 system-ui,sans-serif}main{max-width:940px;margin:auto;padding:28px 16px 48px}.brand{color:#83e7cd;font-size:.8rem;letter-spacing:.2em;font-weight:800}h1,h2,h3{line-height:1.15}h1{font-size:clamp(1.8rem,6vw,2.6rem);margin:12px 0}h2{font-size:1.25rem;margin:0 0 12px}h3{font-size:1rem;margin:0 0 6px}p{color:#c0cedc;margin:.6em 0}a{color:#83e7cd}button,input,select{font:inherit;min-height:44px;border:1px solid #40566d;border-radius:9px;color:#f0f5fa;background:#182638;padding:8px 10px}button{cursor:pointer;font-weight:650}button.primary,button[aria-pressed="true"]{background:#83e7cd;color:#0a241e;border-color:#83e7cd}input,select{width:100%;min-width:0}label{display:block;font-size:.9rem;color:#d2dfeb}label input,label select{display:block;margin-top:5px}fieldset{border:0;margin:0;padding:0}form{display:grid;gap:10px}.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.grid,.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(148px,100%),1fr));gap:10px}.panel,.card{border:1px solid #2a4056;background:#121e2d;border-radius:13px;padding:16px;min-width:0}.panel{margin:12px 0}.card b{font-size:1.4rem;display:block;line-height:1.2}.small,small{font-size:.875rem;color:#c0cedc}.muted{color:#c0cedc}.entry{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:8px;border-top:1px solid #2a4056;padding:10px 0;overflow-wrap:anywhere}.entry button{align-self:center}.entry p{margin:4px 0}meter{width:100%;height:12px;display:block}details{margin-top:10px}summary{cursor:pointer;font-weight:650;min-height:38px;padding:6px 0}#status{min-height:24px}#filter{width:auto}#restore{max-width:100%}[hidden]{display:none!important}
</style><script src="/auditoria/app.js" defer></script></head><body><main>
<div class="brand">WACAPITAL</div><h1>Auditor de redes</h1>
<p>Registra resultados de tus publicaciones y compara enfoques dentro de cada cuenta. El historial vive en este navegador.</p>
<nav class="row" aria-label="Red social">
<button type="button" data-platform="x" aria-pressed="true">X</button>
<button type="button" data-platform="instagram" aria-pressed="false">Instagram</button>
<button type="button" data-platform="tiktok" aria-pressed="false">TikTok</button>
<a id="analytics" target="_blank" rel="noopener noreferrer">Abrir estadísticas</a>
</nav>
<div id="status" role="status" aria-live="polite"></div>
<section class="panel"><div class="row"><h2 id="network"></h2><label>Medición
<select id="filter"><option value="24h">Primeras 24 horas</option><option value="7d">Primeros 7 días</option><option value="total">Acumulada</option></select></label></div>
<div class="cards" id="metrics"></div>
<p class="small">La interacción usa me gusta, comentarios y compartidos. Para medir difusión, observa los compartidos por cada 1.000 impresiones o vistas.</p>
</section>
<section class="panel"><h2>Registrar una publicación</h2>
<form id="post">
<label>Enlace del post<input name="url" type="url" required maxlength="320" placeholder="https://…"></label>
<div class="grid"><label>Fecha de publicación<input name="date" type="date" required></label>
<label>Ventana<select name="window"><option value="24h">Primeras 24 horas</option><option value="7d">Primeros 7 días</option><option value="total">Acumulada</option></select></label>
<label>Enfoque o formato<select name="variant" id="variant"></select></label>
<label>Tema o etiqueta<input name="topic" maxlength="65" placeholder="Bitcoin, Fed, #Nasdaq…"></label></div>
<div class="grid"><label><span id="viewLabel">Impresiones</span><input name="views" type="number" min="0" max="999999999999" step="1" inputmode="numeric" required></label>
<label>Me gusta<input name="likes" type="number" min="0" max="999999999999" step="1" inputmode="numeric"></label>
<label>Comentarios / respuestas<input name="comments" type="number" min="0" max="999999999999" step="1" inputmode="numeric"></label>
<label>Compartidos / reposts<input name="shares" type="number" min="0" max="999999999999" step="1" inputmode="numeric"></label></div>
<div class="grid" id="videoMetrics" hidden><label>Tiempo medio visto (s)<input name="watch" type="number" min="0" max="86400" step="0.01" inputmode="decimal"></label>
<label>Vieron completo (%)<input name="completion" type="number" min="0" max="100" step="0.01" inputmode="decimal"></label></div>
<button class="primary" type="submit">Guardar medición</button>
</form>
</section>
<section class="panel"><h2>Seguidores</h2><p id="followers"></p>
<form id="follow"><div class="grid"><label>Fecha del corte<input name="date" type="date" required></label>
<label>Total de seguidores<input name="count" type="number" min="0" max="999999999999" step="1" required inputmode="numeric"></label></div>
<button type="submit">Guardar corte de seguidores</button></form>
</section>
<section class="panel"><h2>Comparar publicaciones</h2><label>Agrupar por
<select id="groupBy"><option value="variant">Enfoque o formato</option><option value="topic">Tema o etiqueta</option></select></label><div id="groups"></div></section>
<section class="panel"><h2>Publicaciones por rendimiento</h2><div id="posts"></div></section>
<details class="panel"><summary>Copia de seguridad</summary><p>Exporta el historial para conservarlo o llevarlo a otro móvil. Importar fusiona las mediciones por enlace y ventana, y los seguidores por fecha.</p>
<button type="button" id="export">Exportar historial</button>
<label>Importar historial del auditor<input type="file" id="restore" accept=".json,application/json"></label></details>
</main></body></html>`;

const client = String.raw`'use strict';
const config = {
  x: {name:'X', views:'Impresiones', variants:['Directo','Pregunta','Con etiqueta','Otro'], analytics:'https://analytics.x.com/'},
  instagram: {name:'Instagram', views:'Visualizaciones', variants:['Carrusel','Reel','Imagen','Otro'], analytics:'https://www.instagram.com/professional_dashboard/'},
  tiktok: {name:'TikTok', views:'Visualizaciones', variants:['Carrusel','Vídeo','Otro'], analytics:'https://www.tiktok.com/tiktokstudio'}
};
const storageKey = 'wacapital-auditoria-v1';
const maxRows = 1000;
const get = id => document.getElementById(id);
const number = new Intl.NumberFormat('es', {maximumFractionDigits:2});
const fmt = value => number.format(value);
const windows = ['24h','7d','total'];
const postForm = get('post'), followForm = get('follow'), filter = get('filter');
const today = new Date().toLocaleDateString('sv-SE');
let platform = 'x', history = {version:1, posts:[], followers:[]};

function report(message) { get('status').textContent = message; }
function node(tag, text, className) {
  const item = document.createElement(tag);
  if (text !== undefined) item.textContent = text;
  if (className) item.className = className;
  return item;
}
function date(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value) ||
      Number.isNaN(Date.parse(value)) || new Date(value).toISOString().slice(0,10) !== value || value > today) {
    throw Error('Revisa la fecha. Debe ser válida y no futura.');
  }
  return value;
}
function metric(value, {optional=false, max=999999999999, decimal=false}={}) {
  if ((value === '' || value === null || value === undefined) && optional) return null;
  if ((typeof value !== 'string' && typeof value !== 'number') || value === '' ||
      !/^\d+(?:\.\d+)?$/.test(String(value))) throw Error('Revisa las cifras de la medición.');
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0 || n > max || (!decimal && !Number.isSafeInteger(n))) {
    throw Error('Revisa las cifras de la medición.');
  }
  return n;
}
function postUrl(value, network) {
  let url;
  try { url = new URL(value); } catch { throw Error('Introduce el enlace completo del post.'); }
  const host = url.hostname.replace(/^www\./,'');
  const pathname = url.pathname.replace(/\/+$/,'');
  const safe = url.protocol === 'https:' && !url.username && !url.password && !url.port;
  if (safe && network === 'tiktok' &&
      ((['vm.tiktok.com','vt.tiktok.com'].includes(host) && /^\/[a-z0-9_-]+$/i.test(pathname)) ||
        (host === 'tiktok.com' && /^\/t\/[a-z0-9_-]+$/i.test(pathname)))) {
    return 'https://' + url.hostname + pathname + '/';
  }
  const match = {
    x: /^(?:\/[a-z0-9_]{1,15}\/status\/|\/i\/web\/status\/)(\d+)$/i,
    instagram: /^\/(?:p|reel|tv)\/([a-z0-9_-]+)$/i,
    tiktok: /^\/@[\w.-]+\/(?:video|photo)\/(\d+)$/i
  };
  const hosts = {x:['x.com','twitter.com'], instagram:['instagram.com'], tiktok:['tiktok.com']};
  if (!safe || !hosts[network].includes(host) || !match[network].test(pathname)) {
    throw Error('Usa el enlace completo de una publicación de ' + config[network].name + '.');
  }
  if (network === 'x') return 'https://x.com' + pathname;
  return 'https://www.' + host + pathname + (network === 'instagram' ? '/' : '');
}
function cleanPost(row) {
  const network = row.platform;
  if (!Object.hasOwn(config, network) || !windows.includes(row.window) ||
      !config[network].variants.includes(row.variant)) throw Error('Red, ventana o enfoque no válido.');
  return {
    platform:network, url:postUrl(row.url,network), date:date(row.date), window:row.window, variant:row.variant,
    topic:String(row.topic || '').trim().slice(0,65), views:metric(row.views),
    likes:metric(row.likes,{optional:true}), comments:metric(row.comments,{optional:true}),
    shares:metric(row.shares,{optional:true}),
    watch:network === 'tiktok' ? metric(row.watch,{optional:true,max:86400,decimal:true}) : null,
    completion:network === 'tiktok' ? metric(row.completion,{optional:true,max:100,decimal:true}) : null
  };
}
function cleanFollow(row) {
  if (!Object.hasOwn(config, row.platform)) throw Error('Red no válida.');
  return {platform:row.platform, date:date(row.date), count:metric(row.count)};
}
const postKey = row => [row.platform,row.url,row.window].join('|');
const followKey = row => [row.platform,row.date].join('|');
function merge(old, fresh, key) {
  const index = new Map(old.map(row => [key(row),row]));
  for (const row of fresh) index.set(key(row),row);
  if (index.size > maxRows) throw Error('El historial admite hasta 1.000 registros de cada tipo.');
  return [...index.values()];
}
function cleanHistory(data) {
  if (data?.version !== 1 || !Array.isArray(data.posts) || !Array.isArray(data.followers) ||
      data.posts.length > maxRows || data.followers.length > maxRows) throw Error('El archivo debe ser una copia del auditor.');
  return {version:1, posts:merge([],data.posts.map(cleanPost),postKey), followers:merge([],data.followers.map(cleanFollow),followKey)};
}
function save(next) {
  try { localStorage.setItem(storageKey,JSON.stringify(next)); }
  catch { throw Error('El navegador no pudo guardar los cambios. Abre el auditor en tu navegador habitual.'); }
  history = next;
}

function stats(rows) {
  const values = rows.map(row=>row.views).sort((a,b)=>a-b);
  const half = Math.floor(values.length/2);
  const median = values.length === 0 ? null : (values.length%2 ? values[half] : (values[half-1]+values[half])/2);
  const comparable = rows.filter(row => row.views > 0 && [row.likes,row.comments,row.shares].every(value=>value!==null));
  const exposure = comparable.reduce((total,row)=>total+row.views,0);
  const actions = comparable.reduce((total,row)=>total+row.likes+row.comments+row.shares,0);
  const sharing = rows.filter(row=>row.views>0 && row.shares!==null);
  const shareViews = sharing.reduce((total,row)=>total+row.views,0);
  const shares = sharing.reduce((total,row)=>total+row.shares,0);
  return {median, rate:exposure>0?100*actions/exposure:null, measured:comparable.length,
    shareRate:shareViews>0?1000*shares/shareViews:null, shareSample:sharing.length};
}
function card(label,value,note) {
  const result = node('div',undefined,'card');
  result.append(node('small',label),node('b',value));
  if (note) result.append(node('small',note));
  return result;
}
function render() {
  const current = config[platform];
  get('network').textContent = current.name;
  get('analytics').href = current.analytics;
  get('viewLabel').textContent = current.views;
  get('videoMetrics').hidden = platform !== 'tiktok';
  document.querySelectorAll('[data-platform]').forEach(button =>
    button.setAttribute('aria-pressed',String(button.dataset.platform===platform)));
  const rows = history.posts.filter(row=>row.platform===platform && row.window===filter.value);
  const result = stats(rows);
  get('metrics').replaceChildren(
    card('Posts medidos',fmt(rows.length)),
    card('Mediana de '+current.views.toLowerCase(),result.median===null?'—':fmt(result.median)),
    card('Tasa de interacción',result.rate===null?'—':fmt(result.rate)+'%', result.measured ? fmt(result.measured)+' posts con datos completos' : 'Sin datos completos'),
    card('Compartidos por 1.000',result.shareRate===null?'—':fmt(result.shareRate),result.shareSample ? fmt(result.shareSample)+' posts con compartidos y exposición' : 'Sin datos completos')
  );
  const cuts = history.followers.filter(row=>row.platform===platform).sort((a,b)=>b.date.localeCompare(a.date));
  get('followers').textContent = !cuts.length ? 'Agrega un primer corte para empezar a medir crecimiento.' :
    fmt(cuts[0].count)+' seguidores el '+cuts[0].date+(cuts[1] ?
      '. Cambio desde '+cuts[1].date+' de '+(cuts[0].count>=cuts[1].count?'+':'')+fmt(cuts[0].count-cuts[1].count)+'.' :
      '. Guarda otro corte para ver la evolución.');
  const samples = new Map();
  const groupBy = get('groupBy').value;
  for (const row of rows) {
    const name = row[groupBy] || 'Sin tema';
    const key = name.toLocaleLowerCase('es');
    if (!samples.has(key)) samples.set(key, {name, rows:[]});
    samples.get(key).rows.push(row);
  }
  const groups = [...samples.values()].map(group => ({
    name:group.name, count:group.rows.length, ...stats(group.rows)
  })).sort((a,b)=>b.median-a.median);
  get('groups').replaceChildren();
  if (!groups.length) get('groups').append(node('p','Las comparaciones aparecerán al guardar mediciones.'));
  for (const group of groups) {
    const div = node('div',undefined,'entry');
    const content = node('div');
    const rate = group.rate===null ? '' : ' · '+fmt(group.rate)+'% interacción ('+group.measured+' medidos)';
    const shares = group.shareRate===null ? '' : ' · '+fmt(group.shareRate)+' compartidos por 1.000';
    const bar = node('meter');
    bar.max = Math.max(1,groups[0].median);bar.value=group.median;bar.setAttribute('aria-label','Mediana de '+group.name);
    content.append(node('h3',group.name),node('p',group.count+' posts · mediana '+fmt(group.median)+rate+shares,'small'),bar);
    div.append(content);
    get('groups').append(div);
  }
  if (groups.length) get('groups').append(node('p','Compara la misma ventana. Con menos de tres posts por grupo, toma el resultado como una señal inicial.','small'));
  const posts = get('posts');
  posts.replaceChildren();
  if (!rows.length) posts.append(node('p','Aún no hay publicaciones para esta red y ventana.'));
  for (const row of rows.sort((a,b)=>b.views-a.views).slice(0,30)) {
    const entry = node('div',undefined,'entry'), content = node('div');
    const link = node('a',row.topic || row.variant + ' · ' + row.date);
    link.href=row.url;link.target='_blank';link.rel='noopener noreferrer';
    const info = [row.variant,fmt(row.views)+' '+current.views.toLowerCase()];
    if (row.shares!==null) info.push(fmt(row.shares)+' compartidos');
    if (row.watch!==null) info.push(fmt(row.watch)+' s de media');
    if (row.completion!==null) info.push(fmt(row.completion)+'% completado');
    content.append(link,node('p',info.join(' · '),'small'));
    const edit = node('button','Editar');
    edit.type='button';edit.addEventListener('click',()=>{
      for (const [name,value] of Object.entries(row)) if (postForm.elements.namedItem(name)) {
        postForm.elements.namedItem(name).value = value ?? '';
      }
      postForm.scrollIntoView({behavior:'smooth'});
      postForm.elements.url.focus();
    });
    entry.append(content,edit);posts.append(entry);
  }
  if (rows.length>30) posts.append(node('p','Se muestran los 30 posts con mayor exposición. La copia incluye todo el historial.','small'));
}
function selectPlatform(network) {
  platform=network;
  postForm.reset();followForm.reset();
  postForm.elements.date.value=followForm.elements.date.value=today;
  postForm.elements.window.value=filter.value;
  get('variant').replaceChildren(...config[network].variants.map(label=>new Option(label,label)));
  render();
}

try {
  const saved=localStorage.getItem(storageKey);
  if (saved) history=cleanHistory(JSON.parse(saved));
} catch { report('No se pudo leer el historial de este navegador. Puedes restaurar una copia.'); }
document.querySelectorAll('[data-platform]').forEach(button=>button.addEventListener('click',()=>selectPlatform(button.dataset.platform)));
filter.addEventListener('change',()=>{postForm.elements.window.value=filter.value;render();});
get('groupBy').addEventListener('change',render);
postForm.addEventListener('submit',event=>{
  event.preventDefault();
  try {
    const post=cleanPost({...Object.fromEntries(new FormData(postForm)),platform});
    save({...history, posts:merge(history.posts,[post],postKey)});
    filter.value=post.window;
    selectPlatform(platform);
    report('Medición guardada.');
  } catch (error) { report(error.message); }
});
followForm.addEventListener('submit',event=>{
  event.preventDefault();
  try {
    const cut=cleanFollow({...Object.fromEntries(new FormData(followForm)),platform});
    save({...history,followers:merge(history.followers,[cut],followKey)});
    render();report('Corte de seguidores guardado.');
  } catch (error) { report(error.message); }
});
get('export').addEventListener('click',()=>{
  const url=URL.createObjectURL(new Blob([JSON.stringify(history,null,2)],{type:'application/json'}));
  const link=node('a');link.href=url;link.download='WaCapital-auditoria-'+today+'.json';
  document.body.append(link);link.click();link.remove();
  setTimeout(()=>URL.revokeObjectURL(url),30000);
});
get('restore').addEventListener('change',async event=>{
  const file=event.target.files[0];
  if (!file) return;
  try {
    if (file.size > 2_000_000) throw Error('La copia supera los 2 MB.');
    const copy=cleanHistory(JSON.parse(await file.text()));
    save({...history,posts:merge(history.posts,copy.posts,postKey),followers:merge(history.followers,copy.followers,followKey)});
    render();report('Historial importado.');
  } catch(error) { report('No se pudo importar. '+error.message); }
  finally {event.target.value='';}
});
selectPlatform('x');
`;
