import {store} from './store.js';
import {network,telegram,readBounded,ServiceError} from './io.js';
import {xPublication} from './publication.js';

const WEEK=7*86_400_000;
const encoder=new TextEncoder();
const headers={'Cache-Control':'private, no-store','Referrer-Policy':'no-referrer','X-Content-Type-Options':'nosniff','X-Robots-Tag':'noindex, nofollow, noarchive','X-Frame-Options':'DENY'};
const origin=env=>{
  try {const u=new URL(env.WORKER_ORIGIN);return u.protocol==='https:'&&u.hostname.endsWith('.workers.dev')&&u.pathname==='/'&&!u.search&&!u.hash?u.origin:null;}catch{return null;}
};
async function signature(env,id,expires,kind,cover,text) {
  const key=await crypto.subtle.importKey('raw',encoder.encode(env.ADMIN_TOKEN),{name:'HMAC',hash:'SHA-256'},false,['sign']);
  const value=['wacapital-share-x-v1',id,expires,kind,cover.file_id,text].join('\n');
  return [...new Uint8Array(await crypto.subtle.sign('HMAC',key,encoder.encode(value)))].map(n=>n.toString(16).padStart(2,'0')).join('');
}
export async function shareXUrl(env,story,cover,kind='c',now=Date.now()) {
  const base=origin(env),x=xPublication(story);
  if(!base||!env.ADMIN_TOKEN||!x.ready||!cover?.file_id||!['c','l'].includes(kind))return null;
  const expires=now+WEEK;
  const sig=await signature(env,story.id,expires,kind,cover,x.text);
  // The capability stays in the fragment, not query strings or request URLs.
  return base+'/share/x#'+[story.id,expires,kind,sig].join('.');
}
export function xButtons(x,shareUrl) {
  return {inline_keyboard:[...(shareUrl?[[{text:'Compartir texto e imagen',url:shareUrl}]]:[]),
    ...(x.composeUrl?[[{text:shareUrl?'Abrir X solo con texto':'Abrir X · adjuntar portada',url:x.composeUrl}]]:[])]};
}
async function approvedPair(env,id,kind) {
  const db=store(env.DB),story=await db.first("SELECT * FROM stories WHERE id=? AND state='approved'",id);
  if(!story)return null;
  const cover=kind==='c'?await db.first("SELECT a.file_id,a.filename FROM channel_assets a JOIN channel_jobs j ON j.story_id=a.story_id AND j.platform=a.platform WHERE a.story_id=? AND a.platform='x' AND a.position=0 AND j.state='approved' AND j.delivery='sent' AND length(a.file_id)>0",id):
    await db.first("SELECT a.file_id,a.filename FROM visual_assets a JOIN visual_jobs j ON j.story_id=a.story_id WHERE a.story_id=? AND a.position=0 AND j.state='approved' AND a.delivery='sent' AND length(a.file_id)>0",id);
  const x=xPublication(story);
  return cover&&x.ready?{story,cover,x}:null;
}
async function authorize(request,env,now) {
  if(!env.DB||!env.ADMIN_TOKEN||!origin(env))return null;
  const requestOrigin=request.headers.get('Origin');
  if(requestOrigin&&requestOrigin!==origin(env))return null;
  let body;
  try {body=JSON.parse(await readBounded(request,1024));}catch{return null;}
  const {id,expires,kind,sig}=body;
  if(!/^[a-f0-9]{32}$/.test(id||'')||!['c','l'].includes(kind)||!Number.isSafeInteger(expires)||expires<=now||expires>now+WEEK||!/^[a-f0-9]{64}$/.test(sig||''))return null;
  const pair=await approvedPair(env,id,kind);
  if(!pair)return null;
  const expected=await signature(env,id,expires,kind,pair.cover,pair.x.text);
  let mismatch=0;
  for(let i=0;i<expected.length;i++)mismatch|=expected.charCodeAt(i)^sig.charCodeAt(i);
  return mismatch?null:pair;
}
async function imageBytes(response) {
  if(!response.ok)throw new ServiceError('share-image-download',response.status);
  if(Number(response.headers.get('content-length')||0)>1_900_000)throw new ServiceError('share-image-size');
  const reader=response.body?.getReader();if(!reader)throw Error('Image unavailable');
  const chunks=[];let size=0;
  try {
    while(true){const {value,done}=await reader.read();if(done)break;size+=value.length;if(size>1_900_000){await reader.cancel();throw Error('Image too large');}chunks.push(value);}
  }finally{reader.releaseLock();}
  const bytes=new Uint8Array(size);let offset=0;
  for(const chunk of chunks){bytes.set(chunk,offset);offset+=chunk.length;}
  if(![137,80,78,71,13,10,26,10].every((n,i)=>bytes[i]===n))throw new ServiceError('share-image-format');
  return bytes;
}
export async function shareXRequest(request,env,{fetcher=fetch,now=Date.now}={}) {
  const path=new URL(request.url).pathname;
  if(path==='/share/x'&&request.method==='GET')return new Response(page,{headers:{...headers,'Content-Type':'text/html; charset=utf-8','Permissions-Policy':'web-share=(self)','Content-Security-Policy':"default-src 'none'; script-src 'self'; style-src 'unsafe-inline'; img-src blob:; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"}});
  if(path==='/share/x/app.js'&&request.method==='GET')return new Response(client,{headers:{...headers,'Content-Type':'text/javascript; charset=utf-8'}});
  if(!['/share/x/data','/share/x/image'].includes(path)||request.method!=='POST')return new Response('No encontrado',{status:404,headers});
  let stage='authorize';
  try {
    const pair=await authorize(request,env,now());
    if(!pair)return new Response('Enlace no válido o caducado. Recupera el material aprobado desde Telegram.',{status:401,headers});
    if(path.endsWith('/data'))return Response.json({text:pair.x.text,weightedLength:pair.x.weightedLength,filename:pair.cover.filename,composeUrl:pair.x.composeUrl},{headers});
    stage='file-lookup';
    const target=await telegram(env,network(fetcher)).download(pair.cover.file_id);
    stage='file-download';
    const response=await fetcher(target,{redirect:'manual',signal:AbortSignal.timeout(15_000)});
    stage='image-read';
    return new Response(await imageBytes(response),{headers:{...headers,'Content-Type':'image/png'}});
  }catch(error){
    if(env.DB)await store(env.DB).set('share_x_last_error',{at:now(),stage,category:error instanceof ServiceError?error.service:'unavailable',status:error instanceof ServiceError?error.status:0}).catch(()=>{});
    return new Response('No se pudo cargar el material. Inténtalo de nuevo.',{status:503,headers});
  }
}

const page=`<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>WaCapital · Compartir en X</title><style>
*{box-sizing:border-box}body{margin:0;background:#090f17;color:#eef5fa;font:17px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}main{max-width:560px;margin:auto;padding:28px 20px 48px}.brand{color:#80ead0;font-weight:800;letter-spacing:2px;font-size:14px}h1{font-size:29px;line-height:1.15;margin:15px 0}p{color:#b9c9d8}#text{white-space:pre-wrap;background:#142231;padding:18px;border:1px solid #2b4355;border-radius:14px;margin:20px 0 8px}#count{color:#80ead0;font-size:14px}img{display:block;width:100%;border-radius:14px;margin:18px 0}button,a.button{display:block;width:100%;padding:15px 12px;border:1px solid #3c5568;border-radius:12px;color:#eef5fa;background:#152638;font:inherit;font-size:16px;line-height:1.3;font-weight:600;text-align:center;text-decoration:none;margin:10px 0;cursor:pointer;min-height:50px}button.primary{background:#80ead0;color:#0b211d;border:0;font-weight:800}button:disabled{opacity:.5;cursor:wait}.small{font-size:14px}.muted{color:#b9c9d8}details{border-top:1px solid #2b4355;margin-top:24px;padding-top:18px}summary{cursor:pointer;font-weight:600}[hidden]{display:none!important}#status{min-height:28px}#retry{width:auto}
</style><script src="/share/x/app.js" defer></script></head><body><main><div class="brand">WACAPITAL</div><h1>Tu publicación, lista para llevar a X</h1><p>Compartir abre el menú del móvil. Elige X y revisa que lleguen el texto y la portada antes de publicar.</p><div id="status" role="status" aria-live="polite">Cargando el material aprobado…</div><button id="retry" hidden>Volver a cargar</button><section id="material" hidden><div id="text"></div><div id="count"></div><button class="primary" id="share" disabled>Compartir texto e imagen</button><img id="cover" alt="Portada aprobada de esta publicación"><p class="small" id="hint">En iPhone, si no aparece X o falla compartir, abre esta página en Safari desde el menú de Telegram. En Android, prueba Chrome.</p><details><summary>Si X no recibe ambas cosas</summary><p class="small">Puedes copiar el texto y compartir solo la imagen. Si X deja el texto vacío, pégalo en su borrador.</p><button id="copy">Copiar texto</button><button id="imageShare">Compartir solo la imagen</button><a class="button" id="save" download>Guardar portada</a><a class="button" id="openX" target="_blank" rel="noreferrer noopener">Abrir X con el texto</a><p class="small">Si la imagen se abre en una pestaña en iPhone, mantenla pulsada para guardarla en Fotos y adjúntala desde X.</p></details><p class="small muted">Este enlace contiene material privado aprobado y caduca en siete días. No se añade al tuit. Esta página no publica por ti.</p></section></main></body></html>`;

export const shareClient=`'use strict';
const $=id=>document.getElementById(id),status=$('status');
let file=null,data=null,blobUrl=null;
const parts=location.hash.slice(1).split('.');
const credentials={id:parts[0],expires:Number(parts[1]),kind:parts[2],sig:parts[3]};
async function post(path){const r=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(credentials),cache:'no-store',credentials:'omit',redirect:'error'});if(!r.ok)throw Error(r.status===401?'Este enlace ha caducado o el material ya no está aprobado. Recupéralo desde Telegram.':'No se pudo cargar el material. Comprueba la conexión y vuelve a cargar.');return r;}
async function load(){
 $('retry').hidden=true;status.textContent='Cargando el material aprobado…';$('share').disabled=true;$('imageShare').disabled=true;$('material').hidden=true;file=null;data=null;
 try{
  data=await(await post('/share/x/data')).json();
  $('text').textContent=data.text;$('count').textContent=data.weightedLength+'/280 caracteres ponderados';$('openX').href=data.composeUrl;
  $('material').hidden=false;$('cover').hidden=true;$('save').hidden=true;
  const blob=await(await post('/share/x/image')).blob();
  file=new File([blob],data.filename,{type:'image/png'});
  if(blobUrl)URL.revokeObjectURL(blobUrl);blobUrl=URL.createObjectURL(file);
  $('text').textContent=data.text;$('count').textContent=data.weightedLength+'/280 caracteres ponderados';
  $('cover').src=blobUrl;$('cover').hidden=false;$('save').hidden=false;$('save').href=blobUrl;$('save').download=data.filename;$('openX').href=data.composeUrl;$('imageShare').disabled=false;
  $('material').hidden=false;
  const available=Boolean(navigator.share&&navigator.canShare&&navigator.canShare({files:[file],text:data.text}));
  $('share').disabled=!available;
  status.textContent=available?'Listo. Toca compartir y elige X.':'Este navegador no permite compartir el archivo. Abre la página en Safari o Chrome, o usa las opciones de abajo.';
  if(!available)document.querySelector('details').open=true;
 }catch(e){$('material').hidden=!data;if(data)document.querySelector('details').open=true;status.textContent=e.message;$('retry').hidden=false;}
}
async function share(onlyImage){
 if(!file||!data)return;
 if(!navigator.share||!navigator.canShare||!navigator.canShare({files:[file]})){status.textContent='Abre esta página en Safari o Chrome, o guarda la portada.';return;}
 // The file is ready before the tap: no fetch/clipboard await can consume iOS activation.
 try{
  const result=navigator.share(onlyImage?{files:[file]}:{files:[file],text:data.text});
  $('share').disabled=true;$('imageShare').disabled=true;
  await result;
  status.textContent='Revisa el borrador en X y comprueba texto e imagen. Desde aquí no podemos confirmar una publicación.';
 }catch(e){status.textContent=e.name==='AbortError'?'Compartir cancelado. Puedes volver a intentarlo.':'No se pudo compartir. Prueba Safari o copia el texto y guarda la portada.';document.querySelector('details').open=true;}
 finally{$('share').disabled=!(navigator.share&&navigator.canShare&&navigator.canShare({files:[file],text:data.text}));$('imageShare').disabled=false;}
}
$('share').addEventListener('click',()=>share(false));$('imageShare').addEventListener('click',()=>share(true));$('retry').addEventListener('click',load);
$('copy').addEventListener('click',async()=>{try{await navigator.clipboard.writeText(data.text);status.textContent='Texto copiado. Si hace falta, pégalo en el borrador de X.';}catch{const range=document.createRange();range.selectNodeContents($('text'));const selection=window.getSelection();selection.removeAllRanges();selection.addRange(range);status.textContent='Mantén pulsado el texto seleccionado y elige Copiar.';}});
load();`;
const client=shareClient;
