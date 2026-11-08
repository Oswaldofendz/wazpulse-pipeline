import {createServer} from 'node:http';
import {randomBytes,timingSafeEqual} from 'node:crypto';
import {readFileSync,writeFileSync} from 'node:fs';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {resolve,dirname} from 'node:path';
import {loadManagedSecrets} from './managed-secrets.mjs';
import {tiktokConnectUrl} from '../src/tiktok-auth.js';

const root=dirname(dirname(fileURLToPath(import.meta.url)));
const workerOrigin='https://wacapital-online.wacapital-online.workers.dev';
const headers={'Cache-Control':'no-store','Referrer-Policy':'no-referrer','X-Content-Type-Options':'nosniff','X-Frame-Options':'DENY','X-Robots-Tag':'noindex, nofollow',
  'Content-Security-Policy':"default-src 'none'; script-src 'self'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"};
const phaseMessages={saving:'Guardando las claves de Sandbox…',backup:'Guardando una copia cifrada en este PC…',cloudflare:'Guardando las claves en el servidor de WaCapital…',deploying:'Preparando la conexión de prueba del bot…',checking:'Comprobando la conexión…'};

function command(executable,args,input) {
  return new Promise((resolveResult,reject)=>{
    const child=spawn(executable,args,{cwd:root,windowsHide:true,stdio:['pipe','pipe','pipe'],env:{...process.env,WRANGLER_SEND_METRICS:'false'}});
    let output='',size=0,finished=false;
    const timer=setTimeout(()=>{if(!finished){child.kill();reject(Error('command_timeout'));}},120_000);
    child.on('error',()=>{clearTimeout(timer);reject(Error('command_unavailable'));});
    child.stdout.on('data',chunk=>{size+=chunk.length;if(size<=2_000_000)output+=chunk.toString('utf8');});
    child.stderr.resume();
    child.stdin.on('error',()=>{});
    child.on('close',code=>{finished=true;clearTimeout(timer);code===0?resolveResult(output.trim()):reject(Error('command_failed'));});
    child.stdin.end(input);
  });
}

async function installSandbox(values,onPhase) {
  const configFile=resolve(root,'wrangler.local.jsonc');
  const config=JSON.parse(readFileSync(configFile,'utf8'));
  if(config.name!=='wacapital-online'||config.vars?.WORKER_ORIGIN!==workerOrigin)throw Error('unexpected_worker');
  if(config.vars.TIKTOK_MODE!=='sandbox'||config.vars.TIKTOK_MEDIA_VERIFIED!=='true')throw Error('sandbox_not_prepared');
  onPhase('backup');
  const encrypted=await command('pwsh.exe',['-NoProfile','-NonInteractive','-Command',
    "$value=[Console]::In.ReadToEnd(); $secure=ConvertTo-SecureString -String $value -AsPlainText -Force; ConvertFrom-SecureString -SecureString $secure"],JSON.stringify(values));
  writeFileSync(resolve(root,'.tiktok-sandbox-secrets.dpapi'),encrypted,{mode:0o600});
  onPhase('cloudflare');
  const wrangler=resolve(root,'node_modules/wrangler/bin/wrangler.js');
  await command(process.execPath,[wrangler,'secret','bulk','--config','wrangler.local.jsonc'],JSON.stringify(values));
  // Keep every other private setting intact. No change to Telegram, quotas,
  // credentials or the editorial approvals is accepted from this form.
  config.vars.TIKTOK_UPLOAD_ENABLED='true';
  writeFileSync(configFile,JSON.stringify(config,null,2)+'\n');
  onPhase('deploying');
  await command(process.execPath,[wrangler,'deploy','--config','wrangler.local.jsonc']);
  onPhase('checking');
  const internal=loadManagedSecrets();
  const response=await fetch(workerOrigin+'/admin/tiktok-status',{headers:{Authorization:'Bearer '+internal.ADMIN_TOKEN},redirect:'error',signal:AbortSignal.timeout(15_000)});
  const status=await response.json();
  if(!response.ok||status.mode!=='sandbox'||!status.configured||!status.mediaVerified||!status.uploadEnabled)throw Error('configuration_unconfirmed');
  const connectUrl=await tiktokConnectUrl({...values,ADMIN_TOKEN:internal.ADMIN_TOKEN,WORKER_ORIGIN:workerOrigin});
  if(!connectUrl)throw Error('connection_unavailable');
  return {connectUrl};
}

export async function startTikTokSetup({install=installSandbox,ttlMs=30*60_000,onReady=()=>{}}={}) {
  const token=randomBytes(32).toString('hex');
  let origin,state={state:'waiting'},closed=false;
  const auth=request=>{
    const actual=Buffer.from(request.headers.authorization||''),expected=Buffer.from('Bearer '+token);
    return actual.length===expected.length&&timingSafeEqual(actual,expected);
  };
  const send=(response,status,value,type='application/json; charset=utf-8')=>{
    response.writeHead(status,{...headers,'Content-Type':type});response.end(typeof value==='string'?value:JSON.stringify(value));
  };
  const server=createServer(async(request,response)=>{
    try {
      if(request.headers.host!==new URL(origin).host)return send(response,403,{error:'host_denied'});
      if(request.headers.origin&&request.headers.origin!==origin)return send(response,403,{error:'origin_denied'});
      const path=new URL(request.url,origin).pathname;
      if(request.method==='GET'&&path==='/')return send(response,200,page,'text/html; charset=utf-8');
      if(request.method==='GET'&&path==='/app.js')return send(response,200,client,'text/javascript; charset=utf-8');
      if(!auth(request))return send(response,401,{error:'link_expired'});
      if(request.method==='GET'&&path==='/status')return send(response,200,state);
      if(request.method!=='POST'||path!=='/install')return send(response,404,{error:'not_found'});
      if(request.headers.origin!==origin)return send(response,403,{error:'origin_denied'});
      if(request.headers['content-type']!=='application/json')return send(response,415,{error:'json_required'});
      if(['saving','ready'].includes(state.state))return send(response,409,{error:'already_started'});
      let size=0,body='';
      for await(const chunk of request){size+=chunk.length;if(size>4096)return send(response,413,{error:'body_too_large'});body+=chunk.toString('utf8');}
      let input;try{input=JSON.parse(body);}catch{return send(response,400,{error:'invalid_input'});}
      const key=typeof input.clientKey==='string'?input.clientKey.trim():'',secret=typeof input.clientSecret==='string'?input.clientSecret.trim():'';
      if(input.sandbox!==true||!/^[A-Za-z0-9._~+\/=-]{6,256}$/.test(key)||!/^[A-Za-z0-9._~+\/=-]{8,1024}$/.test(secret))return send(response,400,{error:'check_sandbox_fields'});
      if(['saving','ready'].includes(state.state))return send(response,409,{error:'already_started'});
      state={state:'saving',message:phaseMessages.saving};
      send(response,202,state);
      void Promise.resolve().then(()=>install({TIKTOK_CLIENT_KEY:key,TIKTOK_CLIENT_SECRET:secret},phase=>{
        if(!closed)state={state:'saving',message:phaseMessages[phase]||phaseMessages.saving};
      })).then(result=>{
        if(closed)return;
        const url=new URL(result.connectUrl);
        if(url.origin!==workerOrigin||url.pathname!=='/tiktok/connect'||!/^#\d{13}\.[a-f0-9]{64}$/.test(url.hash))throw Error('unexpected_connection');
        state={state:'ready',connectUrl:url.href};onReady();
      }).catch(()=>{if(!closed)state={state:'error',message:'No se pudo confirmar la configuración. Puedes volver a introducir las claves de Sandbox.'};});
    } catch {if(!response.headersSent)send(response,400,{error:'request_unavailable'});else response.end();}
  });
  server.requestTimeout=15_000;server.headersTimeout=10_000;server.maxHeadersCount=30;server.maxConnections=12;
  await new Promise((resolveListen,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolveListen);});
  origin='http://127.0.0.1:'+server.address().port;
  const stop=()=>new Promise(resolveClose=>{closed=true;server.close(resolveClose);server.closeIdleConnections();});
  const timer=setTimeout(()=>{void stop();},ttlMs);timer.unref();
  return {url:origin+'/#'+token,token,origin,stop:async()=>{clearTimeout(timer);await stop();}};
}

const page=`<!doctype html><html lang="es"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>WaCapital · Claves de Sandbox</title><style>*{box-sizing:border-box}body{margin:0;background:#090f17;color:#eef5fa;font:17px/1.5 system-ui}main{max-width:620px;margin:auto;padding:28px 22px 48px}.brand{color:#80ead0;font-weight:800;letter-spacing:2px}h1{font-size:29px;line-height:1.2}p{color:#c1cdda}label{display:block;margin:18px 0 7px}input[type=password]{width:100%;padding:13px;color:inherit;background:#142231;border:1px solid #53667b;border-radius:10px;font:inherit}button,a{display:block;width:100%;margin:20px 0;padding:14px;border:0;border-radius:12px;background:#80ead0;color:#0b211d;font:inherit;font-weight:700;text-align:center;text-decoration:none;cursor:pointer}.check{display:flex;gap:12px;align-items:flex-start}.check input{margin-top:6px}button:disabled{opacity:.5;cursor:wait}[hidden]{display:none!important}.small{font-size:14px}#status{white-space:pre-wrap}</style><script src="/app.js" defer></script><main><div class="brand">WACAPITAL</div><h1>Conectar Sandbox</h1><p>En TikTok for Developers, entra en <strong>Sandbox → App details → Credentials</strong>. Copia los dos valores de esa sección en este formulario.</p><p>Se guardarán en el servidor del bot de WaCapital en <strong>Cloudflare</strong> y en una copia cifrada en este PC. Esta página funciona solo en este ordenador.</p><form id="form" autocomplete="off"><label for="key">Client key</label><input id="key" type="password" autocomplete="off" required maxlength="256"><label for="secret">Client secret</label><input id="secret" type="password" autocomplete="off" required maxlength="1024"><label class="check"><input id="sandbox" type="checkbox" required><span>Estoy copiando las claves de <strong>Sandbox</strong> de WaCapital Publisher.</span></label><button id="save">Guardar claves de Sandbox</button></form><p id="status" role="status" aria-live="polite"></p><section id="done" hidden><h2>Claves guardadas</h2><p>Ahora conecta tu cuenta de WaCapital. En TikTok podrás comprobar y autorizar los permisos.</p><a id="connect" target="_blank" rel="noreferrer noopener">Continuar con TikTok</a></section><p class="small">Es una conexión de prueba. El envío de un carrusel se confirma aparte desde su botón. El formulario caduca en 30 minutos.</p></main></html>`;

const client=String.raw`'use strict';
const $=id=>document.getElementById(id),token=location.hash.slice(1);
let timer;
async function request(path,body){const r=await fetch(path,{method:body?'POST':'GET',headers:{Authorization:'Bearer '+token,...(body?{'Content-Type':'application/json'}:{})},...(body?{body:JSON.stringify(body)}:{}),cache:'no-store',credentials:'omit',redirect:'error'});if(!r.ok)throw Error();return r.json();}
function render(data){clearTimeout(timer);if(data.state==='ready'){$('form').hidden=true;$('done').hidden=false;$('connect').href=data.connectUrl;$('status').textContent='La conexión de Sandbox ya está preparada.';}else if(data.state==='saving'){$('save').disabled=true;$('status').textContent=data.message;timer=setTimeout(poll,3000);}else if(data.state==='error'){$('save').disabled=false;$('status').textContent=data.message;}else{$('save').disabled=false;$('status').textContent='Pega las dos claves de la sección Sandbox para continuar.';}}
async function poll(){try{render(await request('/status'));}catch{$('status').textContent='El formulario ha caducado o se cerró. Solicita un enlace nuevo.';$('save').disabled=true;}}
$('form').addEventListener('submit',async event=>{event.preventDefault();if(!$('form').reportValidity())return;$('save').disabled=true;const body={clientKey:$('key').value,clientSecret:$('secret').value,sandbox:$('sandbox').checked};$('key').value=$('secret').value='';try{render(await request('/install',body));}catch{$('status').textContent='No se pudo guardar. Comprueba el estado recargando esta página.';}});
poll();`;

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
  const setup=await startTikTokSetup({onReady:()=>console.log(JSON.stringify({state:'sandbox_configuration_ready'}))});
  console.log(JSON.stringify({setupUrl:setup.url,expiresInMinutes:30}));
}
