import test from 'node:test';
import assert from 'node:assert/strict';
import {NOW,setup} from './helpers.js';
import {shareTikTokRequest,shareTikTokUrl} from '../src/share-tiktok.js';
import {tiktokAccess,tiktokAuthRequest,tiktokConnectUrl} from '../src/tiktok-auth.js';
import {tiktokMediaRequest} from '../src/tiktok-drafts.js';
import {createStorage} from '../src/storage.js';

const origin='https://fixture.workers.dev',id='b'.repeat(32);
const jpeg=new Uint8Array([255,216,1,2,255,217]);
async function fixture() {
  const s=setup({ADMIN_TOKEN:'fixture-admin-secret',WORKER_ORIGIN:origin,TIKTOK_CLIENT_KEY:'fixture-client',
    TIKTOK_CLIENT_SECRET:'fixture-client-secret',TIKTOK_UPLOAD_ENABLED:'true',TIKTOK_MEDIA_VERIFIED:'true'});
  const state={instant:NOW,init:[],status:[],token:[],refresh:[],profile:0,converted:0,convertFailed:false,
    uploadLost:false,uploadError:null,apiStatus:'SEND_TO_USER_INBOX',refreshLost:false,access:'fixture-access',refreshToken:'fixture-refresh'};
  const tokenData=()=>({access_token:state.access,refresh_token:state.refreshToken,open_id:'fixture-owner',scope:'user.info.basic,video.upload',expires_in:7200,refresh_expires_in:86400*180});
  const fetcher=async(url,options={})=>{
    if(url.endsWith('/v2/oauth/token/')) {
      const data=Object.fromEntries(new URLSearchParams(options.body));
      state.token.push(data);
      if(data.grant_type==='refresh_token') {
        state.refresh.push(data);
        if(state.refreshLost)throw Error('lost refresh response');
        state.access='fixture-new-access';state.refreshToken='fixture-new-refresh';
      }
      return Response.json(tokenData());
    }
    if(url.includes('/v2/user/info/')){state.profile++;return Response.json({error:{code:'ok'},data:{user:{open_id:'fixture-owner',display_name:'WaCapital <fixture>'}}});}
    if(url.endsWith('/v2/post/publish/content/init/')) {
      state.init.push({body:JSON.parse(options.body),headers:options.headers});
      if(state.uploadLost)throw Error('lost upload response');
      if(state.uploadError)return Response.json({error:{code:state.uploadError,message:'untrusted private detail'}},{status:403});
      return Response.json({error:{code:'ok'},data:{publish_id:'fixture-publish-id'}});
    }
    if(url.endsWith('/v2/post/publish/status/fetch/')) {
      state.status.push(JSON.parse(options.body));
      return Response.json({error:{code:'ok'},data:{status:state.apiStatus}});
    }
    throw Error('Unexpected network request');
  };
  const options={now:()=>state.instant,fetcher};
  const auth=request=>tiktokAuthRequest(request,s.env,options);
  async function begin() {
    const link=new URL(await tiktokConnectUrl(s.env,state.instant));
    const [expires,sig]=link.hash.slice(1).split('.');
    const start=await auth(new Request(origin+'/tiktok/start',{method:'POST',headers:{Origin:origin},body:JSON.stringify({expires:Number(expires),sig})}));
    const {url}=await start.json();
    return {url:new URL(url),cookie:start.headers.get('Set-Cookie').split(';')[0]};
  }
  const complete=(started,overrides={})=>auth(new Request(origin+'/tiktok/callback?'+new URLSearchParams({code:'fixture-code',state:started.url.searchParams.get('state'),...overrides}),{headers:{Cookie:started.cookie}}));
  const connect=async()=>assert.equal((await complete(await begin())).status,200);
  const editorial=JSON.stringify({headline:'Cinco cifras de los mercados: prueba'});
  s.db.native.prepare("INSERT INTO stories(id,event_key,title,source,source_url,published_at,priority,snippet,state,editorial,created_at) VALUES(?,?,?,'Fuente',?,?,100,'','approved',?,?)")
    .run(id,id,'Noticia de prueba','https://www.coindesk.com/news/fixture',NOW,editorial,NOW);
  s.db.native.prepare("INSERT INTO channel_jobs(story_id,platform,state,delivery,created_at) VALUES(?,'tiktok','approved','sent',?)").run(id,NOW);
  const insert=s.db.native.prepare("INSERT INTO channel_assets(story_id,platform,position,filename,file_id) VALUES(?,'tiktok',?,?,?)");
  for(let n=4;n>=0;n--)insert.run(id,n,'slide-'+n+'.png','file-'+n);
  const link=new URL(await shareTikTokUrl(s.env,id,NOW));
  const [storyId,expires,sig]=link.hash.slice(1).split('.');
  const payload={id:storyId,expires:Number(expires),sig};
  const serve=(route,extra={},headers={})=>shareTikTokRequest(new Request(origin+'/share/tiktok/'+route,{method:'POST',headers,body:JSON.stringify({...payload,...extra})}),s.env,{
    ...options,allowStorage:async()=>true,convert:async(_env,assets)=>{
      state.converted++;
      assert.deepEqual(assets.map(a=>a.file_id),[0,1,2,3,4].map(i=>'file-'+i));
      if(state.convertFailed)throw Error('browser unavailable');
      return Array.from({length:5},()=>jpeg);
    }
  });
  return {...s,state,auth,begin,complete,connect,serve,options};
}

test('TikTok OAuth is owner initiated, browser bound, single use and encrypted at rest',async()=>{
  const s=await fixture();
  assert.equal((await s.serve('data')).status,200);
  assert.equal((await (await s.serve('data')).json()).tiktok.state,'connection_required');
  const attempt=()=>s.auth(new Request(origin+'/tiktok/start',{method:'POST',headers:{Origin:'https://attacker.example'},body:'{}'}));
  assert.equal((await attempt()).status,401);
  const start=await s.begin();
  assert.equal(start.url.origin,'https://www.tiktok.com');
  assert.equal(start.url.searchParams.get('scope'),'user.info.basic,video.upload');
  assert.equal(start.url.searchParams.get('redirect_uri'),origin+'/tiktok/callback');
  const fake=await s.complete({...start,cookie:'__Host-wacapital-tiktok='+'0'.repeat(64)});
  assert.equal(fake.status,400);assert.equal(s.state.token.length,0);
  assert.equal((await s.complete(start)).status,200);
  assert.equal((await s.complete(start)).status,400);
  assert.equal(s.state.token.length,1);
  const raw=s.db.native.prepare("SELECT value FROM runtime WHERE key='tiktok_account_v1'").get().value;
  assert.doesNotMatch(raw,/fixture-access|fixture-refresh|fixture-owner/);
  const data=await (await s.serve('data')).json();
  assert.equal(data.tiktok.account,'WaCapital <fixture>');
  assert.equal(data.tiktok.state,'ready');
  assert.doesNotMatch(JSON.stringify(data),/fixture-access|fixture-refresh|fixture-client-secret/);
});

test('TikTok sends the approved caption and ordered JPEGs as MEDIA_UPLOAD exactly once',async()=>{
  const s=await fixture();await s.connect();
  const text=(await (await s.serve('data')).json()).text;
  assert.equal((await (await s.serve('draft',{sig:'f'.repeat(64)})).text()),'Enlace no válido o caducado');
  assert.equal(s.state.init.length,0);
  assert.deepEqual(await (await s.serve('draft')).json(),{state:'processing'});
  await s.serve('draft');await s.serve('draft');
  assert.equal(s.state.init.length,1);assert.equal(s.state.converted,1);
  const sent=s.state.init[0].body;
  assert.equal(sent.post_mode,'MEDIA_UPLOAD');assert.equal(sent.media_type,'PHOTO');
  assert.equal(sent.is_aigc,true);assert.deepEqual(sent.post_info,{description:text});
  assert.equal(sent.source_info.photo_cover_index,0);
  assert.equal(sent.source_info.photo_images.length,5);
  for(const [position,url] of sent.source_info.photo_images.entries()) {
    assert.ok(url.startsWith(origin+'/tiktok/media/'+id+'/'+position+'/'));
    assert.doesNotMatch(url,/fixture-admin-secret|fixture-access|file-/);
    const response=await tiktokMediaRequest(new Request(url),s.env,s.options);
    assert.equal(response.status,200);assert.equal(response.headers.get('Content-Type'),'image/jpeg');
    assert.deepEqual(new Uint8Array(await response.arrayBuffer()),jpeg);
    assert.equal((await tiktokMediaRequest(new Request(url,{method:'HEAD'}),s.env,s.options)).headers.get('Content-Length'),String(jpeg.length));
  }
  assert.deepEqual(await (await s.serve('draft-status')).json(),{state:'inbox',error:null});
  await s.serve('draft-status');assert.equal(s.state.status.length,1);
  s.state.instant+=6000;s.state.apiStatus='PUBLISH_COMPLETE';
  assert.equal((await (await s.serve('draft-status')).json()).state,'published');
  assert.equal(s.state.init.length,1);
  s.state.instant+=2*3_600_000;
  assert.equal((await tiktokMediaRequest(new Request(sent.source_info.photo_images[0]),s.env,s.options)).status,401);
});

test('lost upload responses, stale in-flight jobs and explicit failures never repeat initialization',async()=>{
  for(const mode of ['lost','rejected','preparation']) {
    const s=await fixture();await s.connect();
    s.state.uploadLost=mode==='lost';s.state.uploadError=mode==='rejected'?'url_ownership_unverified':null;s.state.convertFailed=mode==='preparation';
    const result=await (await s.serve('draft')).json();
    assert.equal(result.state,mode==='lost'?'uncertain':'failed');
    assert.doesNotMatch(JSON.stringify(result),/untrusted|fixture-access/);
    s.state.uploadLost=false;s.state.uploadError=null;s.state.convertFailed=false;
    await s.serve('draft');
    assert.equal(s.state.init.length,mode==='preparation'?0:1);
  }
  const s=await fixture();await s.connect();
  s.db.native.prepare("INSERT INTO tiktok_uploads(story_id,account_id,fingerprint,state,created_at,updated_at,media_expires) VALUES(?,'fixture-owner','fixture','submitting',?,?,?)").run(id,NOW-180_000,NOW-180_000,NOW+3_600_000);
  assert.equal((await (await s.serve('draft-status')).json()).state,'uncertain');
  await s.serve('draft');assert.equal(s.state.init.length,0);
});

test('revoked approval, foreign origins and changed files cannot upload or retrieve signed media',async()=>{
  const s=await fixture();await s.connect();
  assert.equal((await s.serve('draft',{}, {Origin:'https://attacker.example'})).status,401);
  s.db.native.exec("UPDATE channel_assets SET file_id='changed' WHERE position=4");
  assert.equal((await s.serve('draft')).status,401);assert.equal(s.state.init.length,0);
  s.db.native.exec("UPDATE channel_assets SET file_id='file-4' WHERE position=4");
  await s.serve('draft');
  const url=s.state.init[0].body.source_info.photo_images[0];
  assert.equal((await tiktokMediaRequest(new Request(url.replace(/\w\.jpg$/,'0.jpg')),s.env,s.options)).status,401);
  s.db.native.exec("UPDATE channel_jobs SET state='rejected'");
  assert.equal((await s.serve('draft-status')).status,401);
  assert.equal((await tiktokMediaRequest(new Request(url),s.env,s.options)).status,401);
});

test('rotated refresh tokens persist and an ambiguous refresh is not retried',async()=>{
  const s=await fixture();await s.connect();s.state.instant+=7200_000;
  assert.equal((await tiktokAccess(s.env,s.options)).accessToken,'fixture-new-access');
  assert.equal(s.state.refresh.length,1);
  assert.equal((await tiktokAccess(s.env,s.options)).refreshToken,'fixture-new-refresh');
  s.state.instant+=7200_000;s.state.refreshLost=true;
  await assert.rejects(()=>tiktokAccess(s.env,s.options));
  await assert.rejects(()=>tiktokAccess(s.env,s.options));
  assert.equal(s.state.refresh.length,2);
  assert.equal((await (await s.serve('data')).json()).tiktok.state,'connection_required');
});

test('unconfigured upload stays disabled and keeps the manual sharing route',async()=>{
  const s=await fixture();s.env.TIKTOK_MEDIA_VERIFIED='false';
  assert.equal((await (await s.serve('draft')).json()).state,'setup_required');
  assert.equal(s.state.token.length,0);assert.equal(s.state.converted,0);assert.equal(s.state.init.length,0);
  const page=await shareTikTokRequest(new Request(origin+'/share/tiktok'),s.env,s.options);
  assert.match(await page.text(),/Compartir desde el móvil/);
});

test('sandbox tokens cannot be reused with another TikTok app and the test connection is labeled',async()=>{
  const s=await fixture();s.env.TIKTOK_MODE='sandbox';await s.connect();
  const data=await (await s.serve('data')).json();assert.equal(data.tiktok.mode,'sandbox');
  const page=await s.auth(new Request(origin+'/tiktok/connect'));assert.match(await page.text(),/Conexión de prueba · Sandbox/);
  s.env.TIKTOK_CLIENT_KEY='another-client';
  assert.equal((await (await s.serve('data')).json()).tiktok.state,'connection_required');
  await s.serve('draft');assert.equal(s.state.init.length,0);assert.equal(s.state.converted,0);
});

test('the daily cap blocks a sixth attempt before conversion and temporary JPEGs expire without losing upload history',async()=>{
  const s=await fixture();await s.connect();
  for(const char of ['a','c','d','e','f']) {
    const other=char.repeat(32);
    s.db.native.prepare('INSERT INTO stories(id,event_key,title,source,source_url,published_at,priority,snippet,state,editorial,created_at) SELECT ?,?,title,source,source_url,published_at,priority,snippet,state,editorial,created_at FROM stories WHERE id=?').run(other,other,id);
    s.db.native.prepare("INSERT INTO tiktok_uploads(story_id,account_id,fingerprint,state,created_at,updated_at,media_expires) VALUES(?,'fixture-owner','fixture','processing',?,?,?)").run(other,NOW,NOW,NOW+3600_000);
  }
  assert.equal((await (await s.serve('draft')).json()).state,'daily_limit');
  assert.equal(s.state.converted,0);assert.equal(s.state.init.length,0);
  s.state.instant+=24*3600_000+1;
  assert.equal((await (await s.serve('draft')).json()).state,'processing');
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM tiktok_upload_assets').get().n,5);
  s.state.instant+=2*3600_000;
  await createStorage(s.env,{now:()=>s.state.instant,fetcher:s.options.fetcher}).cleanup();
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM tiktok_upload_assets').get().n,0);
  assert.equal(s.db.native.prepare('SELECT publish_id FROM tiktok_uploads WHERE story_id=?').get(id).publish_id,'fixture-publish-id');
});

test('URL-prefix verification serves only the configured text file',async()=>{
  const s=await fixture();
  s.env.TIKTOK_VERIFICATION_FILENAME='tiktok-fixture.txt';s.env.TIKTOK_VERIFICATION_CONTENT='fixture-verification-only';
  const request=name=>tiktokMediaRequest(new Request(origin+'/tiktok/media/'+name),s.env,s.options);
  assert.equal(await (await request('tiktok-fixture.txt')).text(),'fixture-verification-only');
  assert.equal((await request('another.txt')).status,404);
  s.env.TIKTOK_VERIFICATION_FILENAME='../private.txt';
  assert.equal((await request('../private.txt')).status,404);
});
