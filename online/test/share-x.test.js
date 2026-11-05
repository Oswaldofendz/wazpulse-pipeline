import test from 'node:test';
import assert from 'node:assert/strict';
import {shareXUrl,shareXRequest} from '../src/share-x.js';
import {createChannels} from '../src/channels.js';
import {createPipeline} from '../src/pipeline.js';
import {NOW,setup,decodeUpload} from './helpers.js';
import worker from '../src/worker.js';
const PNG=new Uint8Array([137,80,78,71,13,10,26,10,1,2,3]);
const ORIGIN='https://test.workers.dev';
async function fixture(kind='c') {
  const s=setup({WORKER_ORIGIN:ORIGIN,ADMIN_TOKEN:'test-internal-secret',MEDIA_ENABLED:'true'});
  const p=createPipeline(s.env,{fetcher:s.fetcher,now:()=>NOW});await p.tick();
  const story=s.db.native.prepare('SELECT * FROM stories').get();
  s.db.native.prepare("UPDATE stories SET state='approved',approved_at=? WHERE id=?").run(NOW,story.id);
  const cover={file_id:'approved-cover',filename:'WaCapital-test-X.png'};
  if(kind==='c') {
    s.db.native.prepare("INSERT INTO channel_jobs(story_id,platform,state,delivery,draft_message,created_at) VALUES(?,'x','approved','sent','101',?)").run(story.id,NOW);
    s.db.native.prepare("INSERT INTO channel_assets(story_id,platform,position,filename,file_id) VALUES(?,'x',0,?,?)").run(story.id,cover.filename,cover.file_id);
  }else {
    s.db.native.prepare("INSERT INTO visual_jobs(story_id,state,created_at) VALUES(?,'approved',?)").run(story.id,NOW);
    s.db.native.prepare("INSERT INTO visual_assets(story_id,position,filename,delivery,file_id) VALUES(?,0,?,'sent',?)").run(story.id,cover.filename,cover.file_id);
  }
  const url=await shareXUrl(s.env,story,cover,kind,NOW),parts=new URL(url).hash.slice(1).split('.');
  const credentials={id:parts[0],expires:Number(parts[1]),kind:parts[2],sig:parts[3]};
  const request=(path='/share/x/data',body=credentials,extra={})=>new Request(ORIGIN+path,{method:'POST',headers:{Origin:ORIGIN,...extra},body:JSON.stringify(body)});
  const state={calls:[]};
  const fetcher=async(url,options)=>{
    state.calls.push({url,options});
    if(url.endsWith('/getFile'))return Response.json({ok:true,result:{file_path:'documents/approved.png'}});
    if(url.includes('/file/bot'))return new Response(PNG);
    throw Error('No other service is allowed');
  };
  const serve=req=>shareXRequest(req,s.env,{fetcher,now:()=>NOW});
  return {...s,story,cover,url,credentials,request,serve,shareState:state};
}
test('share capability is short-lived, holds no admin secret and does not enter request paths',async()=>{
  const s=await fixture(),u=new URL(s.url);
  assert.equal(u.pathname,'/share/x');assert.equal(u.search,'');
  assert.ok(u.hash);assert.ok(!s.url.includes(s.env.ADMIN_TOKEN));
  assert.equal(s.credentials.expires,NOW+7*86_400_000);
  const response=await s.serve(s.request());
  assert.equal(response.status,200);
  const body=await response.json();assert.equal(body.text,'Un anuncio que merece contexto.');
  assert.equal(body.filename,s.cover.filename);assert.equal(body.sourceUrl,undefined);
  assert.equal(s.shareState.calls.length,0);
});
test('only the exact approved cover is retrieved, privately, with no upload, AI or X API call',async()=>{
  for(const kind of ['c','l']) {
    const s=await fixture(kind),response=await s.serve(s.request('/share/x/image'));
    assert.equal(response.status,200);assert.equal(response.headers.get('Content-Type'),'image/png');
    assert.equal(response.headers.get('Cache-Control'),'private, no-store');
    assert.deepEqual(new Uint8Array(await response.arrayBuffer()),PNG);
    assert.equal(JSON.parse(s.shareState.calls[0].options.body).file_id,'approved-cover');
    assert.equal(s.shareState.calls.length,2);
  }
});
test('expired, tampered, oversized, forged and cross-origin capabilities cannot read any material',async()=>{
  const s=await fixture();
  for(const body of [{...s.credentials,sig:'0'.repeat(64)},{...s.credentials,id:'a'.repeat(32)},
    {...s.credentials,kind:'l'},{...s.credentials,expires:NOW-1},{...s.credentials,expires:NOW+8*86_400_000},{},{...s.credentials,padding:'x'.repeat(1100)}]) {
    assert.equal((await s.serve(s.request('/share/x/data',body))).status,401);
  }
  assert.equal((await s.serve(s.request('/share/x/data',s.credentials,{Origin:'https://other.example'}))).status,401);
  assert.equal(s.shareState.calls.length,0);
});
test('approval revocation, a different cover and changed copy invalidate an existing share capability',async()=>{
  for(const mutation of [
    s=>s.db.native.prepare("UPDATE channel_jobs SET state='review'").run(),
    s=>s.db.native.prepare("UPDATE channel_jobs SET delivery='uncertain'").run(),
    s=>s.db.native.prepare("UPDATE stories SET state='rejected'").run(),
    s=>s.db.native.prepare("UPDATE channel_assets SET file_id='another-cover'").run(),
    s=>{const e=JSON.parse(s.story.editorial);e.tweet='Texto cambiado después de compartir.';s.db.native.prepare('UPDATE stories SET editorial=?').run(JSON.stringify(e));}
  ]) {
    const s=await fixture();mutation(s);assert.equal((await s.serve(s.request())).status,401);
  }
});
test('generic page and script disclose no private material and use restrictive browser headers',async()=>{
  const s=await fixture();
  const response=await worker.fetch(new Request(ORIGIN+'/share/x'),s.env,{}),html=await response.text();
  assert.equal(response.status,200);assert.ok(!html.includes(s.story.id));assert.ok(!html.includes(s.credentials.sig));
  assert.match(response.headers.get('Content-Security-Policy'),/frame-ancestors 'none'/);
  assert.equal(response.headers.get('Referrer-Policy'),'no-referrer');
  assert.equal(response.headers.get('Permissions-Policy'),'web-share=(self)');
  assert.match(html,/Safari/);assert.match(html,/Chrome/);
  assert.equal((await s.serve(new Request(ORIGIN+'/share/x/image'))).status,404);
});
test('Telegram approval recovery updates the same message with the combined-share option, without sending media',async()=>{
  const s=await fixture(),edits=[];
  const channels=createChannels(s.env,{now:()=>NOW,fetcher:async(url,options)=>{
    options=await decodeUpload(options);assert.ok(url.endsWith('/editMessageText'));
    edits.push(JSON.parse(options.body));return Response.json({ok:true,result:{message_id:101}});
  }});
  assert.equal(await channels.refreshX(s.story.id),true);
  const buttons=edits[0].reply_markup.inline_keyboard.flat();
  assert.equal(buttons[0].text,'Compartir texto e imagen');
  assert.ok(buttons[0].url.startsWith(ORIGIN+'/share/x#'));
  assert.equal(buttons[1].text,'Abrir X solo con texto');
});
test('oversized or non-PNG Telegram responses do not become shareable images',async()=>{
  const s=await fixture();
  for(const data of [new Uint8Array(1_900_001),new TextEncoder().encode('<html>no image</html>')]) {
    const response=await shareXRequest(s.request('/share/x/image'),s.env,{now:()=>NOW,fetcher:async url=>url.endsWith('/getFile')?
      Response.json({ok:true,result:{file_path:'documents/approved.png'}}):new Response(data)});
    assert.equal(response.status,503);
  }
});
