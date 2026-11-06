import test from 'node:test';
import assert from 'node:assert/strict';
import {shareTikTokRequest, shareTikTokUrl} from '../src/share-tiktok.js';
import {tiktokCaption} from '../src/social.js';
import {NOW, setup} from './helpers.js';

const origin = 'https://fixture.workers.dev';
const id = 'a'.repeat(32);
const png = new Uint8Array([137,80,78,71,13,10,26,10,1]);

async function fixture() {
  const {db, env} = setup({ADMIN_TOKEN:'test-token', WORKER_ORIGIN:origin});
  const editorial = JSON.stringify({headline:'Bitcoin recupera los 76.000 dólares'});
  db.native.prepare("INSERT INTO stories(id,event_key,title,source,source_url,published_at,priority,snippet,state,editorial,created_at) VALUES(?,?,?,'Fuente',?,?,100,'','approved',?,?)")
    .run(id,id,'Un titular','https://www.coindesk.com/news/example',NOW,editorial,NOW);
  db.native.prepare("INSERT INTO channel_jobs(story_id,platform,state,delivery,created_at) VALUES(?,'tiktok','approved','sent',?)").run(id,NOW);
  const insert = db.native.prepare("INSERT INTO channel_assets(story_id,platform,position,filename,file_id) VALUES(?,'tiktok',?,?,?)");
  for (let position=4;position>=0;position--) insert.run(id,position,'slide-'+(position+1)+'.png','file-'+position);
  const url = await shareTikTokUrl(env,id,NOW);
  const [storyId, expires, sig] = new URL(url).hash.slice(1).split('.');
  const payload = {id:storyId,expires:Number(expires),sig};
  const files = [];
  const serve = (route, extra={}, headers={}) => shareTikTokRequest(new Request(origin+'/share/tiktok/'+route,{
    method:'POST',headers,body:JSON.stringify({...payload,...extra})
  }),env,{now:()=>NOW,fetcher:async (url,options)=>{
    if (url.endsWith('/getFile')) {
      files.push(JSON.parse(options.body).file_id);
      return Response.json({ok:true,result:{file_path:'documents/fixture.png'}});
    }
    return new Response(png);
  }});
  return {db,env,url,serve,files};
}

test('TikTok shares the approved caption and the exact five ordered PNGs',async()=>{
  const s=await fixture(),link=new URL(s.url);
  assert.equal(link.search,'');assert.equal(link.pathname,'/share/tiktok');
  assert.ok(!s.url.includes(s.env.ADMIN_TOKEN));
  const metadata=await s.serve('data');
  assert.equal(metadata.status,200);
  assert.equal(metadata.headers.get('Cache-Control'),'private, no-store');
  const caption=tiktokCaption(s.db.native.prepare('SELECT * FROM stories').get());
  assert.deepEqual(await metadata.json(),{text:caption,filenames:[1,2,3,4,5].map(i=>'slide-'+i+'.png')});
  for(let position=0;position<5;position++){
    const image=await s.serve('image',{position});
    assert.equal(image.status,200);assert.equal(image.headers.get('Content-Type'),'image/png');
    assert.deepEqual(new Uint8Array(await image.arrayBuffer()),png);
  }
  assert.deepEqual(s.files,[0,1,2,3,4].map(i=>'file-'+i));
  assert.equal((await s.serve('image',{position:5})).status,400);
});

test('TikTok file access rejects forged links, stale approval and changed text or media',async()=>{
  const s=await fixture();
  assert.equal((await s.serve('data',{sig:'f'.repeat(64)})).status,401);
  assert.equal((await s.serve('image',{position:0,expires:NOW})).status,401);
  assert.equal((await s.serve('data',{}, {Origin:'https://other.example'})).status,401);
  s.db.native.exec("UPDATE channel_assets SET file_id='new-file' WHERE position=4");
  assert.equal((await s.serve('image',{position:0})).status,401);
  s.db.native.exec("UPDATE channel_assets SET file_id='file-4' WHERE position=4");
  s.db.native.exec("UPDATE stories SET editorial=json_set(editorial,'$.headline','Otro titular')");
  assert.equal((await s.serve('data')).status,401);
  s.db.native.exec("UPDATE channel_jobs SET state='review'");
  assert.equal(await shareTikTokUrl(s.env,id,NOW),null);
  assert.equal(s.files.length,0);
});
