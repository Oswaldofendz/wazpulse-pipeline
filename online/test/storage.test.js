import test from 'node:test';
import assert from 'node:assert/strict';
import {createStorage, STORAGE_LIMIT_BYTES} from '../src/storage.js';
import {createMedia} from '../src/media.js';
import worker from '../src/worker.js';
import {setup, NOW} from './helpers.js';
const DAY = 86_400_000;
const id = 'b'.repeat(32);
function job(s, options = {}) {
  const {state = 'approved', count = 6, badDelivery = false, missingId = false, png = null} = options;
  s.db.native.prepare("INSERT INTO stories(id,event_key,title,source,source_url,published_at,priority,snippet,state,approved_at,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)")
    .run(id,id,'Fixture only — no actual news','Fixture','https://www.bbc.com/fixture',NOW,90,'Fixture','approved',NOW,NOW-90*DAY);
  s.db.native.prepare('INSERT INTO visual_jobs(story_id,state,image,manifest,created_at,approved_at) VALUES(?,?,?,?,?,?)')
    .run(id,state,'image-to-release',JSON.stringify({title:'Preserved approved title'}),NOW-90*DAY,NOW);
  for(let n=0;n<count;n++) s.db.native.prepare('INSERT INTO visual_assets(story_id,position,filename,png,delivery,file_id) VALUES(?,?,?,?,?,?)')
    .run(id,n,'fixture-'+n+'.png',n===0?png:null,n===0&&badDelivery?'uncertain':'sent',n===0&&missingId?null:'file-'+n);
}
function value(s,key,data) {s.db.native.prepare('INSERT INTO runtime(key,value) VALUES(?,?)').run(key,JSON.stringify(data));}

test('expired preview cache is removed at seven days while recent and unrelated records survive', async () => {
  const s=setup();
  value(s,'visual_preview:old',{image:'old'});
  value(s,'retention_preview:visual_preview:old',NOW-7*DAY);
  value(s,'visual_preview:fresh',{image:'fresh'});
  value(s,'retention_preview:visual_preview:fresh',NOW-7*DAY+1);
  value(s,'last_tick',{image:'not a preview'});
  value(s,'visualXpreview:unrelated',{image:'not our cache'});
  value(s,'retention_preview:visualXpreview:unrelated',NOW-10*DAY);
  const storage=createStorage(s.env,{now:()=>NOW,fetcher:s.fetcher});
  assert.equal((await storage.plan()).expiredPreviews,1);
  assert.equal((await storage.cleanup()).deletedPreviews,1);
  assert.equal(s.db.native.prepare("SELECT key FROM runtime WHERE key='visual_preview:old'").get(),undefined);
  assert.ok(s.db.native.prepare("SELECT key FROM runtime WHERE key='visual_preview:fresh'").get());
  assert.ok(s.db.native.prepare("SELECT key FROM runtime WHERE key='last_tick'").get());
  assert.ok(s.db.native.prepare("SELECT key FROM runtime WHERE key='visualXpreview:unrelated'").get());
});

test('all six confirmed originals permit background cleanup after thirty days without losing audit data', async () => {
  const s=setup(); job(s);
  value(s,'retention_image:'+id,NOW-30*DAY);
  const before=s.db.native.prepare('SELECT * FROM stories').get();
  const storage=createStorage(s.env,{now:()=>NOW,fetcher:s.fetcher});
  assert.equal((await storage.cleanup()).releasedBackgrounds,1);
  const row=s.db.native.prepare('SELECT * FROM visual_jobs').get();
  assert.equal(row.image,null);
  assert.equal(row.state,'approved');
  assert.equal(row.approved_at,NOW);
  assert.equal(row.manifest,JSON.stringify({title:'Preserved approved title'}));
  assert.deepEqual(s.db.native.prepare('SELECT * FROM stories').get(),before);
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM visual_assets WHERE file_id IS NOT NULL').get().n,6);
});

test('incomplete, uncertain or still-active deliveries retain their original and upload buffer', async () => {
  for(const change of [{count:5},{badDelivery:true},{missingId:true},{png:new Uint8Array([1,2,3])},{state:'blocked'},{state:'rendering'}]) {
    const s=setup(); job(s,change);
    value(s,'retention_image:'+id,NOW-100*DAY);
    const result=await createStorage(s.env,{now:()=>NOW,fetcher:s.fetcher}).cleanup();
    assert.equal(result.releasedBackgrounds,0);
    assert.equal(s.db.native.prepare('SELECT image FROM visual_jobs').get().image,'image-to-release');
  }
});

test('legacy images and previews receive a new clock and are not purged based on an old article date', async () => {
  const s=setup(); job(s);
  value(s,'visual_preview:legacy',{image:'legacy',brief:{date:'2020-01-01'}});
  const storage=createStorage(s.env,{now:()=>NOW,fetcher:s.fetcher});
  const result=await storage.cleanup();
  assert.equal(result.deletedPreviews,0); assert.equal(result.releasedBackgrounds,0);
  for(const key of ['retention_image:'+id,'retention_preview:visual_preview:legacy']) assert.equal(JSON.parse(s.db.native.prepare('SELECT value FROM runtime WHERE key=?').get(key).value),NOW);
});

test('cleanup batches stay bounded and repeated runs are idempotent', async () => {
  const s=setup();
  for(let n=0;n<12;n++){value(s,'visual_preview:'+n,{image:'old'});value(s,'retention_preview:visual_preview:'+n,NOW-8*DAY);}
  const storage=createStorage(s.env,{now:()=>NOW,fetcher:s.fetcher});
  assert.equal((await storage.cleanup()).deletedPreviews,10);
  assert.equal((await storage.cleanup()).deletedPreviews,2);
  assert.equal((await storage.cleanup()).deletedPreviews,0);
});

test('corrupt or future retention clocks never authorize deletion', async () => {
  const s=setup();
  for(const [key,clock] of [['invalid','not-json'],['future',JSON.stringify(NOW+DAY)],['string',JSON.stringify('123')]]){
    value(s,'visual_preview:'+key,{image:'keep'});
    s.db.native.prepare('INSERT INTO runtime(key,value) VALUES(?,?)').run('retention_preview:visual_preview:'+key,clock);
  }
  assert.equal((await createStorage(s.env,{now:()=>NOW,fetcher:s.fetcher}).cleanup()).deletedPreviews,0);
});

test('capacity guard pauses before one hundred MB, suppresses repeated alerts and never calls image AI', async () => {
  const s=setup({MEDIA_ENABLED:'true'}); job(s,{state:'queued',count:0});
  const original=s.env.DB.prepare;
  s.env.DB.prepare=sql=>sql==='SELECT 1 AS storage_probe'?{bind(){return this;},all:async()=>({results:[{storage_probe:1}],meta:{size_after:STORAGE_LIMIT_BYTES-1_000_000}})}:original(sql);
  const storage=createStorage(s.env,{now:()=>NOW,fetcher:s.fetcher});
  assert.equal(await storage.allow(),false);
  assert.equal(await storage.allow(),false);
  assert.equal(s.state.messages.length,1);
  let aiCalls=0;
  const media=createMedia(s.env,{now:()=>NOW,fetcher:s.fetcher,generate:async()=>{aiCalls++;},render:async()=>{throw Error('must not render');}});
  assert.equal((await media.step()).state,'storage-paused');
  assert.equal(aiCalls,0);
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM visual_budget').get().n,0);
});

test('unknown database size fails closed, and maintenance respects the media lock and pause switch', async () => {
  const s=setup();
  const original=s.env.DB.prepare;
  s.env.DB.prepare=sql=>sql==='SELECT 1 AS storage_probe'?{bind(){return this;},all:async()=>({results:[],meta:{}})}:original(sql);
  const storage=createStorage(s.env,{now:()=>NOW,fetcher:s.fetcher});
  assert.equal(await storage.allow(),false);
  s.db.native.prepare('INSERT INTO job_lock(name,owner,until_ms) VALUES(?,?,?)').run('visual','another-worker',NOW+60_000);
  assert.equal((await storage.cleanup()).state,'busy');
  s.env.PIPELINE_ENABLED='false';
  assert.equal((await storage.cleanup()).state,'disabled');
});

test('delivery and preview clocks remain stable during retries', async () => {
  const s=setup(); job(s);
  value(s,'visual_preview:fixed',{image:'keep'});
  let instant=NOW;
  const storage=createStorage(s.env,{now:()=>instant,fetcher:s.fetcher});
  await storage.delivered(id); await storage.rememberPreview('visual_preview:fixed');
  instant+=DAY;
  await storage.delivered(id); await storage.rememberPreview('visual_preview:fixed');
  for(const key of ['retention_image:'+id,'retention_preview:visual_preview:fixed']) assert.equal(JSON.parse(s.db.native.prepare('SELECT value FROM runtime WHERE key=?').get(key).value),NOW);
});

test('storage status and cleanup remain private', async () => {
  const s=setup();
  for(const path of ['/admin/storage-status','/admin/storage-plan','/admin/storage-cleanup']) assert.equal((await worker.fetch(new Request('https://test'+path),s.env,{})).status,401);
});
