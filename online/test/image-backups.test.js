import test from 'node:test';
import assert from 'node:assert/strict';
import {Buffer} from 'node:buffer';
import {setup,NOW} from './helpers.js';
import {createImages} from '../src/image-providers.js';
import {imageProvidersStatus} from '../src/image-backups.js';
import {headlineMovement} from '../src/news-subject.js';
import {visualBrief,visualHtml} from '../src/publication.js';
import {socialHtml,socialKitV2} from '../src/social.js';
import {createChannels} from '../src/channels.js';
import {CHART_PIXEL} from '../src/market-graph.js';

const JPEG='/9j/'+'A'.repeat(160);
function fixture(overrides={},title='A company announces an acquisition'){
  const s=setup({MEDIA_ENABLED:'true',DAILY_VISUAL_LIMIT:'0',HORDE_ENABLED:'false',...overrides});
  const story={id:'a'.repeat(32),title,snippet:'The source describes the reported event.',source:'Fuente',source_url:'https://example.test/story',published_at:NOW,editorial:{headline:title,hook:'Detalle de la fuente.',angle:'Contexto de la fuente.',reasoning:'Detalle documentado.',tweet:title,instagram:title}};
  s.db.native.prepare("INSERT INTO stories(id,event_key,title,source,source_url,published_at,priority,snippet,state,editorial,created_at) VALUES(?,?,?,?,?,?,100,?,'approved',?,?)").run(story.id,story.id,title,story.source,story.source_url,NOW,story.snippet,JSON.stringify(story.editorial),NOW);
  return {...s,story,brief:visualBrief(story)};
}

test('reported selloff, possessive falling price and Spanish drop agree while ambiguous claims do not invent price direction',()=>{
  for(const title of ["Bitcoin selloff: Crypto is failing the digital gold test again as it sinks with stocks","Bitcoin's falling price worries traders",'La caída del precio de Bitcoin continúa'])assert.equal(headlineMovement(title)?.direction,'down',title);
  assert.equal(headlineMovement('Bitcoin rises after a difficult session')?.direction,'up');
  for(const title of ['Bitcoin may fall next month','Nvidia drops acquisition plans','Bitcoin rally fizzles','Bitcoin selloff fears grow','Bitcoin falls short of its promise'])assert.equal(headlineMovement(title),null,title);
  assert.equal(headlineMovement('Fed raises rates as Bitcoin falls'),null);
});

test('a price-news illustration uses the actual asset and direction with no AI or invented numeric chart',async()=>{
  const s=fixture({},'Bitcoin selloff: Crypto is failing the digital gold test again as it sinks with stocks');let calls=0;
  const result=await createImages(s.env,{now:()=>NOW,generate:async()=>{calls++;throw Error('must not call');},fetcher:async()=>{calls++;throw Error('must not call');}}).next(s.story.id,s.brief);
  assert.equal(result.provider,'editorial-local');assert.equal(calls,0);assert.equal(result.brief.art.priceMove.direction,'down');
  const html=visualHtml(result.brief,result.image);assert.match(html,/Ilustración editorial de Bitcoin/);assert.match(html,/₿/);assert.match(html,/ILUSTRACIÓN EDITORIAL/);assert.doesNotMatch(html,/ILUSTRACIÓN CON IA/);
  const kit=socialKitV2({...s.story,editorial:{...s.story.editorial,socialSlides:[{title:'Bitcoin pierde terreno',body:'La fuente describe una caída con las acciones.',visual:'news'},{title:'Qué factores se mencionan',body:'Se citan las tasas y los flujos ETF.',visual:'funds'},{title:'El papel de refugio',body:'La noticia cuestiona la idea de oro digital.',visual:'news'}]}},{platform:'tiktok'});
  assert.match(socialHtml({...kit,editorialArt:result.brief.art},result.image,0,'tiktok'),/Ilustración editorial de Bitcoin/);
  assert.doesNotMatch(socialHtml({...kit,editorialArt:result.brief.art},result.image,0,'tiktok'),/Ilustración con IA/);
});

test('five AI provider integrations are reported honestly and missing keys or disabled remote spending never cause requests',async()=>{
  const s=fixture({IMAGE_REMOTE_BACKUPS_ENABLED:'true',POLLINATIONS_API_KEY:'fixture-only',TOGETHER_API_KEY:'fixture-only',STABILITY_API_KEY:'fixture-only'});let calls=0;
  const list=imageProvidersStatus(s.env);assert.equal(list.filter(p=>p.id!=='editorial-local').length,5);assert.ok(list.filter(p=>p.requiresCredentials).every(p=>p.configured&&!p.enabled));
  const result=await createImages(s.env,{now:()=>NOW,fetcher:async()=>{calls++;throw Error('no paid calls');}}).next(s.story.id,s.brief);
  assert.equal(result.provider,'editorial-local');assert.equal(calls,0);assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM image_provider_budget').get().n,0);
});

test('an exhausted provider advances to the next authenticated service without leaking keys or repeating a failed call',async()=>{
  const s=fixture({IMAGE_COST_POLICY:'allow-paid',IMAGE_REMOTE_BACKUPS_ENABLED:'true',POLLINATIONS_API_KEY:'private-fixture-one',TOGETHER_API_KEY:'private-fixture-two'}),calls=[];
  const images=createImages(s.env,{now:()=>NOW,fetcher:async(url,options)=>{calls.push({url,options});if(url.includes('pollinations'))return Response.json({error:'quota'},{status:429});return Response.json({data:[{b64_json:JPEG}]});}});
  assert.equal((await images.next(s.story.id,s.brief)).provider,'together');assert.equal((await images.next(s.story.id,s.brief)).provider,'together');assert.equal(calls.length,2);
  assert.ok(calls[0].options.headers.Authorization.endsWith('private-fixture-one'));assert.ok(calls[1].options.headers.Authorization.endsWith('private-fixture-two'));
  const logs=s.db.native.prepare("SELECT value FROM runtime WHERE key LIKE 'image_%'").all().map(r=>r.value).join('');assert.doesNotMatch(logs,/private-fixture/);
  assert.equal(s.db.native.prepare('SELECT SUM(used) AS n FROM image_provider_budget').get().n,2);
});

test('authentication errors and lost responses exhaust each provider once, then local artwork completes the news',async()=>{
  const s=fixture({IMAGE_COST_POLICY:'allow-paid',IMAGE_REMOTE_BACKUPS_ENABLED:'true',POLLINATIONS_API_KEY:'fixture',TOGETHER_API_KEY:'fixture',STABILITY_API_KEY:'fixture'});let calls=0;
  const images=createImages(s.env,{now:()=>NOW,fetcher:async url=>{calls++;if(url.includes('pollinations'))throw Error('lost');if(url.includes('together'))return new Response('unauthorized',{status:401});return new Response('unavailable',{status:503});}});
  const result=await images.next(s.story.id,s.brief);assert.equal(result.provider,'editorial-local');assert.equal(calls,3);await images.next(s.story.id,s.brief);assert.equal(calls,3);
  assert.equal(s.db.native.prepare('SELECT SUM(used) AS n FROM image_provider_budget').get().n,3);
});

test('Stability consumes bounded image bytes and malformed outputs continue to local artwork',async()=>{
  for(const valid of [true,false]){
    const s=fixture({IMAGE_COST_POLICY:'allow-paid',IMAGE_REMOTE_BACKUPS_ENABLED:'true',STABILITY_API_KEY:'fixture'});
    const images=createImages(s.env,{now:()=>NOW,fetcher:async(url,options)=>{assert.match(url,/api\.stability\.ai/);assert.equal(options.body.get('output_format'),'jpeg');return new Response(valid?Buffer.from(JPEG,'base64'):'not an image');}});
    assert.equal((await images.next(s.story.id,s.brief)).provider,valid?'stability':'editorial-local');
  }
});

test('the Horde wait is bounded to two minutes and a confirmed real-data chart never becomes a synthetic drawing',async()=>{
  const s=fixture({HORDE_ENABLED:'true'});let clock=NOW,submitted=0;
  const images=createImages(s.env,{now:()=>clock,fetcher:async(url,options)=>{if(url.endsWith('/generate/async')){submitted++;return Response.json({id:'12345678-1234-1234-1234-123456789abc'});}if(options.method==='DELETE')return Response.json({ok:true});return Response.json({done:false,is_possible:true});}});
  assert.equal((await images.next(s.story.id,s.brief)).state,'pending');clock+=121000;assert.equal((await images.next(s.story.id,s.brief)).provider,'editorial-local');assert.equal(submitted,1);
  const second=fixture({},'Bitcoin falls as stocks drop'),brief={...second.brief,marketSnapshot:{version:1},art:{...second.brief.art,mediaKind:'market-chart'}};
  const result=await createImages(second.env,{now:()=>NOW}).next(second.story.id,brief);assert.equal(result.provider,'data-chart');assert.equal(result.image,CHART_PIXEL);
});

test('safe image recovery wakes only requested undelivered formats and cannot alter an existing approved album',async()=>{
  const s=fixture({},'Bitcoin selloff continues'),db=s.db.native;
  db.prepare("INSERT INTO channel_jobs(story_id,platform,state,delivery,error,created_at,lease_until) VALUES(?,'x','queued','sent','quota-notice',?,?)").run(s.story.id,NOW,NOW+86400000);
  db.prepare("INSERT INTO channel_jobs(story_id,platform,state,delivery,created_at) VALUES(?,'tiktok','draft','sent',?)").run(s.story.id,NOW);
  db.prepare("INSERT INTO channel_images(story_id,state,manifest,created_at,updated_at) VALUES(?,'fallback',?,?,?)").run(s.story.id,JSON.stringify(s.brief),NOW,NOW);
  const channels=createChannels(s.env,{now:()=>NOW,fetcher:async()=>{throw Error('must not send');}});
  assert.equal((await channels.resumeImage(s.story.id)).resumed,1);assert.equal(db.prepare("SELECT state FROM channel_jobs WHERE platform='tiktok'").get().state,'draft');
  assert.equal(db.prepare("SELECT lease_until FROM channel_jobs WHERE platform='x'").get().lease_until,0);
  db.prepare("INSERT INTO channel_assets(story_id,platform,position,filename,file_id,message_id) VALUES(?,'x',0,'approved.png','file','123')").run(s.story.id);
  assert.equal((await channels.resumeImage(s.story.id)).resumed,0);assert.equal(db.prepare('SELECT file_id FROM channel_assets').get().file_id,'file');
});

test('invalid base64 image data does not reach the browser and a failed primary is not repeated',async()=>{
  const s=fixture({DAILY_VISUAL_LIMIT:'3'});let calls=0;
  const images=createImages(s.env,{now:()=>NOW,generate:async()=>{calls++;return Buffer.from('not an image'.repeat(15)).toString('base64');}});
  assert.equal((await images.next(s.story.id,s.brief)).state,'pending');assert.equal((await images.next(s.story.id,s.brief)).provider,'editorial-local');assert.equal(calls,1);
});

test('stock and positive price illustrations retain their own subject and palette',async()=>{
  const s=fixture({},'Nvidia shares rebound after a difficult session');
  const result=await createImages(s.env,{now:()=>NOW}).next(s.story.id,s.brief);
  assert.equal(result.brief.art.theme,'jade');assert.equal(result.brief.art.priceMove.direction,'up');
  const html=visualHtml(result.brief,result.image);assert.match(html,/NVIDIA/);assert.doesNotMatch(html,/₿/);
  const kit=socialKitV2({...s.story,editorial:{...s.story.editorial,socialSlides:[{title:'Nvidia rebota',body:'La fuente describe el repunte.',visual:'company'},{title:'La sesión anterior',body:'La noticia menciona una sesión difícil.',visual:'company'},{title:'El movimiento',body:'El titular sitúa el rebote después de esa sesión.',visual:'company'}]}},{platform:'tiktok'});
  assert.match(socialHtml({...kit,editorialArt:result.brief.art},result.image,0,'tiktok'),/#50edbc/);
});
