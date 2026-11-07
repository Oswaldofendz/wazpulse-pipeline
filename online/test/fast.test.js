import test from 'node:test';
import assert from 'node:assert/strict';
import {createFast} from '../src/fast.js';
import {fastEditorial} from '../src/fast-editorial.js';
import {MARKET_ASSETS,marketOpen,quoteFromYahoo,appendQuote,marketTrigger,marketEditorial} from '../src/fast-market.js';
import {FAST_FEEDS,parseFeed,fastTopic} from '../src/fast-feeds.js';
import {marketGraph,CHART_PIXEL} from '../src/market-graph.js';
import {createImages} from '../src/image-providers.js';
import {createChannels} from '../src/channels.js';
import {createPipeline} from '../src/pipeline.js';
import {visualBrief,xPublication} from '../src/publication.js';
import {setup,decodeUpload,article} from './helpers.js';
import worker from '../src/worker.js';
const NOW=Date.parse('2026-11-05T16:10:00Z');
const btc=MARKET_ASSETS.find(a=>a.symbol==='BTC-USD');
const raw=(asset,price=100,at=NOW,prev=100,extra={})=>({chart:{result:[{meta:{symbol:asset.symbol,regularMarketPrice:price,regularMarketTime:at/1000,chartPreviousClose:prev,currency:'USD',exchangeDataDelayedBy:0,...extra}}]}});
const articleUrl=(feed,id)=>feed.id.startsWith('bbc')?'https://www.bbc.com/news/'+id:'https://'+new URL(feed.url).hostname+'/news/'+id;
const xml=(feed,id,title,at)=>'<rss><channel><item><title><![CDATA['+title+']]></title><link>'+articleUrl(feed,id)+'</link><pubDate>'+new Date(at).toUTCString()+'</pubDate><description>Source headline fixture.</description></item></channel></rss>';
function fixture(overrides={}) {
  const s=setup({FAST_ENABLED:'true',FAST_ALERTS_ENABLED:'true',DAILY_FAST_LIMIT:'6',FAST_REQUEST_LIMIT:'12000',MEDIA_ENABLED:'true',...overrides});
  const state={clock:NOW,prices:{},previous:{},calls:[],messages:[],edits:[],headlines:{},lose:false,confirmationMismatch:false,stale:false,badQuote:false,http429:false,news:false,rendered:0,uploads:0};
  const translations = {
    'Federal Reserve issues FOMC statement': 'La Reserva Federal publica el comunicado del FOMC',
    'Emergency rate cut after bank failure': 'Recorte de emergencia de tipos tras la quiebra de un banco',
    'Coinbase exchange confirms major hack': 'Coinbase confirma un grave ataque a la plataforma',
    'Iran launches attacks in Strait of Hormuz': 'Irán lanza ataques en el estrecho de Ormuz'
  };
  state.translationCalls = [];
  s.env.AI ??= {async run(_model, input) {
    const title = input.messages[1].content;
    state.translationCalls.push(title);
    return {response: state.translationOverride ?? translations[title]};
  }};
  const fetcher=async(url,options={})=>{
    state.calls.push({url,options});
    if(url.includes('query1.finance.yahoo.com')) {
      if(state.http429)return new Response('limited',{status:429,headers:{'Retry-After':'600'}});
      const symbol=decodeURIComponent(new URL(url).pathname.split('/').at(-1)),asset=MARKET_ASSETS.find(a=>a.symbol===symbol);
      if(state.badQuote)return Response.json({chart:{error:{code:'unavailable'}}});
      return Response.json(raw(asset,state.prices[symbol]||100,state.clock-(state.stale?10*60_000:0),state.previous[symbol]||100));
    }
    const f=FAST_FEEDS.find(f=>f.url===url);
    if (f && state.unchangedFeed && new Headers(options.headers).has('If-None-Match')) return new Response(null, {status: 304});
    if(f) return new Response(xml(f,'baseline-'+f.id,'Ordinary source update',NOW-600_000)+(state.headlines[f.id]?[state.headlines[f.id]].flat().map((title,i)=>xml(f,'new-'+f.id+'-'+i,title,state.clock-30_000)).join(''):'')+
      (state.news&&f.id==='fed'?'<item><title>Federal Reserve issues FOMC statement</title><link>'+articleUrl(f,'new-decision')+'</link><pubDate>'+new Date(state.clock-30_000).toUTCString()+'</pubDate></item>':''),{headers:{ETag:'test-etag'}});
    if(url.startsWith('https://api.exchange.coinbase.com/products/')) {
      const symbol=new URL(url).pathname.split('/')[2];
      if(url.endsWith('/stats'))return Response.json({open:String(state.previous[symbol]||100)});
      return Response.json({price:String((state.prices[symbol]||100)*(state.confirmationMismatch?1.1:1)),time:new Date(state.clock).toISOString()});
    }
    if(url.startsWith('https://api.kraken.com/')) {
      const symbol=url.includes('XBTUSD')?'BTC-USD':'ETH-USD';
      return Response.json({result:{pair:{c:[String(state.prices[symbol]||100)]}}},{headers:{Date:new Date(state.clock).toUTCString()}});
    }
    if(url.endsWith('/sendMessage')) {
      state.messages.push(JSON.parse(options.body));if(state.lose)throw Error('lost response');
      return Response.json({ok:true,result:{message_id:900+state.messages.length}});
    }
    if(url.endsWith('/editMessageText')) {state.edits.push(JSON.parse(options.body));return Response.json({ok:true,result:{message_id:901}});}
    if(url.endsWith('/answerCallbackQuery'))return Response.json({ok:true,result:true});
    if(url.endsWith('/sendDocument')) {state.uploads++;return Response.json({ok:true,result:{message_id:999,document:{file_id:'exact-chart'}}});}
    if(url.endsWith('/api/analysis/news-angle')||url.endsWith('/api/market/impact-alerts'))return s.fetcher(url,options);
    throw Error('No Render/news/paid provider allowed here');
  };
  const fast=createFast(s.env,{fetcher,now:()=>state.clock});
  return {...s,fast,fastState:state,fetcher};
}

async function warm(s) {await s.fast.tick();for(let i=0;i<5;i++){s.fastState.clock+=60000;await s.fast.tick();}}

// D1 counts a batch as one API query. Instrument the boundary, not SQLite's
// internal statements, to exercise the Free plan's per-invocation allowance.
function meterQueries(db) {
  const prepare=db.prepare.bind(db),batch=db.batch.bind(db),raw=new WeakMap(),calls=[];
  db.prepare=sql=>({bind(...args){const statement=prepare(sql).bind(...args),wrapped={};for(const method of ['first','all','run'])wrapped[method]=(...values)=>{calls.push(sql);return statement[method](...values);};raw.set(wrapped,statement);return wrapped;}});
  db.batch=statements=>{calls.push('BATCH '+statements.length);return batch(statements.map(s=>raw.get(s)||s));};
  return calls;
}

test('combined market shock and three different source announcements fit D1 Free in one invocation',async t=>{
  const s=fixture({DAILY_FAST_LIMIT:'12'});await warm(s);s.fastState.clock+=60_000;
  s.fastState.prices['^IXIC']=97;
  s.fastState.headlines={fed:'Federal Reserve issues FOMC statement',cnbc:'Coinbase exchange confirms major hack', 'bbc-world':'Iran launches attacks in Strait of Hormuz'};
  const calls=meterQueries(s.db),result=await s.fast.tick();
  assert.equal(result.state,'ok');assert.equal(result.prices.state,'market-alert');assert.equal(result.feeds.reduce((n,f)=>n+(f.alerts||0),0),3);
  assert.ok(calls.length<=50,'D1 calls: '+calls.length+'\n'+calls.join('\n'));
  t.diagnostic('D1 API calls, market + three source alerts: '+calls.length+'/50');
});

test('scheduled intake of eight stories and editorial fit D1 Free with the fast monitor',async t=>{
  const s=fixture({DAILY_FAST_LIMIT:'12'});await warm(s);s.fastState.clock=Date.parse('2026-11-05T16:23:00Z');
  s.state.news=Array.from({length:8},(_,i)=>article({title:'Official market announcement '+i,url:'https://www.federalreserve.gov/news-'+i,publishedAt:new Date(s.fastState.clock-60_000).toISOString(),eventKey:'fixture-'+i}));
  const calls=meterQueries(s.db),originalFetch=globalThis.fetch,originalNow=Date.now,tasks=[];
  try {globalThis.fetch=s.fetcher;Date.now=()=>s.fastState.clock;await worker.scheduled({scheduledTime:s.fastState.clock},s.env,{waitUntil:p=>tasks.push(p)});await Promise.all(tasks);}
  finally {globalThis.fetch=originalFetch;Date.now=originalNow;}
  assert.equal(s.state.calls.filter(c=>c.url.endsWith('/api/analysis/news-angle')).length,1);
  assert.ok(calls.length<=50,'D1 calls: '+calls.length+'\n'+calls.join('\n'));
  t.diagnostic('D1 API calls, monitor + intake of eight + editorial: '+calls.length+'/50');
});

test('simultaneous crypto fallback, confirmation and three source alerts fit D1 Free',async t=>{
  const s=fixture({DAILY_FAST_LIMIT:'12'});await warm(s);
  s.fastState.badQuote=true;s.fastState.clock+=60_000;await s.fast.tick();s.fastState.clock+=60_000;await s.fast.tick();
  s.fastState.clock+=60_000;s.fastState.prices['BTC-USD']=92;
  s.fastState.headlines={fed:'Federal Reserve issues FOMC statement',cnbc:'Coinbase exchange confirms major hack','bbc-world':'Iran launches attacks in Strait of Hormuz'};
  const calls=meterQueries(s.db),result=await s.fast.tick();
  assert.equal(result.state,'ok');assert.equal(result.prices.state,'market-alert');assert.equal(result.feeds.reduce((n,f)=>n+(f.alerts||0),0),3);
  assert.ok(calls.length<=50,'D1 calls: '+calls.length+'\n'+calls.join('\n'));
  t.diagnostic('D1 API calls, crypto fallback + confirmation + three alerts: '+calls.length+'/50');
});

test('a second urgent headline from the same source remains available on the next check',async()=>{
  const s=fixture({DAILY_FAST_LIMIT:'12'});await warm(s);s.fastState.clock+=60_000;
  s.fastState.headlines={fed:['Federal Reserve issues FOMC statement','Emergency rate cut after bank failure']};
  await s.fast.tick();assert.equal(s.fastState.messages.length,1);
  s.fastState.clock+=120_000;await s.fast.tick();assert.equal(s.fastState.messages.length,2);
  assert.match(s.fastState.messages[1].text,/Recorte de emergencia/);
});

test('a broad sell-off is not repeated for crypto already covered, but a fresh additional shock can alert',async()=>{
  const s=fixture({DAILY_FAST_LIMIT:'12'});await warm(s);s.fastState.clock+=60_000;
  s.fastState.prices={'^GSPC':99,'^IXIC':95,'^DJI':99,'BTC-USD':94,'ETH-USD':98};
  assert.equal((await s.fast.tick()).prices.state,'market-alert');assert.equal(s.fastState.messages.length,1);
  s.fastState.clock+=60_000;assert.equal((await s.fast.tick()).prices.state,'watching');assert.equal(s.fastState.messages.length,1);
  s.fastState.clock+=60_000;s.fastState.prices['BTC-USD']=91;assert.equal((await s.fast.tick()).prices.state,'market-alert');assert.equal(s.fastState.messages.length,2);
});

test('macro releases and actual rate decisions qualify while speculative previews do not',()=>{
  const source=FAST_FEEDS.find(f=>f.id==='cnbc'),topic=title=>fastTopic({title},source);
  assert.equal(topic('CPI inflation rises unexpectedly in latest data'),'inflation-data');
  assert.equal(topic('Nonfarm payrolls report adds 150,000 jobs'),'employment-data');
  assert.equal(topic('GDP data falls sharply in latest release'),'growth-data');
  assert.equal(topic('Fed cuts interest rates by 25 basis points'),'rates-decision');
  for(const title of ['CPI inflation expected to rise','GDP preview: could data fall?','Fed might cut rates next month','Opinion: unemployment report falls short'])assert.equal(topic(title),null);
});

test('scheduled monitor and delivery of an approved X chart fit D1 Free without AI',async t=>{
  const s=fixture();await warm(s);s.fastState.clock+=60_000;s.fastState.prices['^IXIC']=97;await s.fast.tick();
  const story=s.db.native.prepare('SELECT * FROM stories').get(),channels=createChannels(s.env,{fetcher:s.fetcher,now:()=>s.fastState.clock});
  await channels.pick(story.id,'x',Number(story.message_id));
  const job=s.db.native.prepare('SELECT * FROM channel_jobs').get();await channels.action('genx',story.id,Number(job.draft_message));
  const image=await createImages(s.env,{fetcher:s.fetcher,now:()=>s.fastState.clock}).next(story.id,visualBrief(story));assert.equal(image.provider,'data-chart');
  s.db.native.prepare('INSERT INTO channel_assets(story_id,platform,position,filename,png) VALUES(?,?,?,?,?)').run(story.id,'x',0,'exact-chart.png',Buffer.from(CHART_PIXEL,'base64'));
  s.fastState.clock+=120_000;
  const calls=meterQueries(s.db),originalFetch=globalThis.fetch,originalNow=Date.now,tasks=[];
  try {globalThis.fetch=s.fetcher;Date.now=()=>s.fastState.clock;await worker.scheduled({scheduledTime:s.fastState.clock},s.env,{waitUntil:p=>tasks.push(p)});await Promise.all(tasks);}
  finally {globalThis.fetch=originalFetch;Date.now=originalNow;}
  assert.equal(s.fastState.uploads,1);assert.equal(s.db.native.prepare('SELECT state FROM channel_jobs').get().state,'review');
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM visual_budget').get().n,0);
  assert.ok(calls.length<=50,'D1 calls: '+calls.length+'\n'+calls.join('\n'));t.diagnostic('D1 API calls, monitor + approved X delivery: '+calls.length+'/50');
});
test('market hours respect exchange timezones, weekends, Japanese lunch and US daylight-saving changes',()=>{
  const us=MARKET_ASSETS[0],eu=MARKET_ASSETS[5],jp=MARKET_ASSETS[6];
  assert.equal(marketOpen(us,NOW),true);
  assert.equal(marketOpen(us,Date.parse('2026-11-05T13:59:00Z')),false);
  assert.equal(marketOpen(us,Date.parse('2026-07-06T13:40:00Z')),true);
  assert.equal(marketOpen(us,Date.parse('2026-11-07T16:00:00Z')),false);
  assert.equal(marketOpen(btc,Date.parse('2026-11-07T16:00:00Z')),true);
  assert.equal(marketOpen(eu,NOW),true);
  assert.equal(marketOpen(jp,Date.parse('2026-11-05T03:00:00Z')),false);
});
test('stale, future, delayed, malformed and different-symbol quotes fail closed',()=>{
  assert.ok(quoteFromYahoo(raw(btc),btc,NOW));
  for(const [data] of [[raw(btc,0)],[raw(btc,100,NOW-181000)],[raw(btc,100,NOW+31000)],[raw(btc,100,NOW,0)],
    [raw(btc,100,NOW,100,{exchangeDataDelayedBy:15})],[raw(btc,100,NOW,100,{symbol:'OTHER'})],[{}]])assert.equal(quoteFromYahoo(data,btc,NOW),null);
});
test('fresh consecutive samples are required and the actual elapsed window is reported',()=>{
  const q1=quoteFromYahoo(raw(btc,100),btc,NOW),q2=quoteFromYahoo(raw(btc,97,NOW+360000),btc,NOW+360000);
  let s=appendQuote(null,q1,NOW);assert.equal(marketTrigger(btc,s,NOW),null);
  s=appendQuote(s,quoteFromYahoo(raw(btc,100,NOW+300000),btc,NOW+300000),NOW+300000);s=appendQuote(s,q2,NOW+360000);const t=marketTrigger(btc,s,NOW+360000);
  assert.equal(t.period,'5m');assert.equal(t.windowMinutes,6);assert.equal(t.direction,-1);
  assert.ok(t.change<-2);
  assert.equal(marketTrigger(btc,s,NOW+360000,{at:NOW+350000,direction:-1,dayChange:-3}),null);
});
test('daily bootstrap says observed level and never invents an intraminute crash or a cause',()=>{
  let s=appendQuote(null,quoteFromYahoo(raw(btc,94),btc,NOW),NOW);
  s=appendQuote(s,quoteFromYahoo(raw(btc,94,NOW+60000),btc,NOW+60000),NOW+60000);
  const t=marketTrigger(btc,s,NOW+60000),snapshot={version:1,at:NOW+60000,trigger:t,rows:[{...s.quote,series:s.history}]};
  assert.equal(t.period,'session');
  const e=marketEditorial(snapshot);
  assert.match(e.reasoning,/nivel ya alcanzado/);assert.doesNotMatch(e.tweet,/en 5 minutos|liquidaciones|causado por/);
  assert.equal(xPublication({editorial:e,source_url:s.quote.url}).ready,true);
});
test('RSS parser accepts traceable source URLs and dates but blocks entities, wrong domains and future entries',()=>{
  const feed=FAST_FEEDS[0],good=xml(feed,'valid','Federal Reserve issues FOMC statement',NOW-1000);
  assert.equal(parseFeed(good,feed,NOW).length,1);
  assert.equal(parseFeed(good.replace('www.federalreserve.gov/news/valid','evil.example/news/valid'),feed,NOW).length,0);
  assert.equal(parseFeed('<!DOCTYPE xml [<!ENTITY bad SYSTEM "file:///test">]>'+good,feed,NOW).length,0);
  assert.equal(parseFeed(xml(feed,'future','Future decision',NOW+10000),feed,NOW).length,0);
  assert.equal(fastTopic(parseFeed(good,feed,NOW)[0],feed),'fed-decision');
  assert.equal(fastTopic({title:'Opinion: how to profit from a stock crash'},FAST_FEEDS[2]),null);
  assert.equal(fastTopic({title:'Nasdaq plunges as stocks sell off'},FAST_FEEDS[2]),'market-move');
});
test('fast lane warms up without flooding existing RSS headlines, then detects a price shock without Render or AI',async()=>{
  const s=fixture();await warm(s);
  assert.equal(s.fastState.messages.length,0);
  s.fastState.clock+=60000;s.fastState.prices['BTC-USD']=96;
  const result=await s.fast.tick();assert.equal(result.prices.state,'market-alert');
  assert.equal(s.fastState.messages.length,1);
  const row=s.db.native.prepare("SELECT * FROM stories WHERE state='pending'").get(),e=JSON.parse(row.editorial);
  assert.equal(e.provider,'cotizaciones');assert.equal(e.marketSnapshot.confirmation.source,'Coinbase');
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM daily_budget').get().n,0);
  assert.equal(s.fastState.calls.some(c=>c.url.includes('wastake.onrender.com')),false);
});
test('duplicate minute ticks, cooldown and uncertain delivery cannot duplicate market messages',async()=>{
  const s=fixture();await warm(s);s.fastState.clock+=60000;s.fastState.prices['BTC-USD']=96;s.fastState.lose=true;
  await s.fast.tick();assert.equal(s.fastState.messages.length,1);
  for(let n=0;n<4;n++){s.fastState.clock+=60000;await s.fast.tick();}
  assert.equal(s.fastState.messages.length,1);
  assert.equal(s.db.native.prepare('SELECT notification FROM stories').get().notification,'uncertain');
});
test('a contradictory second crypto source prevents an alert',async()=>{
  const s=fixture();await warm(s);s.fastState.clock+=60000;s.fastState.prices['BTC-USD']=96;s.fastState.confirmationMismatch=true;
  assert.equal((await s.fast.tick()).prices.state,'confirmation-unavailable');assert.equal(s.fastState.messages.length,0);
});
test('provider rate-limit backoff and hard request cap count failures and prevent uncontrolled polling',async()=>{
  const s=fixture();s.fastState.http429=true;await s.fast.tick();
  const urls=s.fastState.calls.filter(c=>c.url.includes('query1.finance')).map(c=>c.url);
  s.fastState.clock+=60000;await s.fast.tick();
  for(const url of urls)assert.equal(s.fastState.calls.filter(c=>c.url===url).length,1);
  const capped=fixture({FAST_REQUEST_LIMIT:'0'});await capped.fast.tick();assert.equal(capped.fastState.calls.length,0);
});
test('shadow mode collects observations but never sends alerts or consumes editorial/image quotas',async()=>{
  const s=fixture({FAST_ALERTS_ENABLED:'false'});await warm(s);s.fastState.clock+=60000;s.fastState.prices['BTC-USD']=90;
  assert.equal((await s.fast.tick()).prices.state,'market-candidate');assert.equal(s.fastState.messages.length,0);
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM daily_budget').get().n,0);
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM visual_budget').get().n,0);
});
test('repairing a legacy English review edits the same Telegram message and exposes the platform buttons',async()=>{
  const s=fixture();await s.fast.tick();s.fastState.clock+=120000;s.fastState.news=true;
  await s.fast.tick();
  const before=s.db.native.prepare('SELECT * FROM stories').get();
  s.db.native.prepare("UPDATE stories SET state='ready', editorial=? WHERE id=?").run(
    JSON.stringify(fastEditorial('La Reserva Federal publica su comunicado', before.source)), before.id);
  const pipeline=createPipeline(s.env,{fetcher:s.fetcher,now:()=>s.fastState.clock});
  await pipeline.tick({collect:false});
  const after=s.db.native.prepare('SELECT * FROM stories').get();assert.equal(after.state,'pending');
  assert.equal(s.fastState.edits.length,1);assert.equal(s.fastState.edits[0].message_id,Number(before.message_id));
  assert.ok(s.fastState.edits[0].reply_markup.inline_keyboard.flat().some(button=>button.callback_data==='pickx:'+before.id));
  assert.equal(s.fastState.messages.length,1);assert.equal(s.fastState.calls.some(c=>c.url.endsWith('/api/market/impact-alerts')),false);
});
test('the first RSS message is Spanish and its X button works with the editorial budget exhausted',async()=>{
  const s=fixture();s.db.native.prepare('INSERT INTO daily_budget(day,used) VALUES(?,6)').run('2026-11-05');
  await s.fast.tick();s.fastState.clock+=120000;s.fastState.news=true;await s.fast.tick();
  assert.equal(s.fastState.messages.length,1);
  const card = s.fastState.messages[0];
  assert.match(card.text,/La Reserva Federal publica el comunicado del FOMC/);
  assert.doesNotMatch(card.text,/Federal Reserve issues|pendiente de cupo/);
  const story = s.db.native.prepare('SELECT * FROM stories').get();
  const buttons = card.reply_markup.inline_keyboard.flat();
  for (const action of ['pickx','picki','pickt','reject']) {
    assert.ok(buttons.some(button=>button.callback_data===action+':'+story.id),action);
  }
  assert.ok(buttons.some(button=>button.url===story.source_url));
  assert.equal(story.state,'pending');
  const p=createPipeline(s.env,{fetcher:s.fetcher,now:()=>s.fastState.clock});await p.tick({collect:false});
  assert.equal(s.db.native.prepare('SELECT used FROM daily_budget WHERE day=?').get('2026-11-05').used,6);
  assert.equal(s.fastState.calls.some(c=>c.url.endsWith('/api/analysis/news-angle')),false);
  assert.equal(s.fastState.edits.length,0);
  await p.handleUpdate({callback_query:{
    id:'test-x',from:{id:123},message:{message_id:Number(story.message_id),chat:{id:123}},data:'pickx:'+story.id
  }});
  assert.equal(s.db.native.prepare('SELECT state FROM channel_jobs').get().state,'draft');
  assert.match(s.fastState.messages[1].text,/Borrador · Twitter\/X/);
  assert.equal(s.fastState.uploads,0);
});
test('a failed translation stays unseen and is retried against a normally conditional RSS',async()=>{
  const s=fixture();await s.fast.tick();s.fastState.clock+=120000;s.fastState.news=true;
  s.fastState.translationOverride = 'Federal Reserve issues FOMC statement';
  const result = await s.fast.tick();
  assert.equal(result.feeds.find(f=>f.source==='fed').state,'translation-pending');
  assert.equal(s.fastState.messages.length,0);
  assert.equal((await s.fast.status()).budget.alerts_used,0);
  s.fastState.translationOverride = null;
  s.fastState.unchangedFeed = true;
  s.fastState.clock+=120000;
  await s.fast.tick();
  assert.equal(s.fastState.messages.length,1);
  assert.equal(s.fastState.translationCalls.length,2);
});
test('normal editorials leave two calls available for urgent stories without raising the six-call total',async()=>{
  const s=fixture();s.db.native.prepare('INSERT INTO daily_budget(day,used) VALUES(?,4)').run('2026-11-05');
  const insert=(id,priority)=>s.db.native.prepare("INSERT INTO stories(id,event_key,title,source,source_url,published_at,priority,snippet,created_at) VALUES(?,?,?,'Fixture',?,?,?,?,?)").run(id,id,'Source title fixture','https://www.bbc.com/news/'+id,NOW,priority,'Fixture',NOW);
  insert('a'.repeat(32),90);const p=createPipeline(s.env,{fetcher:s.fetcher,now:()=>NOW});await p.tick({collect:false});
  assert.equal(s.db.native.prepare('SELECT used FROM daily_budget').get().used,4);
  insert('b'.repeat(32),100);await p.tick({collect:false});assert.equal(s.db.native.prepare('SELECT used FROM daily_budget').get().used,5);
});
test('market chart uses approved stored numbers and consumes no image-generation quota, even at the cap',async()=>{
  const s=fixture();await warm(s);s.fastState.clock+=60000;s.fastState.prices['BTC-USD']=96;await s.fast.tick();
  const story=s.db.native.prepare('SELECT * FROM stories').get();
  s.db.native.prepare("UPDATE stories SET state='approved'").run();
  const brief=visualBrief(story);assert.equal(brief.art.mediaKind,'market-chart');
  const image=await createImages(s.env,{now:()=>s.fastState.clock,fetcher:s.fetcher,generate:async()=>{throw Error('must never generate');}}).next(story.id,brief);
  assert.equal(image.provider,'data-chart');assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM visual_budget').get().n,0);
  const html=marketGraph(brief.marketSnapshot,{title:brief.title});assert.match(html,/Gráfico con datos; no utiliza IA/);
  assert.match(html,/Coinbase/);assert.doesNotMatch(html,/ILUSTRACIÓN CON IA/);
});
test('monitor status remains private and disabled mode makes no requests',async()=>{
  const s=fixture({FAST_ENABLED:'false'});assert.equal((await s.fast.tick()).state,'disabled');assert.equal(s.fastState.calls.length,0);
  for(const path of ['/admin/fast-status','/admin/fast-tick'])assert.equal((await worker.fetch(new Request('https://test'+path),s.env,{})).status,401);
});

test('crypto falls back to timestamped Coinbase quotes and independently confirms with Kraken',async()=>{
  const s=fixture();s.fastState.http429=true;await warm(s);s.fastState.clock+=60000;s.fastState.prices['BTC-USD']=96;
  const result=await s.fast.tick();assert.equal(result.prices.state,'market-alert');
  const e=JSON.parse(s.db.native.prepare('SELECT editorial FROM stories').get().editorial);
  assert.equal(e.marketSnapshot.confirmation.source,'Kraken');
  const q=e.marketSnapshot.rows.find(r=>r.symbol==='BTC-USD');assert.equal(q.source,'Coinbase');assert.equal(q.basis,'24h-ref');
  assert.match(marketGraph(e.marketSnapshot,{title:e.headline}),/ref\.24h/);
});
test('changing price provider or comparison basis resets the sample window instead of fabricating a jump',()=>{
  const q=quoteFromYahoo(raw(btc),btc,NOW),s=appendQuote(null,q,NOW);
  const next=appendQuote(s,{...q,source:'Coinbase',basis:'24h-ref',at:NOW+60000,price:95},NOW+60000);
  assert.equal(next.history.length,1);assert.equal(marketTrigger(btc,next,NOW+60000),null);
});
test('market alerts leave budget for genuinely new source announcements',async()=>{
  const s=fixture();await warm(s);s.db.native.prepare("UPDATE fast_budget SET alerts_used=4 WHERE day='2026-11-05'").run();
  s.fastState.clock+=60000;s.fastState.prices['BTC-USD']=96;s.fastState.news=true;
  const result=await s.fast.tick();assert.equal(result.prices.state,'alert-limit');
  assert.equal(s.fastState.messages.length,1);assert.match(s.fastState.messages[0].text,/La Reserva Federal/);
  assert.equal(s.db.native.prepare('SELECT alerts_used FROM fast_budget').get().alerts_used,5);
});
test('overlapping fast ticks share a durable lock and cannot double polling or alerts',async()=>{
  const s=fixture(),results=await Promise.all([s.fast.tick(),s.fast.tick()]);
  assert.equal(results.filter(r=>r.state==='busy').length,1);assert.equal(s.fastState.messages.length,0);
  const urls=s.fastState.calls.filter(c=>c.url.includes('query1.finance')).map(c=>c.url);assert.equal(new Set(urls).size,urls.length);
});
