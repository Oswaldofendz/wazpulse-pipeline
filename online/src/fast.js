import {store} from './store.js';
import {digest,normalizeStory} from './domain.js';
import {readBounded,network,telegram,ServiceError} from './io.js';
import {newsCard,newsButtons} from './channels.js';
import {MARKET_ASSETS,marketOpen,quoteFromYahoo,appendQuote,marketTrigger,marketEditorial,pct} from './fast-market.js';
import {FAST_FEEDS,parseFeed,fastTopic} from './fast-feeds.js';
import {fastEditorial, translateFastHeadline} from './fast-editorial.js';

const DAY=86_400_000;
const integer=(value,fallback,max)=>/^\d+$/.test(String(value))&&Number(value)<=max?Number(value):fallback;
const stateKey=symbol=>'fast_quote:'+symbol;
export function createFast(env,{fetcher=fetch,now=Date.now}={}) {
  const db=store(env.DB),bot=telegram(env,network(fetcher));
  const enabled=()=>env.PIPELINE_ENABLED==='true'&&env.FAST_ENABLED==='true';
  const notifying=()=>env.FAST_ALERTS_ENABLED==='true'&&env.TELEGRAM_BOT_TOKEN&&env.TELEGRAM_CHAT_ID;
  let cached=null,loading=null,budgetReady=null,requestQueue=[],requestScheduled=false;
  let batchChain=Promise.resolve();
  const batch=statements=>{const task=batchChain.then(()=>env.DB.batch(statements));batchChain=task.catch(()=>{});return task;};
  const dirty=new Map();
  const load=()=>loading||(loading=db.all("SELECT key,value FROM runtime WHERE key GLOB 'fast_*'").then(rows=>{cached=new Map(rows.map(r=>[r.key,JSON.parse(r.value)]));}));
  const value=async key=>{await load();return cached.get(key)??null;};
  const setValue=async(key,data)=>{await load();cached.set(key,data);dirty.set(key,data);};
  async function flush(){if(!dirty.size)return;const rows=[...dirty];dirty.clear();try{await batch(rows.map(([key,data])=>env.DB.prepare('INSERT INTO runtime(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value').bind(key,JSON.stringify(data))));}catch(error){for(const [key,data] of rows)if(!dirty.has(key))dirty.set(key,data);throw error;}}
  const day=()=>new Date(now()).toISOString().slice(0,10);
  const ensureBudget=()=>budgetReady||(budgetReady=db.run('INSERT OR IGNORE INTO fast_budget(day) VALUES(?)',day()));
  function reserveRequest() {
    // Coalesce concurrent HTTP reservations into one atomic D1 call. Every
    // attempted request still counts, including failures and confirmations.
    return new Promise((resolve,reject)=>{
      requestQueue.push({resolve,reject});
      if(requestScheduled)return;requestScheduled=true;
      queueMicrotask(async()=>{
        const pending=requestQueue;requestQueue=[];requestScheduled=false;
        try {await ensureBudget();const n=pending.length;
          const allowed=Boolean(await db.first('UPDATE fast_budget SET requests_used=requests_used+? WHERE day=? AND requests_used+?<=? RETURNING requests_used',n,day(),n,integer(env.FAST_REQUEST_LIMIT,12_000,15_000)));
          for(const p of pending)p.resolve(allowed);
        }catch(error){for(const p of pending)p.reject(error);}
      });
    });
  }
  async function reserveAlert(kind) {
    const cap=integer(env.DAILY_FAST_LIMIT,12,24);
    await ensureBudget();
    return Boolean(await db.first('UPDATE fast_budget SET alerts_used=alerts_used+1 WHERE day=? AND alerts_used<? RETURNING alerts_used',day(),kind==='market'?Math.max(0,cap-Math.min(4,Math.floor(cap/3))):cap));
  }
  async function get(url,key,{max=160_000,json=true,conditional=false}={}) {
    const prior=await value('fast_http:'+key)||{};
    if(prior.nextAttempt>now())return {state:'backoff'};
    if(!await reserveRequest())return {state:'request-limit'};
    const attemptedAt=now(),headers={'User-Agent':'WaCapital/1.0 public-market-and-headline-monitor'};
    if(conditional&&prior.etag)headers['If-None-Match']=prior.etag;
    if(conditional&&prior.modified)headers['If-Modified-Since']=prior.modified;
    await setValue('fast_http:'+key,{...prior,attemptedAt,nextAttempt:attemptedAt+45_000});
    try {
      const r=await fetcher(url,{headers,redirect:'manual',signal:AbortSignal.timeout(8000)});
      if(r.status===304){await setValue('fast_http:'+key,{...prior,attemptedAt,lastSuccess:now(),status:304,nextAttempt:attemptedAt+45_000});return {state:'unchanged'};}
      if(!r.ok) {
        const seconds=Number(r.headers.get('Retry-After'));
        const until=Number.isFinite(seconds)&&seconds>0?now()+Math.min(86400,seconds)*1000:Math.max(now()+60_000,Date.parse(r.headers.get('Retry-After')||'')||0);
        const failures=(prior.failures||0)+1;
        await setValue('fast_http:'+key,{...prior,attemptedAt,status:r.status,failures,nextAttempt:r.status===429?until:now()+Math.min(30*60_000,60_000*2**Math.min(failures,5))});
        return {state:'http-error',status:r.status};
      }
      const text=await readBounded(r,max),data=json?JSON.parse(text):text;
      await setValue('fast_http:'+key,{attemptedAt,lastSuccess:now(),status:200,failures:0,nextAttempt:attemptedAt+45_000,
        ...(conditional?{etag:(r.headers.get('etag')||'').slice(0,200),modified:(r.headers.get('last-modified')||'').slice(0,100)}:{})});
      return {state:'ok',data,first:!prior.lastSuccess,serverAt:Date.parse(r.headers.get('Date')||''),cacheAge:Number(r.headers.get('Age')||0)};
    }catch {
      const failures=(prior.failures||0)+1;
      await setValue('fast_http:'+key,{...prior,attemptedAt,status:0,failures,nextAttempt:now()+Math.min(30*60_000,60_000*2**Math.min(failures,5))});
      return {state:'unavailable'};
    }
  }
  async function status() {
    const last=await value('fast_last_tick'),budget=await db.first('SELECT * FROM fast_budget WHERE day=?',day());
    const quotes=await Promise.all(MARKET_ASSETS.map(async a=>{const s=await value(stateKey(a.symbol));return {symbol:a.symbol,name:a.name,open:marketOpen(a,now()),
      quoteAt:s?.quote?.at||null,ageSeconds:s?.quote?Math.round((now()-s.quote.at)/1000):null,price:s?.quote?.price??null,dayChange:s?.quote?.dayChange??null,source:s?.quote?.source||null,basis:s?.quote?.basis||null,quality:await value('fast_quality:'+a.symbol)};}));
    const feeds=await Promise.all(FAST_FEEDS.map(async f=>({id:f.id,name:f.name,...await value('fast_http:feed:'+f.id)})));
    const events=(await db.all('SELECT f.story_id,f.kind,f.topic,f.detected_at,f.published_at,f.delivered_at,s.notification,s.title FROM fast_events f JOIN stories s ON s.id=f.story_id ORDER BY f.detected_at DESC LIMIT 5')).map(e=>({...e,sourceToDeliverySeconds:e.delivered_at?Math.round((e.delivered_at-e.published_at)/1000):null}));
    return {enabled:enabled(),alertsEnabled:env.FAST_ALERTS_ENABLED==='true',dailyAlertLimit:integer(env.DAILY_FAST_LIMIT,12,24),requestLimit:integer(env.FAST_REQUEST_LIMIT,12_000,15_000),budget,last,quotes,feeds,events};
  }
  async function emitMarket(trigger,states) {
    if((await db.first('SELECT COUNT(*) AS n FROM stories')).n>=5000)return {state:'story-cap'};
    const rows=states.map(s=>({...s.quote,series:s.history.filter(p=>p.at>=now()-3_600_000).map(p=>({at:p.at,price:p.price}))})).filter(r=>now()-r.at<=180_000);
    const asset=MARKET_ASSETS.find(a=>a.symbol===trigger.symbol),lead=rows.find(r=>r.symbol===trigger.symbol);
    let confirmation=null;
    if(asset.coinbase) {
      const kraken=lead.source==='Coinbase';
      const result=await get(kraken?'https://api.kraken.com/0/public/Ticker?pair='+(asset.symbol==='BTC-USD'?'XBTUSD':'ETHUSD'):
        'https://api.exchange.coinbase.com/products/'+asset.coinbase+'/ticker','confirm:'+(kraken?'kraken:':'coinbase:')+asset.coinbase);
      const price=Number(kraken?Object.values(result.data?.result||{})[0]?.c?.[0]:result.data?.price),at=kraken?result.serverAt:Date.parse(result.data?.time);
      if(result.state!=='ok'||!Number.isFinite(price)||price<=0||!Number.isFinite(at)||at>now()+30_000||now()-at>90_000||Math.abs(pct(price,lead.price))>1.5)return {state:'confirmation-unavailable'};
      if(kraken&&result.cacheAge>30)return {state:'confirmation-unavailable'};
      confirmation={source:kraken?'Kraken':'Coinbase',symbol:asset.symbol,price,at};
    }
    if(!await reserveAlert('market'))return {state:'alert-limit'};
    const snapshot={version:1,at:now(),trigger,rows,confirmation};
    const id=(await digest('fast-market:'+day()+':'+trigger.symbol+':'+trigger.period+':'+Math.floor(now()/300_000))).slice(0,32);
    const editorial=marketEditorial(snapshot);
    const saved=await db.run("INSERT OR IGNORE INTO stories(id,event_key,title,source,source_url,published_at,priority,snippet,assets,state,editorial,created_at) VALUES(?,?,?,?,?,?,100,?,?,'ready',?,?)",id,'fast-market:'+id,editorial.headline,'Cotizaciones · '+lead.source,lead.url,now(),editorial.reasoning,JSON.stringify([trigger.symbol]),JSON.stringify(editorial),now());
    if(!saved.meta?.changes)return {state:'duplicate'};
    await db.run("INSERT INTO fast_events(story_id,kind,topic,detected_at,published_at,detail) VALUES(?,'market',?,?,?,?)",id,'market:'+trigger.direction,now(),lead.at,JSON.stringify({trigger,quoteAt:lead.at}));
    await setValue('fast_alert:market:'+asset.session+':'+trigger.direction,{at:now(),direction:trigger.direction,dayChange:lead.dayChange,symbol:lead.symbol});
    // Keep a per-symbol level to suppress persistent daily-limit alerts.
    await setValue('fast_level:'+trigger.symbol,{at:now(),direction:trigger.direction,dayChange:lead.dayChange});
    if(editorial.headline.startsWith('🚨 BOLSA Y CRIPTO EN ROJO'))await setValue('fast_basket:'+trigger.direction,{at:now(),rows:rows.map(r=>({symbol:r.symbol,price:r.price,dayChange:r.dayChange,basis:r.basis,source:r.source}))});
    const claimed=await db.notification(id);
    if(!claimed)return {state:'duplicate'};
    await flush();
    try {
      const sent=await bot.send(newsCard(claimed),newsButtons(id));
      if(!Number.isSafeInteger(sent?.message_id))throw new ServiceError('telegram-review');
      await batch([env.DB.prepare("UPDATE stories SET state='pending',notification='sent',message_id=? WHERE id=?").bind(String(sent.message_id),id),env.DB.prepare('UPDATE fast_events SET delivered_at=? WHERE story_id=?').bind(now(),id)]);
      return {state:'market-alert',id};
    }catch {await db.uncertain(id);return {state:'delivery-uncertain',id};}
  }
  async function prices() {
    const minute=Math.floor(now()/60_000);
    const requested=MARKET_ASSETS.filter((a,i)=>marketOpen(a,now())&&(!['future'].includes(a.session)||minute%2===i%2));
    await Promise.all(requested.map(async asset=>{
      const r=await get('https://query1.finance.yahoo.com/v8/finance/chart/'+encodeURIComponent(asset.symbol)+'?interval=5m&range=1d','quote:'+asset.symbol);
      let quote=r.state==='ok'?quoteFromYahoo(r.data,asset,now()):null;
      if(!quote&&asset.coinbase)quote=await coinbaseQuote(asset);
      if(!quote){await setValue('fast_quality:'+asset.symbol,{at:now(),state:'stale-delayed-or-invalid'});return;}
      const prior=await value(stateKey(asset.symbol));
      await setValue(stateKey(asset.symbol),appendQuote(prior,quote,now()));
      await setValue('fast_quality:'+asset.symbol,{at:now(),state:'fresh'});
    }));
    const states=(await Promise.all(MARKET_ASSETS.map(a=>value(stateKey(a.symbol))))).filter(Boolean);
    const candidates=[];
    for(const asset of MARKET_ASSETS) {
      const state=states.find(s=>s.quote.symbol===asset.symbol),saved=await value('fast_level:'+asset.symbol);
      const last=saved&&new Date(saved.at).toISOString().slice(0,10)===day()?saved:null;
      const t=marketTrigger(asset,state,now(),last);
      if(t)candidates.push(t);
    }
    for(const trigger of candidates.sort((a,b)=>b.score-a.score)) {
      const group=MARKET_ASSETS.find(a=>a.symbol===trigger.symbol).session;
      const global=await value('fast_alert:market:'+group+':'+trigger.direction);
      const lead=states.find(s=>s.quote.symbol===trigger.symbol)?.quote;
      // A broad sell-off card already covers its other quoted assets. Do not
      // send the same sell-off again merely because another family qualifies.
      const basket=await value('fast_basket:'+trigger.direction),covered=basket?.rows.find(r=>r.symbol===trigger.symbol);
      if(covered&&now()-basket.at<45*60_000&&covered.source===lead.source&&covered.basis===lead.basis) {
        const asset=MARKET_ASSETS.find(a=>a.symbol===trigger.symbol),additional=pct(lead.price,covered.price)*(asset.inverse?-1:1)*trigger.direction;
        if(trigger.period==='session'?(now()-basket.at<10*60_000||Math.abs(lead.dayChange)-Math.abs(covered.dayChange)<2):additional<trigger.threshold)continue;
      }
      if(global&&new Date(global.at).toISOString().slice(0,10)===day()&&now()-global.at<45*60_000&&!(global.symbol===trigger.symbol&&now()-global.at>=10*60_000&&Math.abs(lead.dayChange)-Math.abs(global.dayChange)>=2))continue;
      if(!notifying())return {state:'market-candidate',symbol:trigger.symbol,period:trigger.period};
      return emitMarket(trigger,states);
    }
    return {state:'watching',fresh:states.filter(s=>now()-s.quote.at<=180_000).length};
  }
  async function coinbaseQuote(asset) {
    const ticker=await get('https://api.exchange.coinbase.com/products/'+asset.coinbase+'/ticker','fallback:'+asset.coinbase);
    let ref=await value('fast_coinbase_ref:'+asset.coinbase);
    if(!ref||now()-ref.at>5*60_000) {
      const stats=await get('https://api.exchange.coinbase.com/products/'+asset.coinbase+'/stats','fallback-stats:'+asset.coinbase),open=Number(stats.data?.open);
      if(stats.state!=='ok'||!Number.isFinite(open)||open<=0)return null;
      ref={at:now(),open};await setValue('fast_coinbase_ref:'+asset.coinbase,ref);
    }
    const price=Number(ticker.data?.price),at=Date.parse(ticker.data?.time);
    if(ticker.state!=='ok'||!Number.isFinite(price)||price<=0||!Number.isFinite(at)||at>now()+30_000||now()-at>90_000)return null;
    return {symbol:asset.symbol,name:asset.name,short:asset.short,price,previous:ref.open,at,receivedAt:now(),dayChange:pct(price,ref.open),
      currency:'USD',source:'Coinbase',basis:'24h-ref',referenceAt:ref.at,url:'https://www.coinbase.com/price/'+(asset.symbol==='BTC-USD'?'bitcoin':'ethereum')};
  }
  async function feed(feed) {
    const primed = await value('fast_feed_primed:' + feed.id);
    const httpKey = 'fast_http:feed:' + feed.id;
    const response = await get(feed.url, 'feed:' + feed.id, {json: false, conditional: Boolean(primed)});
    if (response.state !== 'ok') return {source: feed.id, state: response.state};
    const items = parseFeed(response.data, feed, now());
    if (!items.length) return {source: feed.id, state: 'empty-or-invalid'};
    const known = await db.all('SELECT url FROM fast_seen WHERE url IN (' + items.map(() => '?').join(',') + ')', ...items.map(a => a.url));
    const seenUrls = new Set(known.map(row => row.url));
    const completed = [];
    let sent = 0, attempts = 0;
    let state = primed ? 'checked' : 'primed';
    try {
      for (const article of items) {
        if (seenUrls.has(article.url)) continue;
        completed.push(article.url);
        if (!primed || !notifying() || article.published < now() - 20 * 60_000) continue;
        const topic = fastTopic(article, feed);
        if (!topic) continue;
        const last = await value('fast_alert:news:' + topic);
        if (last && now() - last.at < 20 * 60_000) continue;

        const story = await normalizeStory({
          title: article.title, url: article.url, publishedAt: new Date(article.published).toISOString(),
          source: article.source, priority: 100, snippet: article.snippet, explicitAssets: []
        }, now());
        if (!story) continue;
        const capacity = await db.first(
          'SELECT COUNT(*) AS n, EXISTS(SELECT 1 FROM stories WHERE source_url=?) AS existing, (SELECT alerts_used FROM fast_budget WHERE day=?) AS used FROM stories',
          story.url, day()
        );
        if (capacity.existing) continue;
        if (capacity.n >= 5000) return {source: feed.id, state: 'story-cap'};
        if (capacity.used >= integer(env.DAILY_FAST_LIMIT, 12, 24)) return {source: feed.id, state: 'alert-limit'};

        // Force the next RSS check to include its body. An unchanged response
        // would otherwise hide a deferred translation or a second headline.
        await setValue(httpKey, {...await value(httpKey), etag: '', modified: ''});
        const headline = feed.id === 'bbc-es' ? story.title : await translateFastHeadline(env, db, story.title, now());
        if (!headline) {
          completed.pop();
          state = 'translation-pending';
          break;
        }
        if (!await reserveAlert('news')) return {source: feed.id, state: 'alert-limit'};
        const cooldown = {at: now(), id: story.id};
        const claimedTopic = await db.first(
          "INSERT INTO runtime(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value WHERE CAST(json_extract(runtime.value,'$.at') AS INTEGER)<=? RETURNING key",
          'fast_alert:news:' + topic, JSON.stringify(cooldown), now() - 20 * 60_000
        );
        if (!claimedTopic) break;
        const saved = await db.insert(story, now(), fastEditorial(headline, story.source));
        if (!saved.meta?.changes) break;
        await db.run(
          "INSERT INTO fast_events(story_id,kind,topic,detected_at,published_at,detail) VALUES(?,'news',?,?,?,?)",
          story.id, topic, now(), article.published,
          JSON.stringify({source: feed.id, basis: 'source-headline', language: 'es'})
        );
        await setValue('fast_alert:news:' + topic, cooldown);
        const claimed = await db.notification(story.id);
        if (!claimed) break;
        attempts++;
        await flush();
        try {
          const message = await bot.send(newsCard(claimed), newsButtons(story.id, story.url));
          if (!Number.isSafeInteger(message?.message_id)) throw new ServiceError('telegram-alert');
          await batch([
            env.DB.prepare("UPDATE stories SET state='pending',notification='sent',message_id=? WHERE id=?")
              .bind(String(message.message_id), story.id),
            env.DB.prepare('UPDATE fast_events SET delivered_at=? WHERE story_id=?').bind(now(), story.id)
          ]);
          sent++;
        } catch {
          await db.uncertain(story.id);
        }
        break;
      }
    } finally {
      if (completed.length) {
        await batch(completed.map(url => env.DB.prepare('INSERT OR IGNORE INTO fast_seen(url,seen_at) VALUES(?,?)')
          .bind(url, now())));
      }
    }
    if (!primed) await setValue('fast_feed_primed:' + feed.id, {at: now()});
    return {source: feed.id, state, items: items.length, alerts: sent, attempts};
  }
  async function tick() {
    if(!enabled())return {state:'disabled'};
    const owner=crypto.randomUUID(),instant=now();
    if(!await db.lock('fast',owner,instant,55_000))return {state:'busy'};
    cached=null;loading=null;budgetReady=null;dirty.clear();
    const result={at:instant,state:'ok',notifying:Boolean(notifying())};
    try {
      result.prices=await prices();
      const parity=Math.floor(instant/60_000)%2;
      result.feeds=await Promise.all(FAST_FEEDS.filter((_,i)=>i%2===parity).map(f=>feed(f)));
      // A bounded TTL keeps URL deduplication and provider history small.
      if(new Date(instant).getUTCMinutes()===1)await db.run('DELETE FROM fast_seen WHERE url IN (SELECT url FROM fast_seen WHERE seen_at<? LIMIT 100)',instant-7*DAY);
    }catch {result.state='unavailable';}
    finally {await setValue('fast_last_tick',result);await flush();await db.unlock('fast',owner);}
    return result;
  }
  async function probeCrypto() {
    const quote=await coinbaseQuote(MARKET_ASSETS.find(a=>a.symbol==='BTC-USD'));
    const r=await get('https://api.kraken.com/0/public/Ticker?pair=XBTUSD','probe:kraken'),price=Number(Object.values(r.data?.result||{})[0]?.c?.[0]);
    await flush();
    return {coinbase:quote?{price:quote.price,ageSeconds:Math.round((now()-quote.at)/1000),basis:quote.basis}:null,
      kraken:r.state==='ok'&&Number.isFinite(price)?{price,serverAgeSeconds:Math.round((now()-r.serverAt)/1000),cacheAge:r.cacheAge}:null,
      corroborated:Boolean(quote&&Number.isFinite(price)&&Math.abs(pct(price,quote.price))<=1.5&&Number.isFinite(r.serverAt)&&now()-r.serverAt<=90_000&&r.cacheAge<=30)};
  }
  return {tick,status,probeCrypto};
}
