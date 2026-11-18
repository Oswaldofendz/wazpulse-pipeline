import test from 'node:test';
import assert from 'node:assert/strict';
import {marketStory,draftTikTok,TIKTOK_STORY} from '../src/tiktok-content.js';
import {socialKitV2,socialHtml} from '../src/social.js';
import {marketEditorial} from '../src/fast-market.js';
import {store} from '../src/store.js';
import {database,NOW} from './helpers.js';

const snapshot={version:1,at:NOW,trigger:{symbol:'BTC-USD',period:'session',change:-5,direction:-1},rows:[
  {symbol:'BTC-USD',name:'Bitcoin',short:'BTC',price:65000,previous:68421.05,dayChange:-5,currency:'USD',basis:'close',at:NOW,series:[{at:NOW-60000,price:65080},{at:NOW,price:65000},{at:NOW+60000,price:64000}]},
  {symbol:'ETH-USD',name:'Ethereum',short:'ETH',price:1800,previous:1900,dayChange:-5.26,currency:'USD',basis:'24h-ref',at:NOW}
]};
test('new TikTok market editions use three different cards, accurate values and only observed history; frozen editions retain their layout',()=>{
  const story={id:'a'.repeat(32),title:'Bitcoin cae',source:'Prueba',source_url:'https://finance.yahoo.com/quote/BTC-USD/',published_at:NOW,editorial:JSON.stringify(marketEditorial(snapshot))};
  const kit=socialKitV2(story,{platform:'tiktok'});
  assert.equal(kit.presentation,TIKTOK_STORY);
  assert.deepEqual(kit.slides.map(s=>s.visual),['market-hero','market-line','market-compare']);
  assert.equal(kit.slides[0].metric,'−5,00%');
  assert.equal(kit.slides[1].metric,'65.000');
  assert.equal(kit.slides[1].points.length,2);
  assert.match(kit.slides[2].body,/propia referencia/);
  assert.match(socialHtml(kit,'AAAA',0,'tiktok'),/Ilustración de BTC/);
  assert.match(socialHtml(kit,'AAAA',1,'tiktok'),/Cotizaciones observadas/);
  const old=socialKitV2(story);
  assert.equal(old.presentation,undefined);
  assert.equal(old.slides.length,5);
  assert.doesNotMatch(socialHtml(old,'AAAA',0,'tiktok'),/class="comparisons"/);
  assert.throws(()=>marketStory({...snapshot,rows:[{...snapshot.rows[0],previous:0}]}));
});
const source={id:'b'.repeat(32),title:'Hackers planted crypto-stealing code on 178 sites, including HSBC and Ledger',snippet:'The malicious code could steal cryptocurrency. The sites included HSBC and Ledger.',source_url:'https://example.test/news',editorial:'{"headline":"Noticia"}'};
const valid={title:'Código malicioso en 178 sitios',slides:[
  {title:'178 sitios, un código malicioso',body:'La noticia identifica sitios comprometidos.',evidence:'Hackers planted crypto-stealing code on 178 sites'},
  {title:'El objetivo: las criptomonedas',body:'El código podía robar criptomonedas.',evidence:'The malicious code could steal cryptocurrency'},
  {title:'Entre los sitios: HSBC y Ledger',body:'Ambos nombres aparecen en la fuente.',evidence:'The sites included HSBC and Ledger'}
],question:'¿Cómo afecta este caso a la confianza?'};
test('TikTok scripting sends the real source as data and permits one bounded explicit retry within the shared budget',async()=>{
  const db=database(),state={calls:0},env={DAILY_LLM_LIMIT:'6',AI:{async run(model,input){state.calls++;assert.ok(input.messages[1].content.includes(source.title));return {response:JSON.stringify(valid)};}}};
  const result=await draftTikTok(source,env,store(db),NOW);
  assert.equal(result.socialSlides.length,3);assert.equal(result.socialSlides[0].evidence,valid.slides[0].evidence);
  assert.equal(await draftTikTok(source,env,store(db),NOW),null);
  assert.equal(state.calls,1);
  assert.equal(db.native.prepare('SELECT used FROM daily_budget').get().used,1);
  assert.ok(await draftTikTok(source,env,store(db),NOW,{retry:true}));
  assert.equal(await draftTikTok(source,env,store(db),NOW,{retry:true}),null);
  assert.equal(state.calls,2);
  assert.equal(db.native.prepare('SELECT used FROM daily_budget').get().used,2);
});
test('unsupported numbers, fabricated evidence and exhausted daily budget cannot yield a ready script',async()=>{
  for(const mutate of [data=>data.title='Hay 999 sitios',data=>data.slides[1].body='Robaron 999 millones.',data=>data.slides[0].evidence='Evidence absent from the article']) {
    const data=structuredClone(valid);mutate(data);const db=database();
    assert.equal(await draftTikTok(source,{AI:{run:async()=>({response:JSON.stringify(data)})}},store(db),NOW),null);
  }
  const db=database();db.native.prepare('INSERT INTO daily_budget VALUES(?,6)').run(new Date(NOW).toISOString().slice(0,10));
  assert.equal(await draftTikTok(source,{DAILY_LLM_LIMIT:'6',AI:{run:async()=>{throw Error('must not call');}}},store(db),NOW),null);
});
