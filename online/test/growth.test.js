import test from 'node:test';
import assert from 'node:assert/strict';
import {auditStats,auditRequest} from '../src/audit.js';
import {topicTags,creativePolicy} from '../src/growth-policy.js';
import {marketStoryV2} from '../src/market-story.js';
import {marketGrowthHtml} from '../src/growth-layout.js';
import {socialHtml,socialKitV2} from '../src/social.js';
import {sourceUnits,validateScript} from '../src/tiktok-content.js';
import {NOW} from './helpers.js';

test('source validation preserves decimal magnitudes across English and Spanish formatting',()=>{
  const source=sourceUnits({title:'Bitcoin dropped 5.02% to 68,238.21 dollars.',snippet:'The source reports the observed movement only.'});
  const script={title:'Bitcoin cae un 5,02%',question:'¿Qué dato falta?',slides:[
    {title:'Bitcoin: caída del 5,02%',body:'La fuente señala el movimiento.',evidenceIds:['t0']},
    {title:'Precio de 68.238,21 dólares',body:'Precio citado en el titular.',evidenceIds:['t0']},
    {title:'Qué está documentado',body:'La fuente describe el movimiento observado.',evidenceIds:['e0']}
  ]};
  assert.ok(validateScript(script,source));
  assert.equal(validateScript({...script,title:'Bitcoin cae un 502%'},source),null);
  assert.equal(validateScript({...script,slides:script.slides.map((slide,i)=>i===1?{...slide,title:'Precio de 6823821 dólares'}:slide)},source),null);
});

test('audit rates use their own measured denominators and preserve unknown fields instead of treating them as zero',async()=>{
  const rows=[
    {views:1000,reach:400,likes:20,comments:5,shares:10,saves:20},
    {views:100,reach:90,likes:2,comments:0,shares:10,saves:null},
    {views:800,reach:null,likes:null,comments:null,shares:null,saves:8}
  ];
  const result=auditStats(rows);
  assert.equal(result.median,800);
  assert.equal(result.measured,2);
  assert.equal(result.shareSample,2);
  assert.ok(Math.abs(result.shareRate-20000/1100)<1e-9);
  assert.ok(Math.abs(result.reachShareRate-20000/490)<1e-9);
  assert.ok(Math.abs(result.saveRate-28000/1800)<1e-9);
  assert.equal(auditStats([{views:0,shares:0}]).shareRate,null);
  assert.equal(auditStats([{views:100}]).saveRate,null);
  const js=await auditRequest(new Request('https://fixture.test/auditoria/app.js')).text();
  assert.match(js,/function auditStats/);
  assert.doesNotThrow(()=>new Function(js));
});

test('creative treatments are stable across retries and tags only use subjects present in the actual source',()=>{
  const story={id:'abc',title:'Bitcoin cae mientras Nvidia presenta resultados'};
  assert.deepEqual(creativePolicy(story,'tiktok',{contrast:true}),creativePolicy(story,'tiktok',{contrast:true}));
  assert.deepEqual(topicTags(story.title),['#Bitcoin','#Nvidia']);
  assert.deepEqual(topicTags('Una compañía anuncia un nuevo producto'),[]);
  assert.doesNotMatch(topicTags('Bitcoin cae').join(' '),/fyp|viral|trending/i);
});
const lead={symbol:'BTC-USD',name:'Bitcoin',short:'BTC',price:65000,previous:68421,dayChange:-5,currency:'USD',at:NOW,source:'Fuente de prueba',series:[{at:NOW-60000,price:65100},{at:NOW,price:65000}]};
const snapshot={version:1,at:NOW,trigger:{symbol:'BTC-USD',period:'session',change:-5,direction:-1},rows:[lead]};
test('sparse and tied market data produce useful factual cards without inventing a curve or a strongest loser',()=>{
  const sparse=marketStoryV2({...snapshot,rows:[{...lead,series:[]}]});
  assert.equal(sparse.slides[1].visual,'market-price');
  assert.equal(sparse.slides[2].visual,'market-reference');
  const range=marketStoryV2(snapshot);
  assert.equal(range.slides[2].visual,'market-range');
  const tied=marketStoryV2({...snapshot,rows:[lead,{...lead,symbol:'ETH-USD',name:'Ethereum',short:'ETH'}]});
  assert.doesNotMatch(tied.slides[2].title,/cayó más/);
  const html=marketGrowthHtml(snapshot);
  assert.match(html,/65\.100/);
  assert.match(html,/935/);
});

test('new editorial portraits keep their official credits and percentage covers never show a currency as the percentage unit',()=>{
  const market={id:'x',title:'Bitcoin cae',source:'Prueba',source_url:'https://example.test/btc',published_at:NOW,editorial:{headline:'Bitcoin cae',marketSnapshot:snapshot}};
  const html=socialHtml(socialKitV2(market,{platform:'tiktok'}),'AAAA',0,'tiktok');
  assert.doesNotMatch(html,/<small>USD<\/small>/);
  const politics={...market,title:'Vance breaks with Trump in a political clash',editorial:{headline:'Vance rompe con Trump',socialSlides:[{title:'Vance rompe con Trump',body:'La fuente describe un enfrentamiento entre ambos.',visual:'politics'},{title:'Un frente político abierto',body:'La noticia detalla la disputa política descrita.',visual:'politics'},{title:'Los protagonistas del cruce',body:'Trump y Vance protagonizan este enfrentamiento.',visual:'politics'}]}};
  const picture=socialHtml(socialKitV2(politics,{platform:'instagram'}),'AAAA',0,'instagram');
  assert.match(picture,/Retrato oficial de Donald Trump/);
  assert.match(picture,/Retrato oficial de JD Vance/);
  assert.match(picture,/Montaje · Retratos oficiales/);
  assert.doesNotMatch(picture,/Ilustración con IA/);
});
