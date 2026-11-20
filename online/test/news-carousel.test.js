import test from 'node:test';
import assert from 'node:assert/strict';
import {socialKitV2,socialHtml} from '../src/social.js';
import {newsArtwork,NEWS_CAROUSEL} from '../src/news-layout.js';
import {sourceUnits,validateScript} from '../src/tiktok-content.js';
import {NOW,setup} from './helpers.js';
import {socialRevisions} from '../src/social-revision.js';
import {createChannels} from '../src/channels.js';

export const europe={id:'e'.repeat(32),title:'Europe ready for talks with Russia on ending war in Ukraine, leaders say',snippet:'European allies of Ukraine say they have a "united" negotiating position at a summit in Berlin.',source:'BBC World',source_url:'https://www.bbc.co.uk/news/articles/ckzd9nln5e6go',published_at:NOW,editorial:{
  headline:'Europa abre la puerta a negociar con Rusia',hook:'Europa se prepara para negociar con Rusia.',
  instagram:'Europa muestra un frente unido en Berlín y se declara lista para iniciar conversaciones de paz con Rusia sobre el conflicto en Ucrania. #Europa #Geopolitica #Ucrania',
  tweet:'🚨 Europa cambia de tono. Líderes europeos declaran estar listos para sentarse a negociar el fin de la guerra en Ucrania tras una cumbre clave en Berlín.',
  angle:'La energía subirá un 999% y los inversores deberían comprar.',reasoning:'Una noticia abre preguntas. Comprueba la fuente.'
}};

test('the reported Europe carousel contains two concrete cards, never tags, analysis, duplicate captions or padding',()=>{
  const kit=socialKitV2(europe,{platform:'tiktok'});
  assert.equal(kit.presentation,NEWS_CAROUSEL);assert.equal(kit.slides.length,2);assert.equal(kit.quality.ready,true);
  assert.match(kit.slides[0].body,/guerra en Ucrania/);assert.match(kit.slides[1].title,/frente unido en Berlín/);
  assert.doesNotMatch(JSON.stringify(kit.slides),/#|999|comprar|cambia de tono|Los detalles|El frente político/);
  assert.match(kit.tiktok.caption,/Berlín/);assert.match(kit.tiktok.caption,/Fuente:/);
  const [first,second]=[0,1].map(i=>newsArtwork(kit,i));
  assert.equal(first.scene,'europe-negotiation-map');assert.equal(second.scene,'berlin-summit');assert.notEqual(first.html,second.html);
  assert.doesNotMatch(first.html,/circle cx="325" cy="205"/);
});

test('metadata-only custom slides are removed and insufficient facts cannot be prepared',()=>{
  const story={...europe,editorial:{...europe.editorial,socialSlides:[
    {title:'Europa prepara conversaciones con Rusia',body:'',visual:'politics'},
    {title:'El frente político',body:'#Europa #Geopolitica #Ucrania',visual:'politics'},
    {title:'Los detalles del anuncio',body:'🚨 Europa cambia de tono.',visual:'news'}
  ]}};
  const kit=socialKitV2(story,{platform:'instagram'});
  assert.equal(kit.quality.ready,false);assert.equal(kit.slides.length,1);
});

test('new mobile frames use one logo, concrete next-card text and a current source attribution without changing frozen kits',()=>{
  for(const platform of ['tiktok','instagram']){
    const kit=socialKitV2(europe,{platform});
    for(let i=0;i<kit.slides.length;i++){
      const html=socialHtml(kit,'AAAA',i,platform);
      assert.equal((html.match(/data-wa-logo="1"/g)||[]).length,1);
      assert.ok(html.includes('data-brand-layout="news-v3"'));
      assert.ok(html.includes('BBC World'));assert.ok(!html.includes('El siguiente detalle'));
      assert.ok(!html.includes('Ilustración con IA'));
    }
  }
  const frozen={...socialKitV2(europe,{platform:'tiktok'}),presentation:'social-story-v2'};
  assert.ok(socialHtml(frozen,'AAAA',0,'tiktok').includes('height:1120px'));
});

test('source scripts allow two grounded slides and reject hashtags or filler in place of reported facts',()=>{
  const source=sourceUnits(europe),data={title:'Europa plantea negociar con Rusia',slides:[
    {title:'Europa, dispuesta a negociar con Rusia',body:'Los líderes hablan del fin de la guerra en Ucrania.',evidenceIds:['t0']},
    {title:'Una posición conjunta en Berlín',body:'Los aliados europeos de Ucrania afirman tener una posición unida.',evidenceIds:['e0']}
  ]};
  assert.ok(validateScript(data,source));
  assert.equal(validateScript({...data,slides:[data.slides[0],{...data.slides[1],body:'#Europa #Ucrania'}]},source),null);
});

test('a two-card correction can use original editorial drawings without credentials, generation or changing its parent',async()=>{
  const s=setup({MEDIA_ENABLED:'true'}),story={...europe,editorial:JSON.stringify(europe.editorial)};
  s.db.native.prepare("INSERT INTO stories(id,event_key,title,source,source_url,published_at,priority,snippet,state,editorial,created_at) VALUES(?,?,?,?,?,?,100,?,'approved',?,?)").run(story.id,story.id,story.title,story.source,story.source_url,NOW,story.snippet,story.editorial,NOW);
  const kit=socialKitV2(story,{platform:'tiktok'}),service=socialRevisions(s.env,{fetcher:s.fetcher,now:()=>NOW});
  const input={platform:'tiktok',editorialArtwork:true,title:kit.title,caption:kit.tiktok.caption,slides:kit.slides};
  const revision=await service.create(story.id,input);
  assert.equal(revision.files,2);assert.equal(revision.state,'draft');assert.equal(revision.kit.presentation,NEWS_CAROUSEL);
  const image=s.db.native.prepare('SELECT provider,manifest FROM channel_images WHERE story_id=?').get(revision.id);
  assert.equal(image.provider,'editorial-local');assert.equal(JSON.parse(image.manifest).art.mediaKind,'editorial-vector');
  assert.equal((await service.create(story.id,input)).created,false);
  assert.equal(s.db.native.prepare('SELECT editorial FROM stories WHERE id=?').get(story.id).editorial,story.editorial);
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM visual_budget').get().n,0);
});

test('an old unrendered draft is rebuilt on generation, while an album with existing assets keeps its frozen edition',async()=>{
  for(const rendered of [false,true]){
    const s=setup({MEDIA_ENABLED:'true'}),story={...europe,editorial:JSON.stringify(europe.editorial)};
    s.db.native.prepare("INSERT INTO stories(id,event_key,title,source,source_url,published_at,priority,snippet,state,editorial,created_at) VALUES(?,?,?,?,?,?,100,?,'approved',?,?)").run(story.id,story.id,story.title,story.source,story.source_url,NOW,story.snippet,story.editorial,NOW);
    const old={...socialKitV2(story,{platform:'tiktok'}),presentation:'social-story-v2'};
    old.slides.push({title:'El frente político',body:'#Europa #Ucrania',visual:'politics'});
    s.db.native.prepare('INSERT INTO runtime(key,value) VALUES(?,?)').run('channel_social_kit:'+story.id+':tiktok',JSON.stringify(old));
    s.db.native.prepare("INSERT INTO channel_jobs(story_id,platform,state,delivery,draft_message,created_at) VALUES(?,'tiktok','draft','sent','123',?)").run(story.id,NOW);
    if(rendered)s.db.native.prepare("INSERT INTO channel_assets(story_id,platform,position,filename,png) VALUES(?,'tiktok',0,'old.png',?)").run(story.id,new Uint8Array([1]));
    const channels=createChannels(s.env,{fetcher:s.fetcher,now:()=>NOW});
    assert.equal(await channels.action('gent',story.id,'123'),true);
    const kit=JSON.parse(s.db.native.prepare('SELECT value FROM runtime WHERE key=?').get('channel_social_kit:'+story.id+':tiktok').value);
    assert.equal(kit.presentation,rendered?'social-story-v2':NEWS_CAROUSEL);
    assert.equal(kit.slides.length,rendered?3:2);
  }
});
