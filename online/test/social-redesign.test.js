import test from 'node:test';
import assert from 'node:assert/strict';
import {socialKitV2,socialHtml,tiktokCaption} from '../src/social.js';
import {artDirection} from '../src/art-direction.js';
import {fastEditorial} from '../src/fast-editorial.js';
import {socialRevisions} from '../src/social-revision.js';
import {createChannels} from '../src/channels.js';
import {setup,NOW} from './helpers.js';

const id='c'.repeat(32),jpeg='/9j/'+'A'.repeat(160);
const row=(e={})=>({id,title:'Gnosis Pay breach exposes card numbers, home addresses and linked crypto wallets',source:'Fuente de prueba',source_url:'https://www.coindesk.com/news/fixture',published_at:NOW,snippet:'',editorial:JSON.stringify({...fastEditorial('Gnosis Pay: una filtración expone números de tarjetas, domicilios y carteras vinculadas','Fuente de prueba'),...e})});

test('a headline-only breach yields three factual illustrated cards and no source-only or filler panel',()=>{
  const story=row(),kit=socialKitV2(story);
  assert.equal(kit.quality.ready,true);assert.equal(kit.slides.length,3);
  assert.ok(kit.slides.every(s=>s.visual&&s.title));
  assert.doesNotMatch(kit.slides.map(s=>s.body).join(' '),/Fuente:|Una noticia abre|Comprueba el alcance|Revisa el titular/);
  assert.match(kit.tiktok.caption,/Fuente: https:/);
  for(let index=0;index<3;index++){
    const html=socialHtml(kit,jpeg,index,'tiktok');
    assert.match(html,/class="art-stage"/);assert.match(html,/scene-photo|scene-svg/);
    assert.doesNotMatch(html,/Fuente de prueba|Fuente:|CONTRASTA LA FUENTE/);
  }
  const brief=artDirection(story);assert.equal(brief.event,'breach');assert.doesNotMatch(brief.scene,/connector|plug|coin/i);
});

test('insufficient information and oversized custom copy are flagged instead of padded with invented panels',()=>{
  const insufficient={...row({headline:'Una empresa publica un anuncio',hook:'',tweet:'Una empresa publica un anuncio.',instagram:'Una empresa publica un anuncio.',angle:'Fuente: Fuente de prueba.',reasoning:'Revisa el titular y su contexto en la fuente original.'}),title:'A company publishes an announcement'};
  assert.equal(socialKitV2(insufficient).quality.ready,false);
  const custom=row({socialSlides:[0,1,2].map(i=>({title:'Dato '+i,body:'x'.repeat(270),visual:'data'}))});
  assert.equal(socialKitV2(custom).quality.ready,false);
  const old=tiktokCaption(row());
  assert.match(old,/Qué pasó y qué falta por confirmar/);
  const kit=socialKitV2(row());
  assert.equal(tiktokCaption({...row(),social_kit:JSON.stringify(kit)}),kit.tiktok.caption);
});

test('a corrected revision preserves original approval and upload history, sends one new group and waits for human approval',async()=>{
  const s=setup({MEDIA_ENABLED:'true'}),source=row();
  s.db.native.prepare("INSERT INTO stories(id,event_key,title,source,source_url,published_at,priority,snippet,state,editorial,created_at,approved_at) VALUES(?,?,?,?,?,?,90,'','approved',?,?,?)").run(id,id,source.title,source.source,source.source_url,NOW,source.editorial,NOW,NOW);
  s.db.native.prepare("INSERT INTO channel_jobs(story_id,platform,state,delivery,draft_message,created_at,approved_at) VALUES(?,'tiktok','approved','sent','1',?,?)").run(id,NOW,NOW);
  s.db.native.prepare("INSERT INTO tiktok_uploads(story_id,account_id,fingerprint,state,publish_id,created_at,updated_at,media_expires) VALUES(?,'owner','old','inbox','old-upload',?,?,?)").run(id,NOW,NOW,NOW+7200_000);
  let groups=0,generated=0;
  const fetcher=async(url,options)=>{
    if(url.endsWith('/sendMediaGroup')){groups++;return Response.json({ok:true,result:[0,1,2].map(i=>({message_id:90+i,document:{file_id:'new-'+i}}))});}
    if(url.endsWith('/editMessageText'))return Response.json({ok:true,result:{message_id:2}});
    return s.fetcher(url,options);
  };
  const service=socialRevisions(s.env,{fetcher,now:()=>NOW});
  const input={title:'Datos de prueba: nueva versión',caption:'Nueva revisión. Fuente: '+source.source_url,image:jpeg,slides:[0,1,2].map(i=>({title:'Hecho de prueba '+i,body:'Contenido específico de la muestra '+i+'.',label:'DATO',visual:['data','phishing','verification'][i]}))};
  const created=await service.create(id,input);assert.equal(created.created,true);assert.equal(created.files,3);
  const messages=s.state.messages.length;assert.equal((await service.create(id,input)).created,false);assert.equal(s.state.messages.length,messages);
  assert.equal((await service.ready(created.id)).status,409);
  const png=Buffer.alloc(25);png.set([137,80,78,71,13,10,26,10]);png.writeUInt32BE(1080,16);png.writeUInt32BE(1920,20);
  for(let position=0;position<3;position++){
    const result=await service.asset(created.id,position,new Request('https://fixture.example',{method:'POST',body:png}));assert.equal(result.stored,true);
    assert.equal((await service.asset(created.id,position,new Request('https://fixture.example',{method:'POST',body:png}))).stored,true);
  }
  assert.equal((await service.ready(created.id)).state,'rendering');
  const channels=createChannels(s.env,{fetcher,now:()=>NOW,generate:async()=>{generated++;throw Error('No generation permitted');}});
  assert.equal((await channels.step()).state,'review');assert.equal((await channels.step()).state,'idle');
  assert.equal(groups,1);assert.equal(generated,0);
  const revision=s.db.native.prepare("SELECT state,approved_at FROM channel_jobs WHERE story_id=?").get(created.id);
  assert.equal(revision.state,'review');assert.equal(revision.approved_at,null);
  assert.equal(s.db.native.prepare('SELECT state FROM channel_jobs WHERE story_id=?').get(id).state,'approved');
  assert.equal(s.db.native.prepare('SELECT publish_id FROM tiktok_uploads WHERE story_id=?').get(id).publish_id,'old-upload');
  assert.equal(s.db.native.prepare('SELECT editorial FROM stories WHERE id=?').get(id).editorial,source.editorial);
});
