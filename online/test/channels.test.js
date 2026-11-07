import test from 'node:test';
import assert from 'node:assert/strict';
import {createPipeline} from '../src/pipeline.js';
import {createChannels} from '../src/channels.js';
import {createImages} from '../src/image-providers.js';
import {minimalUpdate} from '../src/domain.js';
import {visualBrief} from '../src/publication.js';
import {shareXRequest} from '../src/share-x.js';
import {createStorage} from '../src/storage.js';
import {network,telegram} from '../src/io.js';
import {NOW,setup,callback,decodeUpload} from './helpers.js';
const JPEG='/9j/'+'A'.repeat(160),PNG=new Uint8Array([137,80,78,71,13,10,26,10,1,2,3]);
async function fixture(overrides={}) {
  const s=setup({MEDIA_ENABLED:'true',LEGACY_APPROVAL_ENABLED:'false',...overrides});
  const state={edits:[],keyboards:[],uploads:[],generated:0,rendered:[],instant:NOW,lose:false,loseDraft:false,failButtons:false};
  const fetcher=async(url,options={})=>{
    options=await decodeUpload(options);
    if(url.endsWith('/editMessageText')){state.edits.push(JSON.parse(options.body));return Response.json({ok:true,result:{message_id:100}});}
    if(url.endsWith('/editMessageReplyMarkup')){
      state.keyboards.push(JSON.parse(options.body));
      if(state.failButtons)throw Error('keyboard response lost');
      return Response.json({ok:true,result:{message_id:JSON.parse(options.body).message_id}});
    }
    if(url.endsWith('/sendMessage')&&state.loseDraft)throw Error('uncertain draft');
    if(url.endsWith('/sendDocument')){
      state.uploads.push(options.body);
      if(state.lose)throw Error('lost upload response');
      return Response.json({ok:true,result:{message_id:500,document:{file_id:'cover'}}});
    }
    if(url.endsWith('/sendMediaGroup')){
      state.uploads.push(options.body);
      if(state.lose)throw Error('lost album response');
      return Response.json({ok:true,result:JSON.parse(options.body.get('media')).map((_,i)=>({message_id:501+i,document:{file_id:'slide-'+i}}))});
    }
    return s.fetcher(url,options);
  };
  const p=createPipeline(s.env,{fetcher,now:()=>state.instant});
  await p.tick();
  const story=s.db.native.prepare('SELECT * FROM stories').get();
  const channels=createChannels(s.env,{fetcher,now:()=>state.instant,
    generate:async()=>{state.generated++;return JPEG;},render:async(html,dims)=>{state.rendered.push({html,dims});return PNG;}});
  const advance=async()=>{state.instant+=30_000;return channels.step();};
  return {...s,p,story,channels,channelState:state,advance,fetcher};
}
function job(s,p='x'){return s.db.native.prepare('SELECT * FROM channel_jobs WHERE platform=?').get(p);}

test('mobile news card is short, with three distinct platform buttons and no automatic attachments',async()=>{
  const s=await fixture(),m=s.state.messages[0];
  assert.ok(m.text.length<350);
  assert.doesNotMatch(m.text,/Relevancia|Lectura propuesta|Referencia|reasoning|6 láminas|Original:/);
  for(const code of ['x','i','t'])assert.ok(m.reply_markup.inline_keyboard.flat().some(b=>b.callback_data==='pick'+code+':'+s.story.id));
  await s.p.handleUpdate(callback(s.story,{data:'pickx:'+s.story.id}));
  assert.equal(s.state.messages.length,2);
  assert.match(s.state.messages[1].text,/280 caracteres ponderados/);
  assert.equal(s.channelState.uploads.length,0);
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM visual_jobs').get().n,0);
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM visual_budget').get().n,0);
});
test('platform callbacks are validated and bound to the owner and exact original/draft message',async()=>{
  const s=await fixture();
  for(const a of ['pickx','picki','pickt','genx','geni','gent','okx','oki','okt','cancelx','infoi'])assert.ok(minimalUpdate(callback(s.story,{data:a+':'+s.story.id})));
  assert.equal(minimalUpdate(callback(s.story,{data:'pickz:'+s.story.id})),null);
  await s.p.handleUpdate(callback(s.story,{data:'pickx:'+s.story.id,from:{id:999}}));
  assert.equal(job(s),undefined);
  assert.equal(await s.channels.pick(s.story.id,'x',999),false);
  await s.channels.pick(s.story.id,'x',s.story.message_id);
  assert.equal(await s.channels.action('genx',s.story.id,999),false);
  assert.equal(await s.channels.action('okx',s.story.id,job(s).draft_message),true);
  assert.equal(job(s).state,'draft');
});
test('X prepares just one PNG and keeps full tweet with weighted count; final approval does not resend it',async()=>{
  const s=await fixture();
  await s.channels.pick(s.story.id,'x',s.story.message_id);
  await s.channels.action('genx',s.story.id,job(s).draft_message);
  for(let i=0;i<4;i++)await s.advance();
  assert.equal(s.channelState.generated,1);
  assert.equal(s.channelState.rendered.length,1);
  assert.equal(s.channelState.uploads.length,1);
  assert.equal(job(s).state,'review');
  assert.match(s.channelState.uploads[0].get('caption'),/Un anuncio que merece contexto\./);
  const message=job(s).draft_message;
  await s.channels.action('okx',s.story.id,message);
  await s.channels.action('okx',s.story.id,message);
  assert.equal(job(s).state,'approved');
  assert.equal(s.channelState.uploads.length,1);
  assert.equal(s.state.messages.length,2);
  assert.ok(s.channelState.edits.at(-1).reply_markup.inline_keyboard[0][0].url.startsWith('https://twitter.com/intent/'));
  assert.equal((await s.advance()).state,'idle');
});

test('X approval and sharing buttons appear below the PNG and accept only the owner and that exact image',async()=>{
  const origin='https://fixture.workers.dev';
  const s=await fixture({WORKER_ORIGIN:origin,ADMIN_TOKEN:'test-only'});
  await s.channels.pick(s.story.id,'x',s.story.message_id);
  await s.channels.action('genx',s.story.id,job(s).draft_message);
  for(let i=0;i<4;i++)await s.advance();
  const asset=s.db.native.prepare("SELECT * FROM channel_assets WHERE platform='x' AND position=0").get();
  const controls=s.channelState.keyboards.at(-1);
  assert.equal(String(controls.message_id),asset.message_id);
  assert.equal(controls.reply_markup.inline_keyboard[0][0].text,'Aprobar y compartir en X');
  assert.equal(controls.reply_markup.inline_keyboard.flat().some(button=>button.url),false);
  const approval=callback(s.story,{data:'okx:'+s.story.id,message:{message_id:Number(asset.message_id),chat:{id:123}}});
  await s.p.handleUpdate({...approval,callback_query:{...approval.callback_query,from:{id:999}}});
  assert.equal(job(s).state,'review');
  assert.equal(await s.channels.action('okx',s.story.id,Number(asset.message_id)+1),false);
  await s.p.handleUpdate(approval);
  assert.equal(job(s).state,'approved');
  assert.equal(job(s).error,null);
  const image=s.channelState.keyboards.at(-1);
  const text=s.channelState.edits.at(-1);
  assert.equal(image.reply_markup.inline_keyboard[0][0].text,'Compartir en Twitter/X');
  assert.match(image.reply_markup.inline_keyboard[0][0].url,/^https:\/\/fixture\.workers\.dev\/share\/x#/);
  assert.deepEqual(image.reply_markup,text.reply_markup);
  assert.equal(s.channelState.uploads.length,1);
});

test('an interrupted X keyboard edit retries only the existing controls and stops after three failures',async()=>{
  const s=await fixture({WORKER_ORIGIN:'https://fixture.workers.dev',ADMIN_TOKEN:'test-only'});
  await s.channels.pick(s.story.id,'x',s.story.message_id);
  await s.channels.action('genx',s.story.id,job(s).draft_message);
  for(let i=0;i<4;i++)await s.advance();
  s.channelState.failButtons=true;
  await s.channels.action('okx',s.story.id,job(s).draft_message);
  assert.equal(job(s).state,'approved');
  assert.equal(job(s).error,'share-edit-pending:1');
  for(let i=0;i<2;i++){s.channelState.instant+=60_000;await s.channels.step();}
  assert.equal(job(s).error,'share-edit-failed');
  s.channelState.instant+=60_000;
  assert.equal((await s.channels.step()).state,'idle');
  assert.equal(s.channelState.uploads.length,1);
  assert.equal(s.channelState.generated,1);
  s.channelState.failButtons=false;
  assert.equal(await s.channels.refreshX(s.story.id),true);
  assert.equal(job(s).error,null);
});

test('Telegram unchanged text and keyboard edits are idempotent while other HTTP 400 errors remain failures',async()=>{
  const env={TELEGRAM_BOT_TOKEN:'test',TELEGRAM_CHAT_ID:'123'};
  const bot=telegram(env,network(async()=>Response.json({ok:false,error_code:400,description:'Bad Request: message is not modified'},{status:400})));
  assert.deepEqual(await bot.edit(5,'Copy',{}),{message_id:5});
  assert.deepEqual(await bot.buttons(5,{inline_keyboard:[]}),{message_id:5});
  const broken=telegram(env,network(async()=>Response.json({ok:false,error_code:400,description:'Bad Request: message to edit not found'},{status:400})));
  await assert.rejects(()=>broken.buttons(5,{inline_keyboard:[]}));
});
test('Instagram and TikTok each send one five-document group, reuse one image and use their own dimensions',async()=>{
  const s=await fixture({WORKER_ORIGIN:'https://fixture.workers.dev',ADMIN_TOKEN:'test-only'});
  for(const [code,platform,height] of [['i','instagram',1350],['t','tiktok',1920]]) {
    await s.channels.pick(s.story.id,code,s.story.message_id);
    await s.channels.action('gen'+code,s.story.id,job(s,platform).draft_message);
    for(let n=0;n<7;n++)await s.advance();
    assert.equal(job(s,platform).state,'review');
    const upload=s.channelState.uploads.at(-1);
    assert.equal(JSON.parse(upload.get('media')).length,5);
    assert.equal(upload.get('disable_notification'),'true');
    assert.deepEqual(s.channelState.rendered.at(-1).dims,{width:1080,height});
    assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM channel_assets WHERE platform=? AND png IS NOT NULL').get(platform).n,0);
  }
  assert.equal(s.channelState.generated,1);
  assert.equal(s.channelState.uploads.length,2);
  assert.equal(s.state.messages.length,3);
  await s.channels.action('okt',s.story.id,job(s,'tiktok').draft_message);
  const link=s.channelState.edits.at(-1).reply_markup.inline_keyboard[0][0];
  assert.equal(link.text,'Preparar post en TikTok');
  assert.match(link.url,/^https:\/\/fixture\.workers\.dev\/share\/tiktok#/);
  assert.equal(s.channelState.uploads.length,2);
});
test('the chosen X variant is used for review, caption and the signed mobile share link',async()=>{
  const origin='https://fixture.workers.dev';
  const s=await fixture({WORKER_ORIGIN:origin,ADMIN_TOKEN:'test-only'});
  const editorial=JSON.parse(s.story.editorial);
  editorial.headline='El Nasdaq cae un 3% tras los resultados de Nvidia';
  editorial.tweet='La caída marca la sesión de las tecnológicas.';
  s.db.native.prepare('UPDATE stories SET editorial=? WHERE id=?').run(JSON.stringify(editorial),s.story.id);
  await s.p.handleUpdate(callback(s.story,{data:'pickx:'+s.story.id}));
  const draft=s.state.messages.at(-1);
  assert.match(draft.text,/Nvidia/);
  assert.ok(draft.reply_markup.inline_keyboard.flat().some(button=>button.callback_data==='genh:'+s.story.id));
  const message=Number(job(s).draft_message);
  const select=callback(s.story,{data:'genh:'+s.story.id,message:{message_id:message,chat:{id:123}}});
  assert.ok(minimalUpdate(select));
  await s.p.handleUpdate(select);
  await s.channels.action('genq',s.story.id,message);
  for(let i=0;i<4;i++) await s.advance();
  assert.match(s.channelState.uploads[0].get('caption'),/#Nasdaq/);
  assert.match(s.channelState.edits.at(-1).text,/#Nasdaq/);
  await s.channels.action('okx',s.story.id,message);
  const link=s.channelState.edits.at(-1).reply_markup.inline_keyboard[0][0].url;
  const [id,expires,kind,sig]=new URL(link).hash.slice(1).split('.');
  const data=await shareXRequest(new Request(origin+'/share/x/data',{method:'POST',
    body:JSON.stringify({id,expires:Number(expires),kind,sig})}),s.env,{now:()=>s.channelState.instant});
  assert.equal(data.status,200);
  const {text}=await data.json();
  assert.match(text,/El Nasdaq cae un 3% tras los resultados de Nvidia/);
  assert.match(text,/#Nasdaq$/);
  assert.ok(s.channelState.edits.at(-1).text.includes(text));
});
test('repeated platform selection and generation cannot duplicate drafts or images',async()=>{
  const s=await fixture();
  await Promise.all([s.channels.pick(s.story.id,'x',s.story.message_id),s.channels.pick(s.story.id,'x',s.story.message_id)]);
  const message=job(s).draft_message;
  await s.channels.action('genx',s.story.id,message);
  await s.channels.action('genx',s.story.id,message);
  assert.equal(s.state.messages.length,2);
  for(let n=0;n<6;n++)await s.advance();
  assert.equal(s.channelState.uploads.length,1);
});
test('ambiguous single and album uploads retain buffers and never automatically resend',async()=>{
  for(const code of ['x','i']){
    const s=await fixture(),platform=code==='x'?'x':'instagram';
    await s.channels.pick(s.story.id,code,s.story.message_id);
    await s.channels.action('gen'+code,s.story.id,job(s,platform).draft_message);
    s.channelState.lose=true;
    for(let n=0;n<8;n++)await s.advance();
    assert.equal(job(s,platform).state,'blocked');
    assert.equal(job(s,platform).delivery,'uncertain');
    assert.equal(s.channelState.uploads.length,1);
    assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM channel_assets WHERE png IS NOT NULL').get().n,code==='x'?1:5);
  }
});
test('lost draft response and retired generic approve buttons do not trigger the old bundle',async()=>{
  const s=await fixture();
  await s.p.handleUpdate(callback(s.story));
  assert.equal(s.channelState.edits.length,1);
  assert.equal(s.channelState.uploads.length,0);
  s.channelState.loseDraft=true;
  await s.channels.pick(s.story.id,'x',s.story.message_id);
  await s.channels.pick(s.story.id,'x',s.story.message_id);
  assert.equal(job(s).error,'draft-uncertain');
  assert.equal((await s.advance()).state,'idle');
});
test('cancel and pause do not generate or send unrelated formats',async()=>{
  const s=await fixture();
  await s.channels.pick(s.story.id,'x',s.story.message_id);
  await s.channels.action('cancelx',s.story.id,job(s).draft_message);
  await s.channels.action('genx',s.story.id,job(s).draft_message);
  assert.equal((await s.advance()).state,'idle');
  s.env.MEDIA_ENABLED='false';
  assert.equal((await s.advance()).state,'disabled');
  assert.equal(s.channelState.generated,0);
});
test('new channel retention waits for all requested formats and protects ambiguous deliveries',async()=>{
  const s=await fixture();
  await s.channels.pick(s.story.id,'x',s.story.message_id);
  await s.channels.action('genx',s.story.id,job(s).draft_message);
  for(let n=0;n<3;n++)await s.advance();
  const storage=createStorage(s.env,{now:()=>s.channelState.instant,fetcher:s.fetcher});
  await storage.cleanup();
  s.channelState.instant+=31*86_400_000;
  await s.channels.pick(s.story.id,'i',s.story.message_id);
  assert.equal((await storage.cleanup()).releasedChannelBackgrounds,0);
  s.db.native.prepare("UPDATE channel_jobs SET state='review',delivery='uncertain' WHERE platform='instagram'").run();
  assert.equal((await storage.cleanup()).releasedChannelBackgrounds,0);
  s.db.native.prepare("DELETE FROM channel_jobs WHERE platform='instagram'").run();
  assert.equal((await storage.cleanup()).releasedChannelBackgrounds,1);
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM channel_assets WHERE file_id IS NOT NULL').get().n,1);
});

async function imageFixture(overrides={}) {
  const s=await fixture({DAILY_VISUAL_LIMIT:'0',HORDE_ENABLED:'true',DAILY_HORDE_LIMIT:'2',...overrides});
  const story={...s.story,editorial:JSON.parse(s.story.editorial)};
  const state={calls:[],done:false,lose:false,instant:NOW};
  const fetcher=async(url,options)=>{
    state.calls.push({url,options});
    if(url.endsWith('/generate/async')){
      if(state.lose)throw Error('lost response');
      return Response.json({id:'12345678-1234-1234-1234-123456789abc'});
    }
    if(url.includes('/generate/check/'))return Response.json({done:state.done,is_possible:true});
    if(url.includes('/generate/status/'))return Response.json({generations:[{img:JPEG,censored:false}]});
    throw Error('unexpected provider');
  };
  const images=createImages(s.env,{fetcher,now:()=>state.instant,generate:async()=>{throw Error('must not use cloudflare');}});
  return {...s,story,providerState:state,images,brief:visualBrief(story)};
}
test('Cloudflare cap opens the free Horde queue once, polls across ticks and persists the same result',async()=>{
  const s=await imageFixture();
  assert.equal((await s.images.next(s.story.id,s.brief)).state,'pending');
  assert.equal((await s.images.next(s.story.id,s.brief)).state,'pending');
  s.providerState.done=true;
  assert.equal((await s.images.next(s.story.id,s.brief)).provider,'horde');
  assert.equal((await s.images.next(s.story.id,s.brief)).image,JPEG);
  assert.equal(s.providerState.calls.filter(c=>c.url.endsWith('/generate/async')).length,1);
  const body=JSON.parse(s.providerState.calls[0].options.body);
  assert.equal(body.params.n,1);assert.equal(body.nsfw,false);assert.equal(body.r2,false);
});
test('Cloudflare is always first; only quota failures open fallback, not invalid images or outages',async()=>{
  for(const failure of [null,Object.assign(Error('429 quota'),{status:429}),Error('service unavailable')]) {
    const s=await imageFixture({DAILY_VISUAL_LIMIT:'3'});
    let calls=0;
    const image=createImages(s.env,{fetcher:async()=>{throw Error('fallback only on next tick');},now:()=>NOW,generate:async()=>{calls++;if(failure)throw failure;return JPEG;}});
    assert.equal((await image.next(s.story.id,s.brief)).state,failure?(failure.status===429?'pending':'blocked'):'ready');
    assert.equal(calls,1);
  }
});
test('lost Horde submission is never repeated; attempts include failures',async()=>{
  const s=await imageFixture();s.providerState.lose=true;
  assert.equal((await s.images.next(s.story.id,s.brief)).state,'blocked');
  assert.equal((await s.images.next(s.story.id,s.brief)).state,'blocked');
  assert.equal(s.providerState.calls.length,1);
  assert.equal(s.db.native.prepare('SELECT used FROM image_provider_budget').get().used,1);
});
test('Horde is opt-in, has its own hard daily cap, and times out instead of making another request',async()=>{
  for(const overrides of [{HORDE_ENABLED:'false'},{DAILY_HORDE_LIMIT:'0'}]){
    const s=await imageFixture(overrides);
    assert.equal((await s.images.next(s.story.id,s.brief)).state,'daily-limit');
    assert.equal(s.providerState.calls.length,0);
  }
  const s=await imageFixture();
  await s.images.next(s.story.id,s.brief);
  s.providerState.instant+=46*60_000;
  assert.equal((await s.images.next(s.story.id,s.brief)).state,'blocked');
  assert.equal(s.providerState.calls.filter(c=>c.url.endsWith('/generate/async')).length,1);
});

test('crypto stories cannot consume Horde quota or submit prompts to that provider',async()=>{
  const s=await imageFixture();
  const brief={...s.brief,sourceTitle:'Binance resumes crypto withdrawals'};
  const result=await s.images.next(s.story.id,brief);
  assert.equal(result.state,'daily-limit');
  assert.equal(result.reason,'horde-subject-restricted');
  assert.equal(s.providerState.calls.length,0);
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM image_provider_budget').get().n,0);
});
test('the Trump/Vance design uses verified portraits with zero AI attempts, even at the cap',async()=>{
  const s=await imageFixture();
  const brief=visualBrief({...s.story,title:'Vance criticizes Trump over Iran war'});
  const result=await s.images.next(s.story.id,brief);
  assert.equal(result.state,'ready');
  assert.equal(result.provider,'official-portraits');
  assert.equal(s.providerState.calls.length,0);
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM visual_budget').get().n,0);
  assert.equal(result.brief.mediaCredits.author,'Daniel Torok');
});
test('failed review edit retries only the existing message, never the confirmed image upload',async()=>{
  const s=await fixture();let fail=true;
  const channels=createChannels(s.env,{now:()=>s.channelState.instant,generate:async()=>JPEG,render:async()=>PNG,
    fetcher:async(url,options)=>{
      if(url.endsWith('/editMessageText')&&fail)throw Error('edit response lost');
      return s.fetcher(url,options);
    }});
  await channels.pick(s.story.id,'x',s.story.message_id);
  await channels.action('genx',s.story.id,job(s).draft_message);
  for(let n=0;n<3;n++){s.channelState.instant+=30_000;await channels.step();}
  assert.equal(job(s).state,'review');
  assert.equal(job(s).error,'review-edit-pending');
  assert.equal(s.channelState.uploads.length,1);
  fail=false;
  assert.equal((await channels.step()).state,'review');
  assert.equal(job(s).error,null);
  assert.equal(s.channelState.uploads.length,1);
});
