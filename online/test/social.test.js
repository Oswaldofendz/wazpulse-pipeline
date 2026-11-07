import test from 'node:test';
import assert from 'node:assert/strict';
import {socialKit,socialText,socialHtml,socialDimensions} from '../src/social.js';
import {visualBrief} from '../src/publication.js';
import {createMedia} from '../src/media.js';
import {createPipeline} from '../src/pipeline.js';
import worker from '../src/worker.js';
import {setup,NOW,callback,decodeUpload} from './helpers.js';
import {network,telegram} from '../src/io.js';
const JPEG='/9j/'+'A'.repeat(160),PNG=new Uint8Array([137,80,78,71,13,10,26,10,1]);

async function fixture(){
 const s=setup({MEDIA_ENABLED:'true',DAILY_VISUAL_LIMIT:'0'});
 const pipeline=createPipeline(s.env,{fetcher:s.fetcher,now:()=>NOW});
 await pipeline.tick();const story=s.db.native.prepare('SELECT * FROM stories').get();
 await pipeline.handleUpdate(callback(story));
 const manifest=JSON.stringify(visualBrief(story));
 s.db.native.prepare("UPDATE visual_jobs SET image=?,manifest=?,state='approved' WHERE story_id=?").run(JPEG,manifest,story.id);
 const state={generated:0,renders:[],images:[],lose:false,clock:NOW};
 const fetcher=async(url,options)=>{
  options=await decodeUpload(options);
  if(url.endsWith('/sendDocument')&&typeof options.body.get('document')!=='string'&&options.body.get('document').type==='image/png'){
   state.images.push(options.body.get('document'));
   if(state.lose)throw new Error('Lost response');
   return Response.json({ok:true,result:{message_id:1234,document:{file_id:'social-file'}}});
  }
  return s.fetcher(url,options);
 };
 const media=createMedia(s.env,{fetcher,now:()=>{state.clock+=30000;return state.clock},
  generate:async()=>{state.generated++;throw new Error('No new AI permitted')},render:async(html,dimensions)=>{state.renders.push({html,dimensions});return PNG}});
 return {...s,story,media,socialState:state,manifest};
}

test('social kit keeps traceable source, five platform-specific slides and a clearly unrendered script',async()=>{
 const s=await fixture(),kit=await s.media.social(s.story.id);
 assert.equal(kit.storyId,s.story.id);assert.equal(kit.slides.length,5);
 assert.equal(kit.source.url,s.story.source_url);assert.ok(kit.requiresReview);
 assert.deepEqual([kit.instagram.width,kit.instagram.height],[1080,1350]);
 assert.deepEqual([kit.tiktok.width,kit.tiktok.height],[1080,1920]);
 assert.equal(kit.video.state,'script-only');assert.equal(kit.video.beats.length,4);
 assert.ok(socialText(kit).includes('no incluye un MP4'));
 assert.equal(s.socialState.generated,0);
});

test('Telegram uploads use a bounded fixed-length body with the correct multipart boundary',async()=>{
 let checked=false;
 const net=network(async(url,options)=>{
  assert.ok(options.body instanceof ArrayBuffer);
  const type=new Headers(options.headers).get('Content-Type');assert.ok(type.includes('boundary='));
  const form=await new Response(options.body,{headers:options.headers}).formData();
  assert.equal(form.get('caption'),'Prueba con acentos y 🧪');
  assert.deepEqual(new Uint8Array(await form.get('document').arrayBuffer()),PNG);
  checked=true;return Response.json({ok:true,result:{document:{file_id:'test'}}});
 });
 await telegram({TELEGRAM_CHAT_ID:'123',TELEGRAM_BOT_TOKEN:'test'},net).imageDocument(PNG,'test.png','Prueba con acentos y 🧪');
 assert.ok(checked);
});

test('all social previews reuse stored imagery at zero AI quota and preserve earlier approvals',async()=>{
 const s=await fixture();
 for(const platform of ['instagram','tiktok'])for(let n=1;n<=5;n++)assert.equal((await s.media.preview(s.story.id,n,{reuseOnly:true,platform})).state,'preview');
 assert.equal(s.socialState.renders.length,10);assert.equal(s.socialState.generated,0);
 assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM visual_budget').get().n,0);
 assert.equal(s.db.native.prepare('SELECT manifest FROM visual_jobs').get().manifest,s.manifest);
 assert.equal(s.db.native.prepare('SELECT state FROM visual_jobs').get().state,'approved');
});

test('reuse-only previews fail rather than create a new paid or over-budget image',async()=>{
 const s=await fixture();s.db.native.prepare('UPDATE visual_jobs SET image=NULL').run();
 assert.equal((await s.media.preview(s.story.id,0,{reuseOnly:true})).state,'image-unavailable');
 assert.equal(s.socialState.generated,0);assert.equal(s.socialState.renders.length,0);
});

test('test image and its text share a reference, and repeated delivery cannot duplicate the image',async()=>{
 const s=await fixture();
 assert.equal((await s.media.deliverPreview(s.story.id)).state,'delivered');
 assert.equal((await s.media.deliverPreview(s.story.id)).state,'already-delivered');
 assert.equal(s.socialState.images.length,1);
 assert.ok(s.state.messages.at(-1).text.includes(s.story.id.slice(0,8)));
 assert.ok(s.state.messages.at(-1).text.includes('no es una alerta nueva'));
 assert.equal(s.socialState.generated,0);
});

test('uncertain social delivery records the ambiguity and never automatically resends',async()=>{
 const s=await fixture();s.socialState.lose=true;
 assert.equal((await s.media.deliverPreview(s.story.id,'instagram',0)).state,'delivery-needs-review');
 s.socialState.lose=false;
 assert.equal((await s.media.deliverPreview(s.story.id,'instagram',0)).state,'delivery-needs-review');
 assert.equal(s.socialState.images.length,1);
});

test('social assets escape unsafe text, reject invalid selections and cannot be read anonymously',async()=>{
 const s=await fixture(),kit=socialKit(s.story);kit.slides[0].title='<script>bad()</script>';
 assert.ok(socialHtml(kit,JPEG,0,'instagram').includes('&lt;script&gt;'));
 assert.throws(()=>socialDimensions('other'));assert.throws(()=>socialHtml(kit,JPEG,7,'tiktok'));
 assert.equal((await s.media.deliverPreview(s.story.id,'other',0)).state,'invalid-selection');
 for(const path of ['social-pack','test-send','social-send','social-deliver/tiktok/1','social-preview/instagram/1'])
  assert.equal((await worker.fetch(new Request('https://test/admin/media/'+s.story.id+'/'+path),s.env,{})).status,401);
 s.db.native.prepare("UPDATE stories SET state='pending'").run();
 assert.equal(await s.media.social(s.story.id),null);
 assert.equal(await s.media.sendSocial(s.story.id),false);
});
