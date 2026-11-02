import test from 'node:test';
import assert from 'node:assert/strict';
import {createMedia} from '../src/media.js';
import {createPipeline} from '../src/pipeline.js';
import {xPublication, visualBrief, visualHtml, visualDimensions} from '../src/publication.js';
import {minimalUpdate} from '../src/domain.js';
import worker from '../src/worker.js';
import {NOW, setup, callback} from './helpers.js';

const JPEG = '/9j/' + 'A'.repeat(160);
const PNG = new Uint8Array([137,80,78,71,13,10,26,10,1,2,3]);
async function fixture() {
  const s = setup({MEDIA_ENABLED: 'true', DAILY_VISUAL_LIMIT: '3'});
  const pipeline = createPipeline(s.env, {fetcher: s.fetcher, now: () => NOW});
  await pipeline.tick();
  const story = s.db.native.prepare('SELECT * FROM stories').get();
  await pipeline.handleUpdate(callback(story));
  const state = {generated: 0, rendered: [], documents: [], loseImage: false, instant: NOW};
  const fetcher = async (url, options) => {
    if (url.endsWith('/sendDocument') && typeof options.body.get('document') !== 'string' && options.body.get('document').type === 'image/png') {
      state.documents.push(options.body.get('document'));
      if (state.loseImage) throw new Error('response lost');
      return Response.json({ok: true, result: {message_id: 200 + state.documents.length, document: {file_id: 'file-' + state.documents.length}}});
    }
    return s.fetcher(url, options);
  };
  const media = createMedia(s.env, {fetcher, now: () => {state.instant += 30_000; return state.instant;},
    generate: async brief => { state.generated++; assert.equal(brief.model, '@cf/black-forest-labs/flux-1-schnell'); return JPEG; },
    render: async (html, dimensions) => {state.rendered.push({html, dimensions}); return PNG;}});
  return {...s, pipeline, story: {...story, editorial: story.editorial}, media, mediaState: state};
}

test('X uses complete approved headlines instead of cropping emoji, CJK or long copy', async () => {
  const s = await fixture();
  const row = s.db.native.prepare('SELECT * FROM stories').get();
  for (const tweet of ['🚀'.repeat(400), '東京'.repeat(300), 'Noticia '.repeat(100), '👨‍👩‍👧‍👦 '.repeat(100)]) {
    const e = JSON.parse(row.editorial); e.tweet = tweet;
    const result = xPublication({...row, editorial: e});
    assert.ok(result.weightedLength <= 280);
    assert.equal(result.text.includes(row.source_url), false);
    assert.equal(result.sourceUrl, row.source_url);
    assert.ok(result.text.includes(e.headline));
    assert.ok(result.text.endsWith('Información, no asesoramiento.'));
    if (result.composeUrl) assert.equal(new URL(result.composeUrl).searchParams.get('text'), result.text);
    else assert.ok(('https://twitter.com/intent/tweet?text=' + encodeURIComponent(result.text)).length > 2000);
    assert.equal(result.text.includes('\ufffd'), false);
  }
});

test('visual brief includes actual story data and publication dimensions escape unsafe text', async () => {
  const s = await fixture(), row = s.db.native.prepare('SELECT * FROM stories').get();
  const brief = visualBrief(row);
  assert.ok(brief.prompt.includes(row.title));
  assert.ok(brief.prompt.length <= 2048);
  assert.equal(brief.slides.length, 5);
  brief.title = '<img src="https://attacker.test/" onerror="alert(1)">';
  const html = visualHtml(brief, JPEG);
  assert.ok(html.includes('&lt;img'));
  assert.ok(html.includes('ILUSTRACIÓN CON IA'));
  assert.ok(html.includes('width:1080px;height:1350px'));
  assert.ok(visualHtml(brief, JPEG, 0).includes('width:1080px;height:1350px'));
  assert.throws(() => visualHtml(brief, '" onerror="alert(1)'));
});

test('no images generated before editorial approval, or when visual pipeline is disabled', async () => {
  const s = setup({MEDIA_ENABLED: 'true'});
  const p = createPipeline(s.env, {fetcher: s.fetcher, now: () => NOW});
  await p.tick();
  const story = s.db.native.prepare('SELECT * FROM stories').get();
  const media = createMedia(s.env, {fetcher: s.fetcher, now: () => NOW});
  assert.equal(await media.enqueue(story.id), false);
  assert.equal((await media.step()).state, 'idle');
  s.env.MEDIA_ENABLED = 'false';
  assert.equal((await media.step()).state, 'disabled');
});

test('approved story creates a durable job only once; one illustration serves all six originals', async () => {
  const s = await fixture();
  assert.equal(await s.media.enqueue(s.story.id), false);
  for (let n = 0; n < 8; n++) await s.media.step();
  assert.equal(s.mediaState.generated, 1);
  assert.equal(s.mediaState.rendered.length, 6);
  assert.equal(s.mediaState.documents.length, 6);
  assert.deepEqual(s.mediaState.rendered[0].dimensions, {width: 1080, height: 1350});
  assert.deepEqual(s.mediaState.rendered[1].dimensions, {width: 1080, height: 1350});
  assert.equal((await s.media.status(s.story.id)).state, 'review');
  assert.equal((await s.media.step()).state, 'idle');
  const counts = s.db.native.prepare('SELECT COUNT(*) AS n FROM visual_assets WHERE png IS NOT NULL').get();
  assert.equal(counts.n, 0);
  assert.equal(s.state.calls.some(x => x.url.includes('api.x.com')), false);
});

test('crash between rendering and upload resumes stored original without another image generation', async () => {
  const s = await fixture();
  await s.media.step();
  s.db.native.prepare('INSERT INTO visual_assets(story_id,position,filename,png) VALUES(?,?,?,?)').run(s.story.id, 0, 'cover.png', PNG);
  assert.equal((await s.media.step()).state, 'asset-delivered');
  assert.equal(s.mediaState.rendered.length, 0);
  assert.equal(s.mediaState.generated, 1);
});

test('ambiguous image delivery stops automatic resend and preserves the upload buffer', async () => {
  const s = await fixture();
  await s.media.step();
  s.mediaState.loseImage = true;
  assert.equal((await s.media.step()).state, 'delivery-needs-review');
  assert.equal((await s.media.step()).state, 'idle');
  assert.equal(s.mediaState.documents.length, 1);
  const row = s.db.native.prepare('SELECT * FROM visual_assets').get();
  assert.equal(row.delivery, 'uncertain');
  assert.ok(row.png);
  s.mediaState.loseImage = false;
  assert.ok(await s.media.resend(s.story.id));
  assert.equal(s.db.native.prepare('SELECT delivery FROM visual_assets').get().delivery, 'sent');
  assert.equal((await s.media.status(s.story.id)).state, 'rendering');
});

test('final approval binds the exact review message; cannot approve missing, rejected or another message', async () => {
  const s = await fixture();
  assert.equal(await s.media.review(s.story.id, 1, true), false);
  for (let n = 0; n < 8; n++) await s.media.step();
  const job = s.db.native.prepare('SELECT * FROM visual_jobs').get();
  assert.equal(await s.media.review(s.story.id, Number(job.review_message) + 1, true), false);
  assert.ok(await s.media.review(s.story.id, Number(job.review_message), true));
  assert.equal(await s.media.review(s.story.id, Number(job.review_message), true), false);
  assert.ok(s.state.messages.at(-1).reply_markup.inline_keyboard[0][0].url.startsWith('https://twitter.com/intent/tweet?'));
});

test('visual callbacks require owner and cannot trigger a new editorial approval', async () => {
  const s = await fixture();
  const q = callback(s.story, {data: 'visualok:' + s.story.id});
  assert.ok(minimalUpdate(q));
  const before = s.state.documents.length;
  await s.pipeline.handleUpdate(q);
  assert.equal(s.state.documents.length, before);
  assert.equal((await s.media.status(s.story.id)).state, 'queued');
});

test('visual daily cap blocks generation and counts unsuccessful attempts', async () => {
  const s = await fixture();
  s.env.DAILY_VISUAL_LIMIT = '0';
  assert.equal((await s.media.step()).state, 'daily-limit');
  assert.equal(s.mediaState.generated, 0);
  s.env.DAILY_VISUAL_LIMIT = '1';
  const media = createMedia(s.env, {fetcher: s.fetcher, now: () => NOW,
    generate: async () => {throw new Error('quota');}, render: async () => PNG});
  assert.equal((await media.step()).state, 'blocked');
  assert.equal(s.db.native.prepare('SELECT used FROM visual_budget').get().used, 1);
  assert.equal(s.db.native.prepare('SELECT state FROM visual_jobs').get().state, 'blocked');
});

test('concurrent visual workers share a lock and cannot duplicate AI requests', async () => {
  const s = await fixture();
  const results = await Promise.all([s.media.step(), s.media.step()]);
  assert.ok(results.some(x => x.state === 'busy'));
  assert.equal(s.mediaState.generated, 1);
});

test('private images, manifests and visual commands require admin authentication', async () => {
  const s = await fixture();
  for (const path of ['/admin/media-status', '/admin/media-step', '/admin/media/' + s.story.id + '/0', '/admin/media/' + s.story.id + '/manifest', '/admin/media/' + s.story.id + '/x-package']) {
    assert.equal((await worker.fetch(new Request('https://test' + path), s.env, {})).status, 401);
  }
});

test('browser rate failures defer the same artwork without another AI call', async () => {
  const s = await fixture();
  await s.media.step();
  const media = createMedia(s.env, {fetcher: s.fetcher, now: () => NOW + 120_000,
    render: async () => {throw new Error('429 Too many requests');}});
  assert.equal((await media.step()).state, 'render-deferred');
  const row = s.db.native.prepare('SELECT * FROM visual_jobs').get();
  assert.equal(row.state, 'rendering');
  assert.ok(row.lease_until > NOW + 120_000);
  assert.equal((await media.step()).state, 'idle');
  assert.equal(s.mediaState.generated, 1);
});

test('Free browser cooldown prevents two launches within twenty-five seconds', async () => {
  const s = await fixture();
  await s.media.step();
  await s.media.step();
  const media = createMedia(s.env, {fetcher: s.fetcher, now: () => s.mediaState.instant,
    render: async () => {throw new Error('must not render during cooldown');}});
  assert.equal((await media.step()).state, 'browser-cooldown');
});

test('source URL length cannot consume the X text budget or create a publisher image card', async () => {
  const s = await fixture(), story = s.db.native.prepare('SELECT * FROM stories').get();
  const result = xPublication({...story, source_url: story.source_url + '?q=' + 'a'.repeat(2100)});
  assert.ok(result.composeUrl);
  assert.ok(result.weightedLength <= 280);
  assert.equal(result.text.includes('?q='), false);
  assert.ok(result.sourceUrl.includes('?q='));
});

test('X package pairs the selected story with its exact approved cover and refreshes legacy text', async () => {
  const s = await fixture();
  for (let n = 0; n < 8; n++) await s.media.step();
  const job = s.db.native.prepare('SELECT * FROM visual_jobs').get();
  const manifest = JSON.parse(job.manifest);
  manifest.x = {text:'Cortado a mitad de los...',composeUrl:'https://twitter.com/intent/tweet?text=old'};
  s.db.native.prepare('UPDATE visual_jobs SET manifest=?').run(JSON.stringify(manifest));
  assert.equal(await s.media.prepareX(s.story.id), false);
  await s.media.review(s.story.id, Number(job.review_message), true);
  const send = s.state.calls.filter(x => x.url.endsWith('/sendDocument') && x.options.body.get('document') === 'file-1').at(-1);
  assert.ok(send);
  assert.ok(send.options.body.get('caption').includes(s.story.id.slice(0,8)));
  const message=s.state.messages.at(-1);
  const link=message.reply_markup.inline_keyboard[0][0].url;
  const text=new URL(link).searchParams.get('text');
  assert.ok(!text.includes('Cortado'));
  assert.ok(!text.includes('http'));
  assert.ok(message.text.includes(s.story.source_url));
  assert.ok(message.text.includes('solo texto'));
  assert.equal(s.mediaState.generated,1);
  assert.equal(s.db.native.prepare('SELECT manifest FROM visual_jobs').get().manifest,JSON.stringify(manifest));
  assert.equal(await s.media.prepareX('f'.repeat(32)), false);
  const before=s.state.messages.length;
  s.db.native.prepare("UPDATE visual_assets SET delivery='uncertain' WHERE position=0").run();
  assert.equal(await s.media.prepareX(s.story.id),false);
  assert.equal(s.state.messages.length,before);
});

test('caption-only preview corrections reuse the image at the cap without changing approval', async()=>{
  const s=await fixture();
  await s.media.preview(s.story.id,0);
  const generated=s.mediaState.generated;
  const editorial=JSON.parse(s.story.editorial);
  editorial.headline='Un anuncio de prueba que merece contexto';
  editorial.hook='Un anuncio de prueba que merece contexto';
  editorial.angle='Comprueba la fecha y las condiciones del comunicado.';
  s.db.native.prepare('UPDATE stories SET editorial=? WHERE id=?').run(JSON.stringify(editorial),s.story.id);
  s.env.DAILY_VISUAL_LIMIT='0';
  const result=await s.media.preview(s.story.id,0);
  assert.equal(result.state,'preview');
  assert.equal(s.mediaState.generated,generated);
  assert.ok(s.mediaState.rendered.at(-1).html.includes(editorial.angle));
  assert.equal(s.db.native.prepare('SELECT state FROM visual_jobs').get().state,'queued');
});

test('a replayed approval recovers a missing visual job without duplicating the text package', async () => {
  const s = await fixture();
  s.db.native.prepare('DELETE FROM visual_jobs WHERE story_id=?').run(s.story.id);
  assert.equal(s.state.documents.length, 1);
  await s.pipeline.handleUpdate(callback(s.story));
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM visual_jobs').get().n, 1);
  assert.equal(s.state.documents.length, 1);
});

test('varied impact style keeps attribution and preserves both previous renderers', async () => {
  const s = await fixture(), story = s.db.native.prepare('SELECT * FROM stories').get();
  const brief = visualBrief(story), html = visualHtml(brief, JPEG);
  assert.equal(brief.version, 3);
  assert.ok(brief.prompt.includes('photorealistic cinematic'));
  assert.ok(html.includes(brief.art.rule));
  assert.ok(html.includes(brief.art.accent));
  assert.ok(html.includes('class="brand">WaCapital'));
  assert.ok(html.includes(brief.hook));
  const legacy = {...brief, version: 1, style: undefined};
  assert.deepEqual(visualDimensions(legacy, 0), {width: 1200, height: 675});
  assert.ok(visualHtml(legacy, JPEG).includes('width:1200px;height:675px'));
  const v2 = {...brief, version: 2, style: 'impact-v2'};
  assert.ok(visualHtml(v2, JPEG).includes('#ef4444'));
  assert.deepEqual(visualDimensions(v2), {width: 1080, height: 1350});
});

test('style previews reuse their own image within the same free budget and preserve delivered approvals', async () => {
  const s = await fixture();
  for (let n = 0; n < 8; n++) await s.media.step();
  const before = s.db.native.prepare('SELECT * FROM visual_jobs').get();
  const p0 = await s.media.preview(s.story.id, 0);
  const p1 = await s.media.preview(s.story.id, 1);
  assert.equal(p0.state, 'preview');
  assert.equal(p1.state, 'preview');
  assert.equal(s.mediaState.generated, 2);
  assert.deepEqual(s.db.native.prepare('SELECT * FROM visual_jobs').get(), before);
  assert.equal(s.db.native.prepare('SELECT used FROM visual_budget').get().used, 2);
  assert.equal(s.mediaState.documents.length, 6);
});

test('AI scene avoids company lettering while the composited headline preserves its exact brand', async () => {
  const s = await fixture(), story = s.db.native.prepare('SELECT * FROM stories').get();
  const e = JSON.parse(story.editorial); e.headline = 'Binance cubre pérdidas';
  const brief = visualBrief({...story, title: 'Binance covers losses after hack', snippet: 'Binance issued an update.', editorial: e});
  assert.equal(brief.title, e.headline);
  assert.equal(/Binance/i.test(brief.prompt), false);
  assert.ok(visualHtml(brief, JPEG).includes(e.headline));
});
