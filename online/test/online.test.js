import test from 'node:test';
import assert from 'node:assert/strict';
import {createPipeline} from '../src/pipeline.js';
import {normalizeStory, validateEditorial, isOwner, minimalUpdate, makePack} from '../src/domain.js';
import {readBounded, network} from '../src/io.js';
import {store} from '../src/store.js';
import worker from '../src/worker.js';
import {NOW, article, angle, setup, callback} from './helpers.js';

test('source URLs, dates and scores fail closed', async () => {
  assert.ok(await normalizeStory(article(), NOW));
  for (const change of [
    {url: 'http://www.cnbc.com/news'}, {url: 'https://example.com/news'},
    {url: 'https://www.cnbc.com@evil.test/news'}, {feedStale: true},
    {priority: '90'}, {priority: null}, {priority: Infinity},
    {publishedAt: new Date(NOW + 1).toISOString()},
    {publishedAt: new Date(NOW - 86_400_000).toISOString()}, {reportType: 'outlook'}
  ]) assert.equal(await normalizeStory(article(change), NOW), null);
});

test('tracking variants share identity; explicit assets stay distinct from market references', async () => {
  const a = await normalizeStory(article(), NOW);
  const b = await normalizeStory(article({url: article().url + '?utm_source=social#section', assets: ['BTC']}), NOW);
  assert.equal(a.id, b.id);
  assert.deepEqual(a.assets, ['USD']);
});

test('invalid AI classifications and incomplete writing cannot become drafts', () => {
  assert.ok(validateEditorial(angle()));
  for (const change of [{strength: null}, {strength: '5'}, {strength: 5.5},
    {strength: 0}, {provider: 'unknown'}, {reasoning: ''}, {tweets: []}]) {
    assert.equal(validateEditorial(angle(change)), null);
  }
});

test('owner checks require both configured chat and sender; groups need an approver', () => {
  const update = callback({id: 'a'.repeat(32), message_id: 1});
  assert.ok(isOwner(update, {TELEGRAM_CHAT_ID: '123'}));
  assert.equal(isOwner(update, {TELEGRAM_CHAT_ID: '456', TELEGRAM_APPROVER_ID: '123'}), false);
  update.callback_query.message.chat.id = -999;
  assert.equal(isOwner(update, {TELEGRAM_CHAT_ID: '-999'}), false);
  assert.ok(isOwner(update, {TELEGRAM_CHAT_ID: '-999', TELEGRAM_APPROVER_ID: '123'}));
});

test('webhook strips unnecessary message content and rejects malformed callbacks', () => {
  const input = callback({id: 'a'.repeat(32), message_id: 1});
  input.callback_query.message.text = 'Unnecessary personal content';
  const minimal = minimalUpdate(input);
  assert.equal(minimal.callback_query.message.text, undefined);
  assert.equal(minimalUpdate({...input, update_id: '50'}), null);
  assert.equal(minimalUpdate(callback({id: '<script>', message_id: 1})), null);
});

test('paused pipeline makes no external requests; AI switch allows collection without LLM', async () => {
  const s = setup({PIPELINE_ENABLED: 'false'});
  const p = createPipeline(s.env, {fetcher: s.fetcher, now: () => NOW});
  assert.equal((await p.tick()).state, 'paused');
  assert.equal(s.state.calls.length, 0);
  s.env.PIPELINE_ENABLED = 'true';
  s.env.AI_ENABLED = 'false';
  await p.tick();
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM stories').get().n, 1);
  assert.equal(s.state.calls.some(x => x.url.includes('news-angle')), false);
});

test('full cycle produces a single review; repeat cycles do not duplicate', async () => {
  const s = setup(), p = createPipeline(s.env, {fetcher: s.fetcher, now: () => NOW});
  await p.tick();
  await p.tick();
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM stories').get().n, 1);
  assert.equal(s.state.messages.length, 1);
  const row = s.db.native.prepare('SELECT * FROM stories').get();
  assert.equal(row.state, 'pending');
  assert.equal(row.notification, 'sent');
  assert.equal(s.state.documents.length, 0);
});

test('budget reservation is atomic and counts failed editorial requests', async () => {
  const s = setup({DAILY_LLM_LIMIT: '1'}), p = createPipeline(s.env, {fetcher: s.fetcher, now: () => NOW});
  s.state.failAI = true;
  await p.tick();
  await p.tick();
  assert.equal(s.state.calls.filter(x => x.url.includes('news-angle')).length, 1);
  assert.equal((await p.status()).dailyBackendCalls, 1);
  const db = store(s.db);
  assert.equal(await db.reserve('2026-10-31', 1), false);
  assert.equal(await db.reserve('2026-11-01', 1), true);
});

test('concurrent ticks cannot consume the same candidate', async () => {
  const s = setup(), p = createPipeline(s.env, {fetcher: s.fetcher, now: () => NOW});
  await Promise.all([p.tick(), p.tick()]);
  assert.equal(s.state.calls.filter(x => x.url.includes('news-angle')).length, 1);
  assert.equal(s.state.messages.length, 1);
});

test('ambiguous Telegram send is recorded and is not automatically resent', async () => {
  const s = setup(), p = createPipeline(s.env, {fetcher: s.fetcher, now: () => NOW});
  s.state.failTelegram = true;
  await p.tick();
  await p.tick();
  assert.equal(s.state.messages.length, 1);
  assert.equal(s.db.native.prepare('SELECT notification FROM stories').get().notification, 'uncertain');
});

test('approval must match original message; repeated callbacks deliver one package', async () => {
  const s = setup(), p = createPipeline(s.env, {fetcher: s.fetcher, now: () => NOW});
  await p.tick();
  let story = s.db.native.prepare('SELECT * FROM stories').get();
  const wrong = callback(story);
  wrong.callback_query.message.message_id++;
  await p.handleUpdate(wrong);
  assert.equal(s.state.documents.length, 0);
  const foreign = callback(story, {from: {id: 456}});
  await p.handleUpdate(foreign);
  assert.equal(s.state.documents.length, 0);
  await p.handleUpdate(callback(story));
  await p.handleUpdate(callback(story));
  story = s.db.native.prepare('SELECT * FROM stories').get();
  assert.equal(story.state, 'approved');
  assert.equal(story.pack_delivery, 'sent');
  assert.equal(s.state.documents.length, 1);
  for (const label of ['X ·', 'Instagram ·', 'TikTok / YouTube', story.source_url, 'titular y extracto RSS']) {
    assert.ok(s.state.documents[0].includes(label));
  }
});

test('rejection blocks later approval and package delivery', async () => {
  const s = setup(), p = createPipeline(s.env, {fetcher: s.fetcher, now: () => NOW});
  await p.tick();
  const story = s.db.native.prepare('SELECT * FROM stories').get();
  await p.handleUpdate(callback(story, {data: 'reject:' + story.id}));
  await p.handleUpdate(callback(story));
  assert.equal(await p.deliverPack(story.id, true), false);
  assert.equal(s.state.documents.length, 0);
});

test('an uncertain approved package stays discoverable and can be retried explicitly', async () => {
  const s = setup(), p = createPipeline(s.env, {fetcher: s.fetcher, now: () => NOW});
  await p.tick();
  const story = s.db.native.prepare('SELECT * FROM stories').get();
  s.state.failTelegram = true;
  await p.handleUpdate(callback(story));
  assert.equal(s.db.native.prepare('SELECT pack_delivery FROM stories').get().pack_delivery, 'uncertain');
  s.state.failTelegram = false;
  await p.handleUpdate({message: {text: '/paquetes', from: {id: 123}, chat: {id: 123}}});
  assert.ok(s.state.messages.at(-1).text.includes('/paquete ' + story.id));
  await p.handleUpdate({message: {text: '/paquete ' + story.id, from: {id: 123}, chat: {id: 123}}});
  assert.equal(s.db.native.prepare('SELECT pack_delivery FROM stories').get().pack_delivery, 'sent');
});

test('deduplicated inbox survives a restart and resumes expired processing leases', async () => {
  const s = setup(), db = store(s.db);
  const update = {update_id: 55, message: {from: {id: 123}, chat: {id: 123}, text: '/estado'}};
  await db.queueUpdate(update, NOW);
  await db.queueUpdate(update, NOW);
  assert.equal(s.db.native.prepare('SELECT COUNT(*) AS n FROM inbox').get().n, 1);
  await db.claimUpdate(NOW - 120_000);
  const p = createPipeline(s.env, {fetcher: s.fetcher, now: () => NOW});
  await p.drainInbox();
  assert.equal(s.db.native.prepare('SELECT state FROM inbox').get().state, 'done');
  assert.equal(s.state.messages.length, 1);
});

test('response bounds prevent oversized news or webhook payloads', async () => {
  await assert.rejects(readBounded(new Response('x'.repeat(100)), 10));
});

test('Worker-compatible fetch rejects redirects without forwarding credentials', async () => {
  const requests = [];
  const net = network(async (url, options) => {
    requests.push({url, options});
    if (options.redirect === 'error') throw new TypeError('Unsupported Worker redirect mode');
    return new Response('', {status: 302, headers: {Location: 'https://other-host.invalid/'}});
  });
  await assert.rejects(net.json('https://api.telegram.org/botfixture/getMe', {
    method: 'POST', body: '{}'
  }), error => error.status === 302);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].options.redirect, 'manual');
});

test('public routes disclose neither stories nor configuration; admin/webhook need secrets', async () => {
  const s = setup(), ctx = {waitUntil() {}};
  const response = await worker.fetch(new Request('https://test/health'), s.env, ctx);
  const text = await response.text();
  assert.equal(response.status, 200);
  assert.equal(text.includes('test-only'), false);
  assert.equal((await worker.fetch(new Request('https://test/admin/status'), s.env, ctx)).status, 401);
  assert.equal((await worker.fetch(new Request('https://test/telegram/webhook', {method: 'POST'}), s.env, ctx)).status, 401);
});

test('webhook registration requires admin access and the configured Worker origin', async () => {
  const s = setup({ADMIN_TOKEN: 'fixture-admin', TELEGRAM_WEBHOOK_SECRET: 'fixture-webhook',
    WORKER_ORIGIN: 'https://fixture-owner.workers.dev'});
  const savedFetch = globalThis.fetch;
  globalThis.fetch = s.fetcher;
  try {
    const url = s.env.WORKER_ORIGIN + '/admin/telegram-connect';
    assert.equal((await worker.fetch(new Request(url, {method: 'POST'}), s.env, {})).status, 401);
    assert.equal(s.state.calls.length, 0);
    const headers = {Authorization: 'Bearer fixture-admin'};
    const response = await worker.fetch(new Request(url, {method: 'POST', headers}), s.env, {});
    assert.equal(response.status, 200);
    const call = s.state.calls.find(x => x.url.endsWith('/setWebhook'));
    const body = JSON.parse(call.options.body);
    assert.equal(body.url, s.env.WORKER_ORIGIN + '/telegram/webhook');
    assert.equal(body.drop_pending_updates, false);
    s.state.calls = [];
    s.env.WORKER_ORIGIN = 'https://other-origin.workers.dev';
    assert.equal((await worker.fetch(new Request(url, {method: 'POST', headers}), s.env, {})).status, 400);
    assert.equal(s.state.calls.length, 0);
  } finally { globalThis.fetch = savedFetch; }
});
