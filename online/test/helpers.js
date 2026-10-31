import {DatabaseSync} from 'node:sqlite';
import {readFileSync} from 'node:fs';

export function database() {
  const native = new DatabaseSync(':memory:');
  native.exec(readFileSync(new URL('../schema.sql', import.meta.url), 'utf8'));
  native.exec(readFileSync(new URL('../media-schema.sql', import.meta.url), 'utf8'));
  return {
    native,
    prepare(sql) {
      const stmt = native.prepare(sql);
      return {
        bind(...args) {
          args = args.map(value => value instanceof ArrayBuffer ? new Uint8Array(value) : value);
          return {
            async first() { return stmt.get(...args) || null; },
            async all() { return {results: stmt.all(...args)}; },
            async run() {
              const result = stmt.run(...args);
              return {meta: {changes: Number(result.changes)}};
            }
          };
        }
      };
    }
  };
}

export const NOW = Date.parse('2026-10-31T12:00:00Z');
export function article(override = {}) {
  return {
    title: 'Federal Reserve issues FOMC statement',
    url: 'https://www.federalreserve.gov/newsevents/pressreleases/monetary-test.htm',
    publishedAt: new Date(NOW - 3_600_000).toISOString(),
    source: 'Federal Reserve', priority: 94, official: true,
    eventKey: 'fed_decision:2026-10-31',
    snippet: 'Test fixture only. No real news or economic assertion.',
    explicitAssets: ['USD'], ...override
  };
}
export function angle(override = {}) {
  return {
    strength: 5, provider: 'groq', headlines: ['Una decisión a revisar'],
    hook: 'Qué cambia con este anuncio',
    angle: 'La lectura depende del contexto y de la fuente.',
    reasoning: 'Revisa el anuncio original. Esta es una muestra de prueba, no una noticia.',
    tweets: ['Un anuncio que merece contexto.'],
    instagram_caption: 'Qué ocurrió y qué conviene comprobar.', ...override
  };
}
export function setup(overrides = {}) {
  const db = database();
  const state = {news: [article()], angle: angle(), calls: [], documents: [], messages: [], failAI: false, failTelegram: false};
  const env = {
    DB: db, PIPELINE_ENABLED: 'true', AI_ENABLED: 'true', DAILY_LLM_LIMIT: '6',
    TELEGRAM_CHAT_ID: '123', TELEGRAM_APPROVER_ID: '123',
    TELEGRAM_BOT_TOKEN: 'test-only', ...overrides
  };
  const fetcher = async (url, options = {}) => {
    state.calls.push({url, options});
    if (url.endsWith('/api/market/impact-alerts')) return Response.json({status: 'ok', alerts: state.news});
    if (url.endsWith('/api/analysis/news-angle')) {
      if (state.failAI) return Response.json({error: 'quota'}, {status: 429});
      return Response.json(state.angle);
    }
    if (url.endsWith('/sendMessage')) {
      state.messages.push(JSON.parse(options.body));
      if (state.failTelegram) throw new Error('response lost after sending');
      return Response.json({ok: true, result: {message_id: 100 + state.messages.length}});
    }
    if (url.endsWith('/answerCallbackQuery')) return Response.json({ok: true, result: true});
    if (url.endsWith('/getMe')) return Response.json({ok: true, result: {username: 'fixture_bot'}});
    if (url.endsWith('/setWebhook')) return Response.json({ok: true, result: true});
    if (url.endsWith('/sendDocument')) {
      state.documents.push(await options.body.get('document').text());
      if (state.failTelegram) throw new Error('response lost after sending');
      return Response.json({ok: true, result: {message_id: 999}});
    }
    throw new Error('Unexpected external call');
  };
  return {env, db, state, fetcher};
}
export function callback(story, overrides = {}) {
  return {update_id: 50, callback_query: {
    id: 'callback-test', data: 'approve:' + story.id, from: {id: 123},
    message: {message_id: Number(story.message_id), chat: {id: 123}},
    ...overrides
  }};
}
