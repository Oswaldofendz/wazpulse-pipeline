import {BACKEND, DAY, normalizeStory, validateEditorial, isOwner, makePack} from './domain.js';
import {store} from './store.js';
import {network, telegram, ServiceError} from './io.js';
import {createMedia} from './media.js';

const enabled = value => value === 'true';
const boundedInt = (value, fallback, max) => {
  if (value === undefined || value === '') return fallback;
  const n = Number(value);
  return Number.isInteger(n) && n >= 0 && n <= max ? n : fallback;
};

export function createPipeline(env, {fetcher = fetch, now = Date.now} = {}) {
  const db = store(env.DB), net = network(fetcher), bot = telegram(env, net);
  const media = createMedia(env, {fetcher, now});
  const active = () => enabled(env.PIPELINE_ENABLED);
  const minimum = () => boundedInt(env.MIN_PRIORITY, 65, 100);
  const strength = () => boundedInt(env.MIN_STRENGTH, 4, 5);

  async function status() {
    const rows = await db.all('SELECT state,COUNT(*) AS count FROM stories GROUP BY state');
    const last = await db.first("SELECT value FROM runtime WHERE key='last_tick'");
    const budget = await db.first('SELECT used FROM daily_budget WHERE day=?', new Date(now()).toISOString().slice(0, 10));
    return {enabled: active(), aiEnabled: enabled(env.AI_ENABLED), states: rows,
      lastTick: last ? JSON.parse(last.value) : null, dailyBackendCalls: budget?.used || 0,
      dailyLimit: boundedInt(env.DAILY_LLM_LIMIT, 6, 16)};
  }

  async function sendReview(id, force = false) {
    const story = await db.notification(id, force);
    if (!story) return false;
    try {
      const e = JSON.parse(story.editorial);
      const text = [
        '📰 WaCapital · Revisión editorial', e.headline,
        'Fuente: ' + story.source + ' · ' + new Date(story.published_at).toISOString(),
        'Original: ' + story.title,
        '🧠 Lectura propuesta: ' + e.angle,
        e.reasoning,
        'Relevancia editorial: ' + e.strength + '/5 · ' + e.provider,
        'Base: titular y extracto RSS. Comprueba cifras y contexto en la fuente.',
        story.source_url,
        'Referencia: ' + story.id,
        env.MEDIA_ENABLED === 'true' ? 'Aprobar prepara el texto y pone las imágenes con IA en cola. Recibirás la portada y cinco láminas para una segunda revisión. No publica en redes.' :
          'Aprobar prepara el paquete de textos. La publicación en redes sigue siendo manual.'
      ].join('\n\n');
      const result = await bot.send(text.slice(0, 3900), {inline_keyboard: [[
        {text: '✅ Aprobar material', callback_data: 'approve:' + story.id},
        {text: '❌ Rechazar', callback_data: 'reject:' + story.id}
      ]]});
      if (!Number.isSafeInteger(result?.message_id)) throw new Error('Missing message ID');
      await db.notified(story.id, result.message_id);
      return true;
    } catch {
      // A failed response may follow a delivered message. Never blindly resend.
      await db.uncertain(story.id);
      return false;
    }
  }

  async function deliverPack(id, force = false) {
    const story = await db.claimPack(id, force);
    if (!story) return false;
    try {
      await bot.document(makePack(story), 'WaCapital-' + story.id + '.txt');
      await db.packDone(story.id);
      return true;
    } catch {
      await db.packUncertain(story.id);
      return false;
    }
  }

  async function handleUpdate(update) {
    if (!isOwner(update, env)) return;
    const callback = update.callback_query;
    if (callback) {
      // Answer immediately; document generation uses no LLM and runs afterwards.
      await bot.answer(callback.id, 'Procesando revisión').catch(() => {});
      const [action, id] = callback.data.split(':');
      if (action === 'visualok' || action === 'visualno') {
        await media.review(id, callback.message.message_id, action === 'visualok');
        return;
      }
      // Resume an interrupted approval without approving a different message
      // or sending a package twice. Both delivery claims remain idempotent.
      const story = await db.review(id, callback.message.message_id, action === 'approve', now()) ||
        (action === 'approve' ? await db.first("SELECT * FROM stories WHERE id=? AND state='approved' AND message_id=?", id, String(callback.message.message_id)) : null);
      if (story?.state === 'approved') {
        await deliverPack(id);
        if (await media.enqueue(id)) await bot.send('🎨 Texto aprobado. Prepararé una ilustración con IA, la portada para X y cinco láminas. Llegarán durante los próximos minutos; después podrás aprobar la versión visual.');
      }
      return;
    }
    const command = update.message?.text?.trim();
    if (command === '/estado' || command === '/start') {
      const result = await status();
      await bot.send('WaCapital · ' + (active() ? 'Activo' : 'Pausado') +
        '\nLlamadas editoriales hoy: ' + result.dailyBackendCalls + '/' + result.dailyLimit +
        '\nEstados: ' + result.states.map(x => x.state + ': ' + x.count).join(', ') +
        '\n/pendientes · /paquetes · /revisar ID · /paquete ID · /imagenes ID · /reenviar ID · /publicar ID');
    } else if (command === '/pendientes') {
      const rows = await db.all("SELECT id,title FROM stories WHERE state IN ('ready','pending') AND published_at>=? ORDER BY priority DESC LIMIT 5", now() - DAY);
      await bot.send(rows.length ? rows.map(x => x.title.slice(0, 180) + '\n/revisar ' + x.id).join('\n\n') : 'No hay materiales pendientes recientes.');
    } else if (command === '/paquetes') {
      const rows = await db.all("SELECT id,title,pack_delivery FROM stories WHERE state='approved' ORDER BY approved_at DESC LIMIT 5");
      await bot.send(rows.length ? rows.map(x => x.title.slice(0, 160) + '\nEntrega: ' + x.pack_delivery + '\n/paquete ' + x.id).join('\n\n') : 'Todavía no hay paquetes aprobados.');
    } else if (/^\/revisar [a-f0-9]{32}$/.test(command || '')) {
      const id = command.split(' ')[1];
      // Explicit user retry; old buttons stop working once the new ID is saved.
      if (!await sendReview(id, true)) await bot.send('No se pudo enviar la revisión. Consulta /estado.');
    } else if (/^\/paquete [a-f0-9]{32}$/.test(command || '')) {
      if (!await deliverPack(command.split(' ')[1], true)) await bot.send('No hay un paquete aprobado disponible o falló el envío.');
    } else if (/^\/imagenes [a-f0-9]{32}$/.test(command || '')) {
      const result = await media.status(command.split(' ')[1]);
      const labels = {queued: 'En cola', rendering: 'Preparando imágenes', review: 'Pendiente de tu revisión', approved: 'Versión final aprobada', rejected: 'Imágenes descartadas', blocked: 'Preparación interrumpida'};
      await bot.send(result ? '🎨 Imágenes: ' + labels[result.state] + '\nOriginales entregados: ' + result.assets.filter(x => x.delivery === 'sent').length + '/6' : 'No hay imágenes en preparación para esa noticia.');
    } else if (/^\/reenviar [a-f0-9]{32}$/.test(command || '')) {
      if (!await media.resend(command.split(' ')[1])) await bot.send('No hay imágenes disponibles para reenviar.');
    } else if (/^\/publicar [a-f0-9]{32}$/.test(command || '')) {
      if (!await media.prepareX(command.split(' ')[1])) await bot.send('Primero aprueba la versión visual y confirma la entrega de su portada. No se ha publicado nada.');
    }
  }

  async function drainInbox(max = 2) {
    if (!active()) return;
    for (let n = 0; n < max; n++) {
      const row = await db.claimUpdate(now());
      if (!row) break;
      try {
        await handleUpdate(JSON.parse(row.payload));
        await db.finishUpdate(row.update_id);
      } catch { await db.failUpdate(row.update_id, row.attempts, now()); }
    }
  }

  async function tick() {
    if (!active()) return {state: 'paused'};
    const owner = crypto.randomUUID(), instant = now();
    if (!await db.lock('tick', owner, instant, 8 * 60_000)) return {state: 'busy'};
    const result = {at: new Date(instant).toISOString(), state: 'ok', inserted: 0, editorial: 0, notifications: 0};
    let stage = 'inbox';
    try {
      await drainInbox(2);
      stage = 'capacity';
      const size = await db.first('SELECT COUNT(*) AS count FROM stories');
      if (size.count >= 5000) {
        result.state = 'storage-cap';
      } else {
        // This existing endpoint performs RSS aggregation; no provider keys move.
        stage = 'source';
        const source = await net.json(BACKEND + '/api/market/impact-alerts');
        result.sourceStatus = source.status || 'unknown';
        if (['ok', 'partial'].includes(source.status) && Array.isArray(source.alerts)) {
          const candidates = source.alerts.slice(0, 40).filter(a => typeof a.priority === 'number' && a.priority >= minimum())
            .sort((a, b) => b.priority - a.priority || Date.parse(b.publishedAt) - Date.parse(a.publishedAt));
          for (const article of candidates.slice(0, 8)) {
            stage = 'normalize';
            const story = await normalizeStory(article, instant);
            if (story) {
              stage = 'insert';
              const saved = await db.insert(story, instant);
              result.inserted += saved.meta?.changes || 0;
            }
          }
        }
      }
      if (enabled(env.AI_ENABLED)) {
        stage = 'editorial';
        const pending = await db.next(instant, instant - 6 * 3_600_000);
        const day = new Date(instant).toISOString().slice(0, 10);
        if (pending && await db.reserve(day, boundedInt(env.DAILY_LLM_LIMIT, 6, 16))) {
          const story = await db.claim(pending.id, instant);
          if (story) {
            try {
              const raw = await net.json(BACKEND + '/api/analysis/news-angle', {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({title: story.title, summary: story.snippet, link: story.source_url, tickers: JSON.parse(story.assets), lang: 'es'})
              }, 25_000);
              const editorial = validateEditorial(raw);
              if (!editorial) throw new Error('Invalid editorial');
              if (editorial.strength >= strength()) {
                await db.ready(story.id, editorial);
                result.editorial++;
              } else await db.ignore(story.id);
            } catch {
              await db.retry(story.id, instant, story.attempts);
              result.state = 'editorial-unavailable';
            }
          }
        }
      }
      stage = 'review';
      const review = await db.first("SELECT id FROM stories WHERE state='ready' AND notification='new' AND published_at>=? ORDER BY priority DESC LIMIT 1", instant - DAY);
      if (review && env.TELEGRAM_BOT_TOKEN && env.TELEGRAM_CHAT_ID) {
        result.notifications = Number(await sendReview(review.id));
      }
    } catch (error) {
      result.state = 'source-or-storage-unavailable';
      result.failedStage = stage;
      if (error instanceof ServiceError) {
        result.failure = error.service;
        result.upstreamStatus = error.status;
      }
    }
    finally {
      await db.set('last_tick', result);
      await db.unlock('tick', owner);
    }
    return result;
  }
  return {tick, status, drainInbox, handleUpdate, sendReview, deliverPack};
}
