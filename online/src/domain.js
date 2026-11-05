import {xPublication} from './publication.js';

export const BACKEND = 'https://wastake.onrender.com';
export const DAY = 86_400_000;
export const DISCLAIMER = 'Información educativa. No es asesoramiento financiero.';

export function clean(value, max = 1800) {
  return String(value ?? '').replace(/<[^>]*>/g, ' ').replace(/[\u0000-\u001f]/g, ' ')
    .replace(/\s+/g, ' ').trim().slice(0, max);
}

export function publicSourceUrl(value) {
  try {
    const u = new URL(value);
    const allowed = new Set(['www.federalreserve.gov', 'www.ecb.europa.eu', 'www.cnbc.com',
      'www.coindesk.com', 'cointelegraph.com', 'www.bbc.co.uk', 'www.bbc.com']);
    if (u.protocol !== 'https:' || u.username || u.password || (u.port && u.port !== '443') || !allowed.has(u.hostname)) return null;
    u.hash = '';
    for (const key of [...u.searchParams.keys()]) {
      if (/^utm_|^(fbclid|gclid)$/i.test(key)) u.searchParams.delete(key);
    }
    u.searchParams.sort();
    return u.href;
  } catch { return null; }
}

export async function digest(value) {
  const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return Array.from(new Uint8Array(bytes), x => x.toString(16).padStart(2, '0')).join('');
}

export async function normalizeStory(article, now) {
  if (!article || article.feedStale || article.reportType === 'outlook') return null;
  const url = publicSourceUrl(article.url);
  const title = clean(article.title, 500);
  const published = Date.parse(article.publishedAt);
  const priority = article.priority;
  if (!url || title.length < 8 || !Number.isFinite(published) || published > now || now - published >= DAY ||
      typeof priority !== 'number' || !Number.isFinite(priority) || priority < 0 || priority > 100) return null;
  const id = (await digest(url)).slice(0, 32);
  // Backend event keys deduplicate macro announcements across newsrooms.
  const eventKey = clean(article.eventKey, 600) || url;
  return {
    id, eventKey, title, url, published, priority,
    source: clean(article.source, 80),
    snippet: clean(article.snippet, 1800),
    assets: (Array.isArray(article.explicitAssets) ? article.explicitAssets : [])
      .filter(x => typeof x === 'string' && /^[A-Z0-9.^=-]{1,16}$/.test(x)).slice(0, 8)
  };
}

export function validateEditorial(value) {
  const score = value?.strength;
  if (typeof score !== 'number' || !Number.isInteger(score) || score < 1 || score > 5 ||
      !['groq', 'gemini'].includes(String(value?.provider).toLowerCase())) return null;
  const array = x => Array.isArray(x) ? x.filter(y => typeof y === 'string').map(y => clean(y, 600)) : [];
  const result = {
    strength: score, provider: String(value.provider).toLowerCase(),
    headline: array(value.headlines)[0] || '',
    hook: clean(value.hook, 200), angle: clean(value.angle, 1000),
    reasoning: clean(value.reasoning, 2200),
    tweet: array(value.tweets)[0] || '', instagram: clean(value.instagram_caption, 2000)
  };
  return result.headline && result.angle && result.reasoning && result.tweet ? result : null;
}

export function isOwner(update, env) {
  const item = update.callback_query || update.message;
  const message = update.callback_query?.message || update.message;
  const owner = env.TELEGRAM_APPROVER_ID || (/^\d+$/.test(env.TELEGRAM_CHAT_ID || '') ? env.TELEGRAM_CHAT_ID : null);
  return Boolean(owner && item?.from?.id && message?.chat?.id &&
    String(item.from.id) === String(owner) && String(message.chat.id) === String(env.TELEGRAM_CHAT_ID));
}

export function minimalUpdate(update) {
  if (!Number.isSafeInteger(update?.update_id) || update.update_id < 0) return null;
  if (update.callback_query) {
    const q = update.callback_query;
    if (typeof q.id !== 'string' || !/^(approve|reject|visualok|visualno|(?:pick|gen|ok|cancel|info)[xit]):[a-f0-9]{32}$/.test(q.data || '') ||
        !Number.isSafeInteger(q.message?.message_id)) return null;
    return {update_id: update.update_id, callback_query: {
      id: q.id.slice(0, 128), data: q.data, from: {id: q.from?.id},
      message: {message_id: q.message.message_id, chat: {id: q.message.chat?.id}}
    }};
  }
  if (update.message && typeof update.message.text === 'string') {
    return {update_id: update.update_id, message: {
      text: update.message.text.slice(0, 100), from: {id: update.message.from?.id},
      chat: {id: update.message.chat?.id}
    }};
  }
  return null;
}

export function makePack(story) {
  const e = typeof story.editorial === 'string' ? JSON.parse(story.editorial) : story.editorial;
  const stamp = new Date(story.published_at).toISOString();
  const x = xPublication(story);
  return [
    '# WaCapital · Material editorial aprobado',
    'Noticia original: ' + story.title,
    'Fuente: ' + story.source + ' · ' + story.source_url,
    'Publicada: ' + stamp,
    'Aprobación: ' + new Date(story.approved_at).toISOString(),
    'Base: titular y extracto RSS. La IA propone una interpretación; revisión humana obligatoria.',
    'Proveedor editorial: ' + e.provider + ' · Relevancia editorial: ' + e.strength + '/5',
    '',
    '## X · Borrador', x.text,
    x.ready ? 'Caracteres ponderados: ' + x.weightedLength + '/280. Comprobado con twitter-text.' :
      'El texto completo necesita edición para X. No se recortó ni se habilitó un botón de publicación.',
    x.composeUrl ? 'Abrir el borrador (adjuntar la imagen y confirmar manualmente): ' + x.composeUrl : 'Copia el texto en X, adjunta la imagen y confirma manualmente.',
    '',
    '## Instagram · Caption', e.instagram || e.hook,
    '', 'Lectura del evento: ' + e.angle, e.reasoning,
    'Fuente: ' + story.source_url, DISCLAIMER,
    '',
    '## Carrusel · Texto para 5 láminas',
    '1. ' + e.headline, '2. Qué se informó: ' + story.title,
    '3. Por qué podría importar: ' + e.angle,
    '4. Contexto y límites: ' + e.reasoning,
    '5. Consulta la fuente y contrasta antes de decidir. ' + DISCLAIMER,
    '',
    '## TikTok / YouTube Shorts · Guion base',
    'Entrada: ' + e.hook,
    'Hecho atribuido a ' + story.source + ': ' + story.title,
    'Interpretación editorial: ' + e.reasoning,
    'Cierre: Lee la fuente en la descripción y revisa cómo evoluciona la noticia.',
    'Descripción: ' + story.source_url + '\n' + DISCLAIMER,
    '',
    '## Dirección visual',
    'Crear una ilustración editorial conceptual sobre: ' + story.title,
    'No simular fotografías documentales del suceso. No inventar cifras ni testimonios.',
    'Marcar la imagen como ilustración. Formato vertical 1080×1920; miniatura 1280×720.',
    '',
    '## Revisión final',
    'Comprobar cifras, fecha, nombres propios y estado de la noticia en la fuente.',
    'Confirmar derechos de uso de imágenes, clips y música antes de publicarlos.',
    'Estos materiales son textos y guiones; no incluyen un vídeo renderizado.'
  ].join('\n\n');
}
