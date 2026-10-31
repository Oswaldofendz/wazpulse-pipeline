export class ServiceError extends Error {
  constructor(service, status = 0) {
    super(service + ' unavailable');
    this.service = service;
    this.status = status;
  }
}

export async function readBounded(response, maxBytes = 240_000) {
  const reader = response.body?.getReader();
  if (!reader) throw new ServiceError('response');
  const decoder = new TextDecoder();
  let size = 0, text = '';
  try {
    while (true) {
      const {value, done} = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > maxBytes) {
        await reader.cancel();
        throw new ServiceError('response-size');
      }
      text += decoder.decode(value, {stream: true});
    }
    return text + decoder.decode();
  } finally { reader.releaseLock(); }
}

export function network(fetcher = fetch) {
  return {
    async json(url, options = {}, timeout = 20_000) {
      let response;
      try {
        response = await fetcher(url, {...options, redirect: 'manual', signal: AbortSignal.timeout(timeout)});
      } catch { throw new ServiceError('network'); }
      if (!response.ok) throw new ServiceError('http', response.status);
      try { return JSON.parse(await readBounded(response)); }
      catch (error) {
        if (error instanceof ServiceError) throw error;
        throw new ServiceError('json');
      }
    }
  };
}

export function telegram(env, net) {
  const url = method => 'https://api.telegram.org/bot' + env.TELEGRAM_BOT_TOKEN + '/' + method;
  async function call(method, body) {
    const data = await net.json(url(method), {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)
    }, 8000);
    if (!data.ok) throw new ServiceError('telegram', data.error_code || 0);
    return data.result;
  }
  return {
    info: () => call('getMe', {}),
    connect: target => call('setWebhook', {
      url: target, secret_token: env.TELEGRAM_WEBHOOK_SECRET,
      allowed_updates: ['message', 'callback_query'],
      drop_pending_updates: false, max_connections: 1
    }),
    send: (text, keyboard) => call('sendMessage', {
      chat_id: env.TELEGRAM_CHAT_ID, text, link_preview_options: {is_disabled: true},
      ...(keyboard ? {reply_markup: keyboard} : {})
    }),
    answer: (id, text) => call('answerCallbackQuery', {callback_query_id: id, text}),
    async document(text, filename) {
      const form = new FormData();
      form.set('chat_id', env.TELEGRAM_CHAT_ID);
      form.set('caption', 'WaCapital · Material aprobado. Revisa el archivo antes de publicar.');
      form.set('document', new Blob([text], {type: 'text/plain;charset=utf-8'}), filename);
      const data = await net.json(url('sendDocument'), {method: 'POST', body: form}, 10_000);
      if (!data.ok) throw new ServiceError('telegram', data.error_code || 0);
      return data.result;
    }
  };
}
