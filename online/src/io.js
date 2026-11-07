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
    async json(url, options = {}, timeout = 20_000, uploadLimit = 2_100_000, allowUnchangedEdit = false) {
      let response;
      if(options.body instanceof FormData){
        // Workers can stream FormData with no known length. Materialize this
        // bounded upload so the origin receives a fixed-length request body.
        const encoded=new Response(options.body);
        const body=await encoded.arrayBuffer();
        if(body.byteLength>Math.min(uploadLimit,10_000_000))throw new ServiceError('upload-size');
        const headers=new Headers(options.headers);
        headers.set('Content-Type',encoded.headers.get('Content-Type'));
        options={...options,headers,body};
      }
      try {
        response = await fetcher(url, {...options, redirect: 'manual', signal: AbortSignal.timeout(timeout)});
      } catch (error) { throw new ServiceError(['TimeoutError','AbortError'].includes(error?.name)?'network-timeout':'network'); }
      if (!response.ok) {
        if (allowUnchangedEdit && response.status === 400) {
          try {
            const data = JSON.parse(await readBounded(response, 16_000));
            if (data.ok === false && data.error_code === 400 && /message is not modified/i.test(data.description || '')) return data;
          } catch {}
        }
        throw new ServiceError('http', response.status);
      }
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
  async function call(method, body, timeout = 8000) {
    const editing = ['editMessageText', 'editMessageReplyMarkup'].includes(method);
    const data = await net.json(url(method), {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)
    }, timeout, 2_100_000, editing);
    if(!data.ok && editing && data.error_code===400 && /message is not modified/i.test(data.description||''))return {message_id:body.message_id};
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
    edit: (messageId, text, keyboard) => call('editMessageText', {
      chat_id: env.TELEGRAM_CHAT_ID, message_id: Number(messageId), text,
      link_preview_options: {is_disabled:true}, reply_markup: keyboard || {inline_keyboard:[]}
    }),
    buttons: (messageId, keyboard) => call('editMessageReplyMarkup', {
      chat_id: env.TELEGRAM_CHAT_ID, message_id: Number(messageId), reply_markup: keyboard
    }),
    async album(assets, caption) {
      const form=new FormData();
      form.set('chat_id',env.TELEGRAM_CHAT_ID);
      form.set('disable_notification','true');
      form.set('media',JSON.stringify(assets.map((asset,i)=>({type:'document',media:'attach://image'+i,...(i===0?{caption}:{} )}))));
      for(const [i,asset] of assets.entries())form.set('image'+i,new Blob([asset.png],{type:'image/png'}),asset.filename);
      const result=await net.json(url('sendMediaGroup'),{method:'POST',body:form},45_000,10_000_000);
      if(!result.ok)throw new ServiceError('telegram-album',result.error_code||0);
      return result.result;
    },
    async imageDocument(image, filename, caption) {
      const form = new FormData();
      form.set('chat_id', env.TELEGRAM_CHAT_ID);
      form.set('caption', caption.slice(0, 950));
      if (typeof image === 'string') form.set('document', image);
      else form.set('document', new Blob([image], {type: 'image/png'}), filename);
      const data = await net.json(url('sendDocument'), {method: 'POST', body: form}, 45_000);
      if (!data.ok) throw new ServiceError('telegram', data.error_code || 0);
      return data.result;
    },
    async download(fileId) {
      const info = await call('getFile', {file_id: fileId},25_000);
      if (typeof info.file_path !== 'string' || !/^[A-Za-z0-9_./-]+$/.test(info.file_path) || info.file_path.includes('..')) throw new ServiceError('telegram-file');
      return 'https://api.telegram.org/file/bot' + env.TELEGRAM_BOT_TOKEN + '/' + info.file_path;
    },
    async document(text, filename) {
      const form = new FormData();
      form.set('chat_id', env.TELEGRAM_CHAT_ID);
      form.set('caption', 'WaCapital · Material aprobado. Revisa el archivo antes de publicar.');
      form.set('document', new Blob([text], {type: 'text/plain;charset=utf-8'}), filename);
      const data = await net.json(url('sendDocument'), {method: 'POST', body: form}, 45_000);
      if (!data.ok) throw new ServiceError('telegram', data.error_code || 0);
      return data.result;
    }
  };
}
