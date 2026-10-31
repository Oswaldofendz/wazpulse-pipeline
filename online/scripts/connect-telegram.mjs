// Run manually after configuring the same secrets in the user's Worker.
// No .env file is opened and no secret values are printed.
let base;
try { base = new URL(process.argv[2]); } catch {}
const token = process.env.TELEGRAM_BOT_TOKEN;
const secret = process.env.TELEGRAM_WEBHOOK_SECRET;
if (!base || base.protocol !== 'https:' || !base.hostname.endsWith('.workers.dev') ||
    base.username || base.password || base.search || base.hash || (base.port && base.port !== '443') ||
    !token || !/^[A-Za-z0-9_-]{16,256}$/.test(secret || '')) {
  console.error('Indica la URL HTTPS real del Worker y las variables TELEGRAM_BOT_TOKEN y TELEGRAM_WEBHOOK_SECRET.');
  process.exit(1);
}
try {
  const healthResponse = await fetch(new URL('/health', base), {redirect: 'error', signal: AbortSignal.timeout(10_000)});
  const health = await healthResponse.json();
  if (!healthResponse.ok || health.service !== 'wacapital-online' || !health.enabled || !health.databaseBound) {
    throw new Error('Worker not ready');
  }
  const response = await fetch('https://api.telegram.org/bot' + token + '/setWebhook', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    redirect: 'error', signal: AbortSignal.timeout(10_000),
    body: JSON.stringify({
      url: new URL('/telegram/webhook', base).href,
      secret_token: secret,
      allowed_updates: ['callback_query', 'message'],
      drop_pending_updates: false,
      max_connections: 1
    })
  });
  const result = await response.json();
  if (!response.ok || !result.ok) throw new Error('Webhook rejected');
  console.log('Webhook conectado. Los mensajes pendientes de Telegram se conservan.');
} catch {
  console.error('No se pudo conectar el webhook. Revisa salud, configuración y permisos; no se muestran secretos.');
  process.exitCode = 1;
}
