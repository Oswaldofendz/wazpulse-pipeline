import {loadManagedSecrets} from './managed-secrets.mjs';

const origin = 'https://wacapital-online.wacapital-online.workers.dev';
const command = process.argv[2] || 'status';
const routes = {
  status: ['/admin/status', 'GET'],
  tick: ['/admin/tick', 'POST'],
  'media-status': ['/admin/media-status', 'GET'],
  'media-step': ['/admin/media-step', 'POST'],
  'channel-status': ['/admin/channel-status', 'GET'],
  'channel-step': ['/admin/channel-step', 'POST'],
  'storage-status': ['/admin/storage-status', 'GET'],
  'storage-plan': ['/admin/storage-plan', 'GET'],
  'storage-cleanup': ['/admin/storage-cleanup', 'POST'],
  'telegram-info': ['/admin/telegram-info', 'GET'],
  'telegram-connect': ['/admin/telegram-connect', 'POST']
};
try {
  if (!routes[command]) throw new Error('Comando administrativo no reconocido.');
  const values = loadManagedSecrets();
  const [path, method] = routes[command];
  const response = await fetch(origin + path, {
    method, redirect: 'error', signal: AbortSignal.timeout(90_000),
    headers: {Authorization: 'Bearer ' + values.ADMIN_TOKEN}
  });
  const result = await response.json();
  console.log(JSON.stringify({http: response.status, ...result}, null, 2));
  if (!response.ok) process.exitCode = 1;
} catch {
  console.error('No se pudo completar la operación administrativa; no se muestran credenciales.');
  process.exitCode = 1;
}
