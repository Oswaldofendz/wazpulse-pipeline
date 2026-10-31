import {existsSync, readFileSync, writeFileSync} from 'node:fs';
import {randomBytes} from 'node:crypto';
import {spawnSync} from 'node:child_process';

const file = new URL('../.managed-secrets.dpapi', import.meta.url);
function powershell(script, input) {
  const result = spawnSync('pwsh.exe', ['-NoProfile', '-NonInteractive', '-Command', script], {
    input, encoding: 'utf8', windowsHide: true
  });
  if (result.status !== 0) throw new Error('No se pudo usar el almacenamiento protegido de Windows.');
  return result.stdout.trim();
}
export function loadManagedSecrets(create = false) {
  if (process.platform !== 'win32') throw new Error('Este ayudante requiere el almacenamiento protegido de Windows.');
  if (!existsSync(file)) {
    if (!create) throw new Error('Faltan los secretos internos protegidos.');
    const values = {
      ADMIN_TOKEN: randomBytes(32).toString('hex'),
      TELEGRAM_WEBHOOK_SECRET: randomBytes(32).toString('hex')
    };
    const encrypted = powershell(
      "$value=[Console]::In.ReadToEnd(); $secure=ConvertTo-SecureString -String $value -AsPlainText -Force; ConvertFrom-SecureString -SecureString $secure",
      JSON.stringify(values));
    writeFileSync(file, encrypted, {mode: 0o600});
    return values;
  }
  return JSON.parse(powershell(
    "$value=[Console]::In.ReadToEnd(); $secure=ConvertTo-SecureString -String $value; [System.Net.NetworkCredential]::new('', $secure).Password",
    readFileSync(file, 'utf8')));
}
if (process.argv[2] === 'upload') {
  try {
    const values = loadManagedSecrets(true);
    const result = spawnSync(process.execPath, [
      'node_modules/wrangler/bin/wrangler.js', 'secret', 'bulk', '--config', 'wrangler.local.jsonc'
    ], {input: JSON.stringify(values), encoding: 'utf8', windowsHide: true,
      env: {...process.env, WRANGLER_SEND_METRICS: 'false'}});
    if (result.status !== 0) throw new Error('Cloudflare no confirmó la configuración de secretos internos.');
    console.log('Dos secretos internos guardados en Cloudflare. Copia local protegida por Windows; valores no mostrados.');
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
