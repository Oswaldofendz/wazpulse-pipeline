import {mkdir, writeFile} from 'node:fs/promises';
import {resolve, join} from 'node:path';
import {loadManagedSecrets} from './managed-secrets.mjs';

const origin = 'https://wacapital-online.wacapital-online.workers.dev';
const [command = 'status', id, output, selection] = process.argv.slice(2);
try {
  const secret = loadManagedSecrets().ADMIN_TOKEN;
  const request = async (path, method = 'GET') => {
    const response = await fetch(origin + path, {method, redirect: 'error',
      headers: {Authorization: 'Bearer ' + secret}, signal: AbortSignal.timeout(90_000)});
    if (!response.ok) throw new Error('HTTP ' + response.status);
    return response;
  };
  if (command === 'status' || command === 'step') {
    const response = await request('/admin/media-' + (command === 'step' ? 'step' : 'status'), command === 'step' ? 'POST' : 'GET');
    console.log(JSON.stringify(await response.json(), null, 2));
  } else {
    if (!/^[a-f0-9]{32}$/.test(id || '')) throw new Error('Invalid story ID');
    if (command === 'enqueue' || command === 'retry') console.log(JSON.stringify(await (await request('/admin/media/' + id + '/' + command, 'POST')).json()));
    else if (command === 'download') {
      if (!output) throw new Error('Output directory required');
      const directory = resolve(output);
      await mkdir(directory, {recursive: true});
      const manifest = await (await request('/admin/media/' + id + '/manifest')).json();
      await writeFile(join(directory, 'manifest.json'), JSON.stringify(manifest, null, 2), {flag: 'wx'});
      const positions = /^[0-5]$/.test(selection || '') ? [Number(selection)] : [0,1,2,3,4,5];
      for (const n of positions) {
        const response = await request('/admin/media/' + id + '/' + n);
        const bytes = new Uint8Array(await response.arrayBuffer());
        const filename = n === 0 ? 'X-portada.png' : 'carrusel-' + n + '.png';
        await writeFile(join(directory, filename), bytes, {flag: 'wx'});
        console.log(filename + ': ' + bytes.length + ' bytes');
      }
    } else throw new Error('Unknown command');
  }
} catch (error) {
  const status = /^HTTP \d{3}$/.test(error.message || '') ? ' (' + error.message + ')' : '';
  console.error('No se completó la operación visual' + status + '. No se muestran credenciales ni respuestas privadas.');
  process.exitCode = 1;
}
