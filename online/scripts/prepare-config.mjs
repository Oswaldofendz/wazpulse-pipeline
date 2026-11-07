import {readFileSync, writeFileSync} from 'node:fs';

const id = process.argv[2];
if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id || '')) {
  console.error('Uso: node scripts/prepare-config.mjs ID_REAL_DE_D1');
  process.exit(1);
}
const template = readFileSync(new URL('../wrangler.jsonc', import.meta.url), 'utf8');
const config = JSON.parse(template.replace(/^\s*\/\/.*$/gm, ''));
config.d1_databases = [{binding: 'DB', database_name: 'wacapital-editorial', database_id: id}];
writeFileSync(new URL('../wrangler.local.jsonc', import.meta.url), JSON.stringify(config, null, 2) + '\n', {mode: 0o600});
console.log('Configuración local preparada con la base indicada. Bot e IA siguen pausados.');
