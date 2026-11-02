import test from 'node:test';
import assert from 'node:assert/strict';
import {coverSubtitle,xPublication,visualBrief} from '../src/publication.js';

const base={headline:'Binance reabre retiros tras hackeo de $6.3B',hook:'Binance reabre retiros tras un hackeo histórico de $6.3 mil millones.',
  angle:'Bitcoin lidera la primera fase de reapertura.',reasoning:'Revisa el alcance del anuncio.',tweet:'Binance reabre retiros por fases, empezando por Bitcoin.'};
const story=(editorial={})=>({id:'1'.repeat(32),title:'Binance reopens withdrawals',snippet:'Bitcoin leads the first phase.',source:'Fixture',
  source_url:'https://www.coindesk.com/markets/test',published_at:Date.parse('2026-11-02T12:00:00Z'),editorial:{...base,...editorial}});

test('cover keeps the headline and selects complementary approved context instead of repeating it',()=>{
  assert.equal(coverSubtitle(base),base.angle);
  const brief=visualBrief(story());
  assert.equal(brief.title,base.headline);
  assert.equal(brief.hook,base.angle);
  assert.equal(brief.slides[0].body,base.angle);
  assert.equal(coverSubtitle({...base,angle:base.hook,reasoning:base.hook}), '');
});

test('a non-redundant approved hook stays unchanged',()=>{
  assert.equal(coverSubtitle({...base,hook:'Comprueba qué servicios vuelven a estar disponibles.'}),'Comprueba qué servicios vuelven a estar disponibles.');
});

test('X keeps all copy when it fits without the source URL, including a complete closing sentence',()=>{
  const tweet='Binance reabre los retiros por fases, empezando por Bitcoin. El alcance de la reapertura debe contrastarse con el comunicado original antes de interpretar su efecto en el mercado.';
  const result=xPublication(story({tweet}));
  assert.equal(result.text,tweet+'\n\nInformación, no asesoramiento.');
  assert.equal(result.strategy,'full');
  assert.ok(result.weightedLength<=280);
  assert.equal(new URL(result.composeUrl).searchParams.get('text'),result.text);
});

test('oversize copy becomes a complete headline, never a fragment ending in ellipsis or dangling words',()=>{
  const result=xPublication(story({tweet:'🚨 $6.3 MIL MILLONES menos y el sistema aguanta. Binance acaba de abrir los retiros por fases, empezando por Bitcoin. La gran pregunta es si la liquidez resistirá la presión de venta masiva de los usuarios. '+ 'Contexto adicional. '.repeat(10)}));
  assert.ok(result.text.startsWith(base.headline));
  assert.ok(result.weightedLength<=280);
  assert.ok(!/…|\.\.\./.test(result.text));
  assert.ok(!result.text.includes('http'));
});

test('incomplete or wholly oversize copy requires editing without losing the original or opening X',()=>{
  const huge='Textoextenso'.repeat(80);
  const result=xPublication(story({headline:huge,tweet:huge}));
  assert.equal(result.text,huge);
  assert.equal(result.ready,false);
  assert.equal(result.composeUrl,null);
  const fragment=xPublication(story({headline:'Una noticia de los...',tweet:'Esto acaba de...'}));
  assert.equal(fragment.ready,false);
  assert.equal(fragment.composeUrl,null);
});
