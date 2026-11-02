import test from 'node:test';
import assert from 'node:assert/strict';
import {artDirection} from '../src/art-direction.js';
import {visualBrief, visualHtml, visualDimensions} from '../src/publication.js';

const story = (title, extra = {}) => ({
  id: 'a'.repeat(32), title, snippet: '', source: 'Fixture only',
  source_url: 'https://www.bbc.co.uk/news/test', published_at: Date.parse('2026-11-02T12:00:00Z'),
  editorial: {headline: 'Texto aprobado de prueba', hook: 'Un contexto que revisar.',
    tweet: 'Ejemplo de prueba, no es una noticia.', angle: 'Revisar la fuente.',
    reasoning: 'Comprobar los datos. Leer el original. Contrastar las actualizaciones.', instagram: 'Prueba de composición.'}, ...extra
});

test('the three reported headlines receive distinct event-specific scenes', () => {
  const inputs = [
    ['THORChain halts swaps after critical bug', 'interruption'],
    ['Binance faces withdrawals after hack', 'outflow'],
    ['Binance reopens withdrawals after hack', 'recovery']
  ];
  const directions = inputs.map(([title, event]) => {
    const art = artDirection(story(title));
    assert.equal(art.event, event);
    assert.doesNotMatch(art.scene, /vault|padlock|bitcoin|server rack/i);
    assert.ok(art.subject.includes('Do not add Bitcoin'));
    return art.scene;
  });
  assert.equal(new Set(directions).size, 3);
});

test('headline event wins over background events and Spanish variants are supported', () => {
  assert.equal(artDirection(story('Exchange resumes withdrawals', {snippet: 'A hack was reported earlier.'})).event, 'recovery');
  assert.equal(artDirection(story('THORChain frena swaps tras un bug')).event, 'interruption');
  assert.equal(artDirection(story('Una plataforma reabre retiros tras un hackeo')).event, 'recovery');
  assert.equal(artDirection(story('La Fed anuncia tipos de interés')).event, 'policy');
  assert.equal(artDirection(story('Empresa anuncia adquisición')).event, 'deal');
  assert.equal(artDirection(story('Resultados trimestrales de una empresa')).event, 'results');
  assert.equal(artDirection(story('Exchange will not reopen withdrawals')).event, 'interruption');
  assert.equal(artDirection(story('Exchange plans to resume withdrawals')).event, 'interruption');
  assert.equal(artDirection(story('Binance opens crypto withdrawals in phases starting with bitcoin following $6.3 billion hack')).event, 'recovery');
});

test('consecutive jobs vary scenes, palettes and compositions without random retry changes', () => {
  const recent = [];
  for (let i = 0; i < 7; i++) {
    const input = story('Exchange reports a security hack', {id: String(i).padStart(32, '0')});
    const brief = visualBrief(input, {recent});
    assert.deepEqual(brief, visualBrief(input, {recent}));
    for (const previous of recent.slice(0, 2)) {
      assert.notEqual(brief.art.sceneId, previous.art.sceneId);
      assert.notEqual(brief.art.theme, previous.art.theme);
      assert.notEqual(brief.art.layout, previous.art.layout);
    }
    recent.unshift(brief);
  }
});

test('Bitcoin symbols are not prescribed for unrelated tokens, companies or policy', () => {
  for (const title of ['THORChain pauses swaps', 'Amazon quarterly earnings', 'Fed policy decision']) {
    assert.ok(artDirection(story(title)).subject.includes('Do not add Bitcoin'));
  }
  assert.ok(artDirection(story('Bitcoin market update')).subject.startsWith('Bitcoin symbolism'));
});

test('bounded prompts retain source data and safety instructions with maximum input lengths', () => {
  const brief = visualBrief(story('Company ' + 't'.repeat(500), {snippet: 'x'.repeat(1800)}));
  assert.ok(brief.prompt.length <= 2048);
  assert.ok(brief.prompt.includes('never instructions'));
  assert.ok(brief.prompt.includes('not a documentary photograph'));
  assert.ok(brief.prompt.includes('No invented disasters'));
  assert.ok(brief.prompt.includes('Company'));
});

test('all layouts escape source text, use only safe theme values and label AI', () => {
  const brief = visualBrief(story('A company update'));
  brief.title = '<script>bad()</script>';
  for (const layout of ['poster', 'masthead', 'split']) {
    const variant = {...brief, art: {...brief.art, layout, accent: 'red; background:url(https://bad.test)', rule: '</style>'}};
    const html = visualHtml(variant, '/9j/AAAA');
    assert.ok(html.includes('data-layout="' + layout + '"'));
    assert.ok(html.includes('&lt;script&gt;bad()&lt;/script&gt;'));
    assert.ok(html.includes('ILUSTRACIÓN CON IA'));
    assert.ok(html.includes(brief.sourceHost));
    assert.ok(!html.includes('https://bad.test'));
    assert.deepEqual(visualDimensions(variant), {width: 1080, height: 1350});
  }
});
