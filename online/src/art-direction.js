// Direction is derived from source data, not from a model's financial claims.
// Persist it in the manifest so retries never change an approved composition.
const themes = {
  amber: {accent: '#ffbf69', rule: '#fb923c', lighting: 'warm amber practical light and deep navy shadows'},
  jade: {accent: '#80ead0', rule: '#2dd4bf', lighting: 'jade reflections, charcoal shadows and soft neutral daylight'},
  violet: {accent: '#c4b5fd', rule: '#a78bfa', lighting: 'muted violet highlights, silver textures and dark plum shadows'},
  daylight: {accent: '#7dd3fc', rule: '#38bdf8', lighting: 'natural window light, warm ivory surfaces and ink-blue shadows'}
};
const scenes = {
  breach: [
    'a fractured anonymous payment card with a gold chip, translucent customer identity records and linked wallet nodes escaping as red data fragments, conceptual customer data breach',
    'anonymous payment card, house-address record and digital wallet outline linked by exposed red data paths, editorial illustration of personal information exposure',
    'a cracked customer-data vault surrounded by unbranded payment cards and anonymous identity records, editorial illustration of a data leak'
  ],
  politics: [
    'two opposing press lecterns facing each other in a divided parliamentary chamber, editorial political disagreement',
    'a split diplomatic meeting table with two empty chairs facing each other, conceptual political tension',
    'two opposing silhouettes at press microphones in a government setting, editorial political debate'
  ],
  recovery: [
    'a restored bridge of light connecting two abstract payment terminals, editorial metaphor for service resumption',
    'a close view of an anonymous operator reconnecting a fibre-optic cable at a network switch',
    'an overhead view of a circuit with a newly connected path, editorial metaphor for reopening a service'
  ],
  interruption: [
    'an elevated side view of an interrupted network path between geometric nodes, one unconnected junction',
    'a macro view of a circuit board with a single isolated connector being inspected under a work lamp',
    'an anonymous engineer at a maintenance desk examining a disconnected fibre-optic module'
  ],
  outflow: [
    'an overhead still life of blank account cards and tokens moving out along separate paths, metaphor for fund withdrawals',
    'a side view of glass channels with metallic tokens diverging from a central hub, conceptual capital flows',
    'a close view of anonymous hands sorting plain financial tokens into separate trays, conceptual withdrawals'
  ],
  security: [
    'an anonymous cybersecurity analyst inspecting a network switch through a magnifying lens',
    'a macro view of a disconnected fibre-optic connector beside a tamper-evident seal, security investigation metaphor',
    'an overhead view of a fractured abstract circuit path under an inspection light, conceptual digital vulnerability'
  ],
  deal: [
    'two anonymous executives shaking hands in a setting relevant to the industry in the source',
    'an overhead still life of two interlocking architectural models on a corporate planning desk',
    'two distinct industrial production paths meeting at one junction, conceptual business combination'
  ],
  policy: [
    'an exterior establishing view of the central bank or regulator named in the article',
    'an editorial view of the relevant central bank or regulatory institution, focusing on its setting',
    'a deliberate architectural study of the policy institution involved in the action described'
  ],
  results: [
    'a close editorial view of the named company’s product or operation mentioned in the article',
    'a panoramic view of an industry-relevant production floor with a single sharply focused foreground object',
    'an environmental photograph-style composition focused on the named company’s business activity'
  ],
  market: [
    'a close view of an industry-relevant physical subject, dramatic shallow depth of field',
    'a wide environmental view of the industry relevant to the source, with one strong foreground focal subject',
    'an editorial close-up of a concrete place, product or action explicitly described in the source'
  ]
};
const normal = text => String(text || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
function headlinePriceMove(text) {
  const headline = normal(text).replace(/^[^\p{L}\p{N}]+/u, '').replace(/\s+/g, ' ');
  const subject = /^(?:(?:el|la|las|los) )?(?:(?:precio de|acciones de|shares of|price of) )?(bitcoin|btc|ethereum|ether|eth|solana|sol|xrp|nvidia|nvda|tesla|tsla|nasdaq|s&p\s*500|gold|oro|oil|petroleo)\b(?:['’]s)?(?: (?:price|shares|stock|futures|cotizacion))?(?: (?:se|hoy|sharply|nearly|briefly|ligeramente)){0,3} /.exec(headline);
  if (!subject) return null;
  const action = headline.slice(subject[0].length);
  if (/^falls? short\b/.test(action)) return null;
  if (/^(?:slips?|falls?|drops?|tumbles?|plunges?|sinks?|slides?|retreats?|declines?|dips?|loses?|cae|baja|retrocede|desploma|hunde|cede|pierde)\b/.test(action)) return {asset: subject[1], direction: 'down'};
  if (/^(?:rises?|gains?|jumps?|surges?|rall(?:y|ies)|climbs?|rebounds?|recovers?|sube|avanza|rebota|repunta|recupera|gana)\b/.test(action)) return {asset: subject[1], direction: 'up'};
  return null;
}
function eventOf(text) {
  if (/\btrump\b|\bvance\b|\belection\w*|\belectoral\w*|\bpresident\w*|\bwhite house\b|\bcasa blanca\b|\bdiplomac\w*|\biran\b|\bwar\b|\bguerra\b/.test(text) &&
      !/\bbitcoin\b|\bbtc\b|\bethereum\b|\bcrypto\w*|\bcripto\w*/.test(text)) return 'politics';
  const recovery = /\breopen\w*|\bresum\w*|\brestor\w*|\breanud\w*|\breabr\w*|\brestablec\w*|\bopens?\b.{0,32}\bwithdrawals?\b|\babre\w*\b.{0,24}\bretiros?\b/;
  if (recovery.test(text)) {
    // A denied or merely proposed reopening must not look like a completed restoration.
    if (/\bnot\b|\bnever\b|\bno\b|\bwon.t\b|\bcannot\b|\bplans?\b|\bmay\b|\bcould\b|\bexpects?\b|\bwill\b|\bplanea\w*|\bpodria\b|\bprev[eé]\w*/.test(text)) return 'interruption';
    return 'recovery';
  }
  if(/\bbreach\b|\bfiltracion\b|data leak|\bexpone\b|\bexpos\w*/.test(text)&&/data|datos|card|tarjeta|wallet|cartera|address|domicilio/.test(text))return 'breach';
  if (/\bbug\b|\boutage\b|\bhalt\w*|\bpaus\w*|\bfrena\w*|\binterrup\w*|\bvulnerab\w*/.test(text)) return 'interruption';
  if (/\bwithdraw\w*|\boutflows?\b|\bretiros?\b|\bretiran\b|salidas? de fondos/.test(text)) return 'outflow';
  if (/\bhack\w*|\bstolen\b|\btheft\b|\bcyber\w*|\bbreach\b|\brobo\w*/.test(text)) return 'security';
  if (/\bacquir\w*|\bmerger\b|\bacquisition\b|\btakeover\b|\badquisicion\b|\bfusion\b/.test(text)) return 'deal';
  if (/federal reserve|\bfed\b|\bsec\b|central bank|interest rate|\becb\b|banco central|tipos de interes/.test(text)) return 'policy';
  if (/\bearnings\b|\brevenue\b|quarterly results|\bbeneficios\b|\bfacturacion\b|resultados trimestrales/.test(text)) return 'results';
  return null;
}
function hash(text) {
  let n = 2166136261;
  for (const c of text) n = Math.imul(n ^ c.codePointAt(0), 16777619) >>> 0;
  return n;
}
function pick(items, seed, recent) {
  const available = items.filter(item => !recent.includes(item));
  const pool = available.length ? available : items;
  return pool[seed % pool.length];
}

export function artDirection(story, recent = []) {
  // The headline takes precedence: "reopens after hack" is a recovery, not a theft scene.
  const movement = headlinePriceMove(story.title);
  const event = movement ? 'price-' + movement.direction : eventOf(normal(story.title)) || eventOf(normal(story.snippet)) || 'market';
  const seed = hash(String(story.id || story.source_url || '') + '|' + story.title);
  const prior = recent.map(brief => brief?.art).filter(Boolean).slice(0, 2);
  const choices = movement ? [
    'Make ' + movement.asset + ' the main subject. Show ' + (movement.direction === 'down' ? 'descending red candlestick shapes and a falling line' : 'ascending green candlestick shapes and a rising line') +
    ' as an illustrative market motif, with no axes, ticks, quotes or numbers. Keep background news secondary.'
  ] : scenes[event];
  const sceneIds = choices.map((_, i) => event + '-' + i);
  const sceneId = pick(sceneIds, seed, prior.map(x => x.sceneId));
  const theme = pick(Object.keys(themes), seed >>> 3, prior.map(x => x.theme));
  const layout = pick(['poster', 'masthead', 'split'], seed >>> 6, prior.map(x => x.layout));
  // Only the source's named asset may receive its own symbol. Altcoins are not Bitcoin.
  const bitcoin = event !== 'politics' && /\bbitcoin\b|\bbtc\b/i.test(story.title);
  const faceoff = event === 'politics' && /\btrump\b/.test(normal(story.title)) && /\bvance\b/.test(normal(story.title)) &&
    /critique|critic\w*|tightrope|rompe|ruptura|enfrent\w*|versus|\bvs\b|blam\w*|culp\w*|clash|rift/.test(normal(story.title+' '+story.snippet));
  return {
    event, sceneId: faceoff ? 'politics-faceoff' : sceneId, theme, layout:faceoff?'masthead':layout, ...themes[theme],
    scene: faceoff ? 'Donald Trump on the left, elderly man with swept blond hair, facing JD Vance on the right, younger man about forty with short dark brown hair and a full brown beard, recognizable editorial painted portraits, symmetrical boxing-poster-style political face-off composition, formal suits, no real fight, plain divided backdrop' : choices[Number(sceneId.split('-').at(-1))],
    ...(movement ? {priceMove: movement} : {}),
    faceoff,
    subject: bitcoin ? 'Bitcoin symbolism is appropriate only as an editorial metaphor.' :
      'Depict only the actual subject of the source. No currency, coins or unrelated financial imagery.',
    framing: faceoff ? 'Both faces fully visible within the central square, equal visual importance, no text over faces.' : layout === 'split' ? 'Asymmetric composition, subject in the upper two-thirds, rich environmental depth.' :
      layout === 'masthead' ? 'Wide environmental composition with breathing room around the focal subject.' :
        'A strong photographic focal subject with shallow depth of field.'
  };
}
