export const GROWTH_VERSION='editorial-growth-v2';
export const cleanText=value=>String(value||'').replace(/<[^>]*>/g,' ').replace(/[\u0000-\u001f]/g,' ').replace(/\s+/g,' ').trim();
export const normalText=value=>cleanText(value).normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
const topics=[
  [/\bbitcoin\b|\bbtc\b/i,'Bitcoin'],[/\bethereum\b|\bether\b|\beth\b/i,'Ethereum'],
  [/\bnvidia\b|\bnvda\b/i,'Nvidia'],[/\btesla\b|\btsla\b/i,'Tesla'],
  [/\bnasdaq\b/i,'Nasdaq'],[/s&p\s*500/i,'SP500'],[/\bfed\b|reserva federal|fomc/i,'Fed'],
  [/inflaci[oó]n|\bCPI\b|\bIPC\b/i,'Inflación'],[/\btrump\b/i,'Trump'],
  [/hack|phishing|ciber|breach|filtraci[oó]n/i,'Ciberseguridad']
];
export function topicTags(value,max=2) {
  return topics.filter(([pattern])=>pattern.test(value)).map(([,tag])=>'#'+tag).slice(0,max);
}
export function creativePolicy(story,platform,{contrast=false}={}) {
  let hash=2166136261;
  for(const char of String(story.id||story.source_url||story.title))hash=Math.imul(hash^char.charCodeAt(0),16777619)>>>0;
  const variant=contrast&&hash%2?'Contraste':'Dato protagonista';
  return {version:GROWTH_VERSION,platform,variant,id:'wc2-'+platform+'-'+(variant==='Contraste'?'b':'a'),
    objective:platform==='x'?'Lectura y compartidos':'Lectura de la secuencia y compartidos',
    hypotheses:['Un protagonista y un hecho visibles en la portada','Cada lámina añade información','Pregunta específica al cerrar'],
    tags:topicTags(story.title||''),researchDate:'2026-11-18'};
}
export function growthCaption(story,plan) {
  const e=typeof story.editorial==='string'?JSON.parse(story.editorial):story.editorial;
  const title=cleanText(plan.slides[0]?.title||e.headline);
  const bodies=plan.slides.slice(1).map(slide=>[cleanText(slide.title),cleanText(slide.body)].filter(Boolean).join('. ')).filter(Boolean);
  const question=cleanText(plan.slides.at(-1)?.question||e.socialQuestion);
  const tags=topicTags([title,story.title,...bodies].join(' ')).join(' ');
  return [title,...bodies,question,tags,'Fuente: '+story.source_url].filter(Boolean).join('\n\n');
}
