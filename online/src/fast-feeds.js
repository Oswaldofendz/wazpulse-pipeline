import {clean,publicSourceUrl} from './domain.js';
export const FAST_FEEDS=[
  {id:'fed',name:'Reserva Federal',url:'https://www.federalreserve.gov/feeds/press_monetary.xml',official:true},
  {id:'ecb',name:'BCE',url:'https://www.ecb.europa.eu/rss/press.html',official:true},
  {id:'cnbc',name:'CNBC',url:'https://www.cnbc.com/id/100003114/device/rss/rss.html'},
  {id:'coindesk',name:'CoinDesk',url:'https://www.coindesk.com/arc/outboundfeeds/rss?outputType=xml'},
  {id:'bbc-world',name:'BBC World',url:'https://feeds.bbci.co.uk/news/world/rss.xml'},
  {id:'bbc-es',name:'BBC Mundo',url:'https://feeds.bbci.co.uk/mundo/rss.xml'}
];
const normal=s=>s.normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
const decode=s=>s.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g,'$1').replace(/&#(x[0-9a-f]+|\d+);/gi,(_,v)=>{const n=v[0].toLowerCase()==='x'?parseInt(v.slice(1),16):Number(v);return n>0&&n<=0x10ffff?String.fromCodePoint(n):' ';})
  .replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&quot;/g,'"').replace(/&apos;/g,"'");
const tag=(item,name)=>decode(new RegExp('<'+name+'(?:\\s[^>]*)?>([\\s\\S]*?)</'+name+'>','i').exec(item)?.[1]||'');
export function parseFeed(xml,feed,now) {
  if(xml.length>160_000||/<!DOCTYPE|<!ENTITY/i.test(xml))return [];
  return [...xml.matchAll(/<item(?:\s[^>]*)?>([\s\S]*?)<\/item>/gi)].slice(0,30).flatMap(match=>{
    const item=match[1],title=clean(tag(item,'title'),500),url=publicSourceUrl(tag(item,'link').trim()),published=Date.parse(tag(item,'pubDate'));
    if(!url||title.length<8||!Number.isFinite(published)||published>now)return [];
    const host=new URL(url).hostname,expected=new URL(feed.url).hostname;
    const same=feed.id.startsWith('bbc')?['www.bbc.com','www.bbc.co.uk'].includes(host):host===expected;
    if(!same)return [];
    return [{title,url,published,source:feed.name,snippet:clean(tag(item,'description'),700)}];
  });
}
export function fastTopic(article,feed) {
  const title=normal(article.title);
  if(/\bopinion\b|\bforecast\b|\bprediction\b|\bwhat is\b|\bhow to\b|\bpodcast\b|\bexplicador\b|\bprediccion\b|\bpreview\b|\bexpects?\b|\bexpected\b|\bcould\b|\bmight\b|\bpreve\b|\bpodria\b/.test(title))return null;
  if(feed.official&&/fomc statement|monetary policy decisions|interest rates|tipos de interes|decision.*monetaria/.test(title))return feed.id+'-decision';
  if(/\bfed\b|federal reserve|\becb\b|\bbce\b|banco central/.test(title)&&/cuts?\b|raises?\b|holds?\b|keeps?\b|recorta\b|mantiene\b|sube\b|baja\b/.test(title)&&/rates|tipos|tasas/.test(title))return 'rates-decision';
  const dataRelease=/report|data|release|print|rise|rose|rises|falls?|fell|slow|cool|hits?|surge|jump|drop|increase|decrease|adds?|added|dips?|unexpected|sorpresa|dato|informe|sube|baja|aument|cay|cai|desaceler|aceler/.test(title);
  if(dataRelease&&/\bcpi\b|\bpce\b|inflation|inflacion|\bipc\b/.test(title))return 'inflation-data';
  if(dataRelease&&/nonfarm|non.farm|payroll|unemployment|jobless|empleo|desempleo|nominas|jobs report/.test(title))return 'employment-data';
  if(dataRelease&&/\bgdp\b|\bpib\b|gross domestic product/.test(title))return 'growth-data';
  if(/bankrupt|bank failure|bank collapse|impago|quiebra|circuit breaker|trading halt|suspende.*cotizacion|emergency rate|recorte.*emergencia/.test(title))return 'financial-emergency';
  if(/hack|breach|robo/.test(title)&&/binance|coinbase|crypto|cripto|exchange|bank|banco/.test(title))return 'security-incident';
  if(/stock|market|nasdaq|wall street|bitcoin|ethereum|crude|oil|gold|bolsa|mercado|petroleo|bonos/.test(title)&&
    /plung|crash|tumble|sell.?off|rout|surge|soar|desplom|hund|venta masiva|se dispar/.test(title))return 'market-move';
  if(/iran|taiwan|hormuz|oil|petroleo|strait/.test(title)&&/launches|strikes|attacks|invades|blocks|closes|ataca|invade|bloquea|cierra/.test(title))return 'geopolitics';
  if(/nvidia|apple|microsoft|alphabet|amazon/.test(title)&&/earnings|guidance|results|resultados/.test(title)&&/miss|cut|plung|surge|recort|hund|desplom/.test(title))return 'major-earnings';
  return null;
}
