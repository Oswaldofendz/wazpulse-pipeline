import {creativePolicy} from './growth-policy.js';
export const MARKET_STORY='market-story-v2';
export const percent=value=>(value<0?'−':'+')+Math.abs(value).toFixed(2).replace('.',',')+'%';
export const amount=value=>new Intl.NumberFormat('es-ES',{maximumFractionDigits:2}).format(value);
export const quoteUnit=row=>row.symbol==='GC=F'?'USD/oz':row.symbol==='CL=F'?'USD/barril':row.symbol.startsWith('^')?'puntos':row.currency||'USD';
const basis=row=>row.basis==='24h-ref'?'referencia de 24 h':'cierre previo';
export function marketStoryV2(snapshot,{platform='tiktok',story={}}={}) {
  if(snapshot?.version!==1||!Number.isFinite(snapshot.at)||!Array.isArray(snapshot.rows))throw Error('Invalid market snapshot');
  const rows=snapshot.rows.filter(row=>typeof row.symbol==='string'&&Number.isFinite(row.price)&&row.price>0&&Number.isFinite(row.previous)&&row.previous>0&&Number.isFinite(row.dayChange)&&Number.isFinite(row.at)&&row.at<=snapshot.at&&snapshot.at-row.at<=180000);
  const lead=rows.find(row=>row.symbol===snapshot.trigger?.symbol);
  if(!lead||!Number.isFinite(snapshot.trigger.change))throw Error('Missing market lead');
  const daily=snapshot.trigger.period==='session',change=daily?lead.dayChange:snapshot.trigger.change,negative=change<0;
  const minutes=snapshot.trigger.windowMinutes||(snapshot.trigger.period==='5m'?5:15);
  const period=daily?'vs. '+basis(lead):'en '+minutes+' min';
  const byTime=new Map();
  for(const point of lead.series||[])if(Number.isFinite(point.at)&&Number.isFinite(point.price)&&point.price>0&&point.at<=snapshot.at&&point.at<=lead.at)byTime.set(point.at,point);
  let points=[...byTime.values()].sort((a,b)=>a.at-b.at);
  if(!daily)points=points.filter(point=>point.at>=lead.at-minutes*60000);
  const other=rows.filter(row=>row.symbol!==lead.symbol&&basis(row)===basis(lead)).slice(0,2);
  const policy=creativePolicy(story,platform,{contrast:other.length>0});
  const subject=lead.short||lead.name,headline=policy.variant==='Contraste'?lead.name+': ¿y el resto?':lead.name+(negative?(change<=-5?' se desploma':' retrocede'):(change>=5?' se dispara':' avanza'));
  const lineSlide={label:'EL RECORRIDO',title:subject+': el precio al corte',body:points.length>=2?(daily?'Precios del tramo observado. La portada compara con '+(lead.basis==='24h-ref'?'la referencia de 24 h':'el cierre previo')+'.':'Recorrido observado en '+minutes+' min. La portada mide ese intervalo.'):'Una cotización al corte. El recorrido todavía no está disponible.',
    metric:amount(lead.price),unit:quoteUnit(lead),visual:points.length>=2?'market-line':'market-price',negative,points,asset:subject,period,source:lead.source};
  const cover={label:'EL MOVIMIENTO',title:headline,body:subject+' '+percent(change)+' '+period+'.',metric:percent(change),visual:'market-hero',negative,points,asset:subject,period,source:lead.source,price:amount(lead.price),unit:quoteUnit(lead),...(daily&&points.length>=2?{reference:{price:amount(lead.previous),label:lead.basis==='24h-ref'?'REFERENCIA 24 H':'CIERRE PREVIO'}}:{})};
  let closing;
  if(other.length){
    const compared=[lead,...other],weakest=[...compared].sort((a,b)=>a.dayChange-b.dayChange)[0],distinct=compared.filter(row=>Math.round(row.dayChange*100)===Math.round(weakest.dayChange*100)).length===1;
    closing={label:'LA COMPARACIÓN',title:weakest.dayChange<0&&distinct?weakest.name+' cayó más':'Así se movió el resto',body:'Cambio diario de estos activos frente a '+basis(lead)+'. Misma referencia, misma escala.',
      visual:'market-compare',negative,rows:compared.map(row=>({name:row.short||row.name,value:row.dayChange,basis:basis(row)})),question:'¿Qué activo de esta comparación sigues?',period:'Cambio diario · '+basis(lead)};
  } else if(points.length>=2) {
    const min=Math.min(...points.map(point=>point.price)),max=Math.max(...points.map(point=>point.price));
    closing={label:'LOS EXTREMOS DEL TRAMO',title:subject+': mínimo y máximo',body:'Extremos de las muestras observadas. Este rango no equivale al cambio diario.',metric:((max-min)/min*100).toFixed(2).replace('.',',')+'%',unit:'de amplitud',priceUnit:quoteUnit(lead),visual:'market-range',negative,low:amount(min),high:amount(max),asset:subject,question:'¿Qué dato necesitas para explicar este movimiento?',period:'Solo el tramo observado'};
  } else closing={label:'EL PUNTO DE PARTIDA',title:'La referencia de '+subject,body:'El cambio diario usa '+basis(lead)+'. El movimiento de precio, por sí solo, no explica su causa.',metric:amount(lead.previous),unit:quoteUnit(lead),visual:'market-reference',negative,asset:subject,question:'¿Qué dato falta para explicar este movimiento?',period:basis(lead)};
  return {slides:[cover,lineSlide,closing],quality:{ready:true,issues:[]},growth:policy,presentation:MARKET_STORY};
}
