export const MARKET_ASSETS=[
  {symbol:'^GSPC',name:'S&P 500',short:'S&P 500',session:'us',five:.8,fifteen:1.5,day:3},
  {symbol:'^IXIC',name:'Nasdaq Composite',short:'Nasdaq',session:'us',five:1,fifteen:1.8,day:4},
  {symbol:'^DJI',name:'Dow Jones',short:'Dow',session:'us',five:.8,fifteen:1.5,day:3},
  {symbol:'BTC-USD',name:'Bitcoin',short:'BTC',session:'crypto',five:2,fifteen:3,day:5,coinbase:'BTC-USD'},
  {symbol:'ETH-USD',name:'Ethereum',short:'ETH',session:'crypto',five:2,fifteen:3,day:7,coinbase:'ETH-USD'},
  {symbol:'^STOXX50E',name:'Euro Stoxx 50',short:'Euro Stoxx',session:'eu',five:.8,fifteen:1.5,day:3},
  {symbol:'^N225',name:'Nikkei 225',short:'Nikkei',session:'jp',five:1,fifteen:1.8,day:4},
  {symbol:'GC=F',name:'Oro (futuro)',short:'Oro',session:'future',five:1,fifteen:1.5,day:3},
  {symbol:'CL=F',name:'WTI (futuro)',short:'WTI',session:'future',five:2,fifteen:3,day:5},
  {symbol:'^VIX',name:'VIX',short:'VIX',session:'us',five:12,fifteen:18,day:25,inverse:true}
];
export const pct=(a,b)=>(a/b-1)*100;
export const formatPct=n=>(n<0?'−':'+')+Math.abs(n).toFixed(2).replace('.',',')+'%';
export const quoteUrl=symbol=>'https://finance.yahoo.com/quote/'+encodeURIComponent(symbol)+'/';
const clocks=new Map();
function local(now,zone) {
  if(!clocks.has(zone))clocks.set(zone,new Intl.DateTimeFormat('en-GB',{timeZone:zone,weekday:'short',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}));
  const parts=clocks.get(zone).formatToParts(now);
  const get=t=>parts.find(x=>x.type===t)?.value;
  return {weekday:get('weekday'),minute:Number(get('hour'))*60+Number(get('minute'))};
}
export function marketOpen(asset,now) {
  if(asset.session==='crypto')return true;
  const zone=asset.session==='eu'?'Europe/Berlin':asset.session==='jp'?'Asia/Tokyo':'America/New_York';
  const {weekday,minute}=local(now,zone);
  if(asset.session==='future')return weekday==='Sun'?minute>=18*60:weekday==='Sat'?false:weekday==='Fri'?minute<17*60:minute<17*60||minute>=18*60;
  if(['Sat','Sun'].includes(weekday))return false;
  if(asset.session==='us')return minute>=570&&minute<=965;
  if(asset.session==='eu')return minute>=540&&minute<=1060;
  return minute>=540&&minute<=935&&!(minute>=690&&minute<750);
}
export function quoteFromYahoo(raw,asset,now) {
  const result=raw?.chart?.result?.[0],m=result?.meta;
  const price=m?.regularMarketPrice,previous=m?.chartPreviousClose,at=m?.regularMarketTime*1000;
  if(!m||m.symbol!==asset.symbol||!Number.isFinite(price)||price<=0||!Number.isFinite(previous)||previous<=0||
    !Number.isFinite(at)||at>now+30_000||now-at>180_000||Number(m.exchangeDataDelayedBy||0)>2||!marketOpen(asset,now))return null;
  return {symbol:asset.symbol,name:asset.name,short:asset.short,price,previous,at,receivedAt:now,dayChange:pct(price,previous),
    currency:asset.symbol.startsWith('^')?'ptos.':asset.symbol==='GC=F'?'USD/oz':asset.symbol==='CL=F'?'USD/bbl':typeof m.currency==='string'?m.currency.slice(0,8):'',inverse:Boolean(asset.inverse),basis:'close',source:'Yahoo Finance',url:quoteUrl(asset.symbol)};
}
export function appendQuote(prior,quote,now) {
  const compatible=!prior?.quote||(prior.quote.source===quote.source&&prior.quote.basis===quote.basis);
  const history=(compatible?prior?.history||[]:[]).filter(p=>p.at>=now-2*3_600_000&&p.at<quote.at&&Number.isFinite(p.price));
  if(prior?.quote?.at>quote.at)return prior;
  history.push({at:quote.at,price:quote.price,dayChange:quote.dayChange});
  return {quote,history:history.slice(-125)};
}
export function marketTrigger(asset,state,now,last=null) {
  const q=state?.quote,h=state?.history||[];
  if(!q||now-q.at>180_000||h.length<2||q.at-h.at(-2).at>180_000)return null;
  const changes=[];
  for(const [period,minutes,threshold] of [['5m',5,asset.five],['15m',15,asset.fifteen]]) {
    const base=h.filter(p=>p.at<=q.at-minutes*60_000&&p.at>=q.at-(minutes+2)*60_000).at(-1);
    if(base){const change=pct(q.price,base.price);if(Math.abs(change)>=threshold)changes.push({period,change,threshold,windowMinutes:Math.round((q.at-base.at)/60_000)});}
  }
  const prev=h.at(-2),day=Math.abs(q.dayChange);
  // Bootstrap is explicitly an observed level, not a claim that it happened now.
  if(day>=asset.day&&(!last||Math.abs(prev.dayChange)<asset.day||Math.floor(day/asset.day)>Math.floor(Math.abs(last.dayChange||0)/asset.day)))
    changes.push({period:'session',change:q.dayChange,threshold:asset.day});
  if(!changes.length)return null;
  const selected=changes.sort((a,b)=>Math.abs(b.change/b.threshold)-Math.abs(a.change/a.threshold))[0];
  const direction=(selected.change<0?-1:1)*(asset.inverse?-1:1);
  if(last&&last.direction===direction) {
    const elapsed=now-last.at,escalated=Math.abs(q.dayChange)-Math.abs(last.dayChange||0)>=2;
    if(elapsed<45*60_000&&!(elapsed>=10*60_000&&escalated))return null;
    if(selected.period==='session'&&!escalated&&day<2*Math.abs(last.dayChange||0))return null;
  }
  return {...selected,symbol:asset.symbol,name:asset.name,direction,score:Math.abs(selected.change/selected.threshold)};
}
export function marketEditorial(snapshot) {
  const t=snapshot.trigger,lead=snapshot.rows.find(r=>r.symbol===t.symbol),clock=new Date(snapshot.at).toISOString().slice(11,16);
  const us=snapshot.rows.filter(r=>['^GSPC','^IXIC','^DJI'].includes(r.symbol)),crypto=snapshot.rows.filter(r=>['BTC-USD','ETH-USD'].includes(r.symbol));
  const broad=us.length>=2&&crypto.length>=1&&us.every(r=>r.dayChange<0)&&crypto.every(r=>r.dayChange<0)&&t.direction<0;
  const heading=broad?'🚨 BOLSA Y CRIPTO EN ROJO':t.symbol==='^VIX'?'🚨 MOVIMIENTO FUERTE DEL VIX':t.direction<0?'🚨 VENTA FUERTE EN '+lead.name.toUpperCase():'🚨 FUERTE SUBIDA EN '+lead.name.toUpperCase();
  const detail=t.period==='session'?lead.short+' '+formatPct(lead.dayChange)+(lead.basis==='24h-ref'?' frente a su referencia de 24 h.':' frente al cierre previo.'):
    lead.short+' '+formatPct(t.change)+' en '+(t.windowMinutes||(t.period==='5m'?5:15))+' minutos.';
  const context=snapshot.rows.filter(r=>r.symbol!==lead.symbol&&['^GSPC','^IXIC','BTC-USD','ETH-USD'].includes(r.symbol)).slice(0,2).map(r=>r.short+' '+formatPct(r.dayChange)+(r.basis==='24h-ref'?' (ref.24h)':'')).join(' · ');
  const tweet=[heading+'.',detail,context?context+'.':'','Corte '+clock+' UTC. Datos intradía; cierre previo salvo ref.24h indicada.'].filter(Boolean).join('\n\n');
  return {strength:5,provider:'cotizaciones',headline:heading+': '+detail,hook:'Cotizaciones observadas a las '+clock+' UTC.',angle:detail,
    reasoning:'El aviso se activa por un umbral de precio y no atribuye una causa a la variación. '+
      (t.period==='session'?'Se detectó un nivel ya alcanzado; no acredita que la caída comenzara en este minuto. ':'')+
      'Datos de '+[...new Set(snapshot.rows.map(r=>r.source))].join(' y ')+(snapshot.confirmation?' con contraste de la cotización cripto en '+snapshot.confirmation.source+'.':'.')+' Revisión de fecha, precios e imagen antes de publicar.',
    tweet,instagram:tweet,marketSnapshot:snapshot};
}
