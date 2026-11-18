export const esc=value=>String(value??'').replace(/[&<>"']/g,char=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
const money=value=>new Intl.NumberFormat('es-ES',{maximumFractionDigits:Math.abs(value)<1?6:Math.abs(value)<100?4:2}).format(value);
const pct=value=>(value<0?'−':'+')+Math.abs(value).toFixed(2).replace('.',',')+'%';
export function referenceStep(slide) {
  const color=slide.negative?'#ff335a':'#50edbc';
  if(!slide.reference)return `<svg viewBox="0 0 1000 440" role="img" aria-label="Activo y dirección del movimiento"><rect x="55" y="45" width="890" height="340" rx="32" fill="#160d1c" stroke="${color}" stroke-width="3"/><text x="110" y="250" fill="#fff7f2" font-family="Arial" font-size="128" font-weight="900">${esc(slide.asset)}</text><path d="${slide.negative?'M650 122L810 290m-85 0h85v-85':'M650 290L810 122m-85 0h85v85'}" stroke="${color}" stroke-width="26" stroke-linecap="round" stroke-linejoin="round" fill="none"/></svg>`;
  return `<svg viewBox="0 0 1000 440" role="img" aria-label="Comparación entre la referencia y el precio al corte"><g font-family="Arial"><rect x="25" y="42" width="950" height="340" rx="28" fill="#100b1580" stroke="${color}70" stroke-width="2"/><text x="75" y="120" fill="#c9bdc9" font-size="30">${esc(slide.reference.label)}</text><text x="75" y="185" fill="#fff7f2" font-size="63" font-weight="900">${esc(slide.reference.price)}</text><path d="M440 182L560 263m-45 0h45v-45" fill="none" stroke="${color}" stroke-width="14" stroke-linecap="round" stroke-linejoin="round" transform="${slide.negative?'':'translate(0 445) scale(1 -1)'}"/><text x="610" y="255" fill="#c9bdc9" font-size="30">AL CORTE</text><text x="610" y="320" fill="${color}" font-size="63" font-weight="900">${esc(slide.price)}</text><text x="75" y="336" fill="#bba8bc" font-size="26">${esc(slide.unit)}</text></g></svg>`;
}
export function priceLine(points,{negative=true,compact=false}={}) {
  if(!Array.isArray(points)||points.length<2)return '<div class="waiting">El recorrido aparecerá al reunir más muestras.</div>';
  const values=points.map(p=>p.price),min=Math.min(...values),max=Math.max(...values),start=points[0].at,end=points.at(-1).at;
  if(end<=start||!values.every(Number.isFinite))throw Error('Invalid chart points');
  const pad=Math.max((max-min)*.12,max*.00001),low=min-pad,high=max+pad;
  const font=compact?31:35,left=Math.min(320,Math.max(190,Math.max(...[min,max].map(value=>money(value).length))*font*.6+30));
  const x=at=>left+(at-start)/(end-start)*(935-left),y=value=>54+(high-value)/(high-low)*290;
  const coords=points.map(p=>x(p.at).toFixed(1)+','+y(p.price).toFixed(1)).join(' ');
  const color=negative?'#ff335a':'#50edbc',clock=at=>new Date(at).toISOString().slice(11,16);
  const ticks=[max,(max+min)/2,min].map(value=>'<line x1="'+left+'" x2="935" y1="'+y(value)+'" y2="'+y(value)+'" stroke="#ffffff20"/><text x="'+(left-18)+'" y="'+(y(value)+9)+'" text-anchor="end">'+money(value)+'</text>').join('');
  return `<svg class="price-line" viewBox="0 0 1000 440" role="img" aria-label="Precios observados, eje vertical ajustado"><defs><linearGradient id="chart-fill" x1="0" y1="0" x2="0" y2="1"><stop stop-color="${color}" stop-opacity=".42"/><stop offset="1" stop-color="${color}" stop-opacity="0"/></linearGradient></defs><g fill="#d4c7d2" font-family="Arial" font-size="${font}">${ticks}</g><polygon points="${left},367 ${coords} 935,367" fill="url(#chart-fill)"/><polyline points="${coords}" fill="none" stroke="${color}" stroke-width="10" stroke-linejoin="round" stroke-linecap="round"/><circle cx="935" cy="${y(points.at(-1).price)}" r="10" fill="${color}" stroke="#fff" stroke-width="3"/><g fill="#c9bdc9" font-family="Arial" font-size="28"><text x="${left}" y="412">${clock(start)} UTC</text><text x="935" y="412" text-anchor="end">${clock(end)} UTC</text></g></svg>`;
}
export function comparisonBars(rows) {
  if(!Array.isArray(rows)||!rows.length||rows.length>3||rows.some(row=>!Number.isFinite(row.value)))throw Error('Invalid comparison');
  const limit=Math.max(.1,...rows.map(row=>Math.abs(row.value))),height=rows.length*145+58;
  return `<svg class="comparison-bars" viewBox="0 0 1000 ${height}" role="img" aria-label="Variación porcentual con escala común y referencia cero"><g font-family="Arial">${rows.map((row,i)=>{
    const cy=66+i*145,length=Math.abs(row.value)/limit*252,negative=row.value<0,color=negative?'#ff335a':'#50edbc';
    return '<text x="28" y="'+cy+'" fill="#fff7f2" font-weight="900" font-size="52">'+esc(row.name)+'</text><text x="28" y="'+(cy+36)+'" fill="#b9abbc" font-size="23">'+esc(row.basis)+'</text><line x1="567" x2="567" y1="'+(cy-38)+'" y2="'+(cy+41)+'" stroke="#fff7f244" stroke-width="2"/><rect x="'+(negative?567-length:567)+'" y="'+(cy-30)+'" width="'+length+'" height="54" rx="10" fill="'+color+'"/><text x="966" y="'+(cy+11)+'" text-anchor="end" fill="'+color+'" font-size="59" font-weight="900">'+pct(row.value)+'</text>';
  }).join('')}<text x="567" y="${height-8}" text-anchor="middle" font-size="25" fill="#c9bdc9">0%</text></g></svg>`;
}
