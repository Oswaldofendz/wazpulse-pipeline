import {escapeHtml as esc} from './publication.js';
import {socialIllustration} from './social-layout.js';

const pct=value=>(value<0?'−':'+')+Math.abs(value).toFixed(2).replace('.',',')+'%';

function coin(asset,negative) {
  const color=negative?'#ff365e':'#59f2bb';
  if(!['BTC','ETH'].includes(asset))return `<svg viewBox="0 0 800 420" role="img" aria-label="Ilustración de ${esc(asset)}"><rect x="60" y="25" width="680" height="345" rx="28" fill="#17141e" stroke="${color}" stroke-width="6"/><text x="400" y="135" text-anchor="middle" fill="#fff7f2" font-family="Arial" font-size="60" font-weight="900">${esc(asset)}</text><path d="${negative?'M200 180L350 240 470 215 600 310m-70 0h70v-70':'M200 310L350 250 470 275 600 180m-70 0h70v70'}" fill="none" stroke="${color}" stroke-width="23" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
  const symbol=asset==='BTC'?'₿':'Ξ';
  return `<svg viewBox="0 0 800 420" role="img" aria-label="Ilustración de ${esc(asset)}"><defs><radialGradient id="metal"><stop stop-color="#ffd99c"/><stop offset=".65" stop-color="#e39e42"/><stop offset="1" stop-color="#6e3514"/></radialGradient><filter id="glow"><feGaussianBlur stdDeviation="23"/></filter></defs><ellipse cx="340" cy="290" rx="210" ry="60" fill="${color}" opacity=".3" filter="url(#glow)"/><g transform="rotate(-14 340 210)"><ellipse cx="360" cy="217" rx="158" ry="170" fill="#643013"/><ellipse cx="338" cy="199" rx="158" ry="170" fill="url(#metal)" stroke="#ffdaa2" stroke-width="5"/><ellipse cx="338" cy="199" rx="135" ry="146" fill="none" stroke="#b56d26" stroke-width="8"/><text x="338" y="251" text-anchor="middle" fill="#81400e" font-family="Arial" font-size="${symbol.length>2?68:175}" font-weight="900">${esc(symbol)}</text></g><path d="${negative?'M530 95L610 172 666 151 747 258m-80 0h80v-80':'M530 258L610 181 666 202 747 95m-80 0h80v80'}" fill="none" stroke="${color}" stroke-width="24" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
}

function line(points,negative) {
  if(!points||points.length<2)return '';
  const values=points.map(p=>p.price),low=Math.min(...values),high=Math.max(...values);
  if(high<=low)return '';
  const color=negative?'#ff4c6d':'#59f2bb';
  const start=points[0].at,end=points.at(-1).at;
  if(end<=start)return '';
  const coordinates=points.map(p=>(26+(p.at-start)/(end-start)*730).toFixed(1)+','+(48+(high-p.price)/(high-low)*215).toFixed(1)).join(' ');
  const price=value=>new Intl.NumberFormat('es-ES',{maximumFractionDigits:2}).format(value);
  const time=at=>new Date(at).toISOString().slice(11,16);
  return `<svg viewBox="0 0 790 360" role="img" aria-label="Cotizaciones observadas"><defs><linearGradient id="fade" x2="0" y2="1"><stop stop-color="${color}" stop-opacity=".3"/><stop offset="1" stop-color="${color}" stop-opacity="0"/></linearGradient></defs><path d="M26 48H756M26 155H756M26 263H756" stroke="#ffffff24"/><polygon points="26,310 ${coordinates} 756,310" fill="url(#fade)"/><polyline points="${coordinates}" fill="none" stroke="${color}" stroke-width="9" stroke-linejoin="round" stroke-linecap="round"/><g font-family="Arial" font-size="27" fill="#bcc4d1"><text x="26" y="27">${price(high)}</text><text x="26" y="301">${price(low)}</text><text x="26" y="350">${time(start)} UTC</text><text x="756" y="350" text-anchor="end">${time(end)} UTC</text></g></svg>`;
}

function comparisons(rows) {
  const max=Math.max(...rows.map(row=>Math.abs(row.value)),.1);
  return `<div class="comparisons">${rows.map(row=>`<section><div class="row"><b>${esc(row.name)}<small>${esc(row.basis)}</small></b><strong style="color:${row.value<0?'#ff4c6d':'#59f2bb'}">${pct(row.value)}</strong></div><div class="track"><i style="width:${Math.max(2,Math.abs(row.value)/max*100)}%;background:${row.value<0?'#ff4c6d':'#59f2bb'}"></i></div></section>`).join('')}</div>`;
}

export function tiktokStoryHtml(kit,image,index) {
  const slide=kit.slides[index];
  if(!slide||!Number.isInteger(index)||typeof image!=='string'||!/^[A-Za-z0-9+/=]+$/.test(image)||image.length>2_400_000)throw Error('Invalid TikTok asset');
  const market=slide.visual.startsWith('market-');
  const accent=market?(slide.negative===false?'#59f2bb':'#ff4569'):index===1?'#edff82':'#ff4569';
  let art;
  if(slide.visual==='market-hero')art=coin(slide.asset,slide.negative);
  else if(slide.visual==='market-line')art=line(slide.points,slide.negative)||coin(slide.asset,slide.negative);
  else if(slide.visual==='market-compare')art=comparisons(slide.rows);
  else if(slide.visual==='market-reference')art=coin(slide.asset||'REF',false);
  else if(index===0)art=`<img src="data:image/jpeg;base64,${image}" alt="Ilustración editorial de la noticia">`;
  else art=socialIllustration(slide.visual,index,slide.title+' '+slide.body);
  const metric=slide.metric?`<div class="metric fit" data-min="57"><strong>${esc(slide.metric)}</strong>${slide.unit?'<small>'+esc(slide.unit)+'</small>':''}</div>`:'';
  const headlineHeight=slide.metric?196:266;
  const sourceTime=kit.marketSnapshot?.at||Date.parse(kit.source.publishedAt);
  const stamp=new Date(sourceTime).toLocaleDateString('es-ES',{timeZone:'UTC',day:'2-digit',month:'short'})+' · '+new Date(sourceTime).toISOString().slice(11,16)+' UTC';
  const credit=market?'Datos al corte':index===0?'Ilustración con IA':'Ilustración editorial';
  const last=index===kit.slides.length-1;
  const cue=last?(slide.question||'¿Qué dato te parece más relevante?'):(index===0?'Desliza para ver el dato →':'Sigue con el contexto →');
  return `<!doctype html><html lang="es"><head><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; img-src data:; style-src 'unsafe-inline'; script-src 'unsafe-inline'"><style>
  *{box-sizing:border-box}html,body{margin:0;width:1080px;height:1920px;overflow:hidden;color:#fff7f2;background:#0b0b12;font-family:Arial,Helvetica,sans-serif}body{background:radial-gradient(ellipse at 68% 47%,${accent==='#59f2bb'?'#123f31':accent==='#edff82'?'#283119':'#52102a'} 0,#0b0b12 65%)}body:before{content:"";position:absolute;left:-100px;right:-100px;top:840px;height:520px;background:linear-gradient(115deg,${accent}15,transparent);transform:rotate(-8deg)}
  main{position:absolute;left:76px;top:420px;width:792px;height:1090px}header{height:36px;display:flex;align-items:center;justify-content:space-between;font-size:23px;font-weight:900;letter-spacing:2px;color:${accent}}.brand{padding-left:80px;font-size:22px}.headline{position:absolute;top:63px;height:${headlineHeight}px;width:792px}h1{margin:0;font-size:${slide.metric?83:95}px;line-height:1.01;font-weight:900;letter-spacing:-3px;text-wrap:balance;overflow-wrap:break-word}.metric{position:absolute;top:278px;height:151px;width:792px;display:flex;align-items:baseline;gap:16px;color:${accent}}.metric strong{font-size:${slide.metric?.length>8?97:139}px;line-height:1;font-weight:900;letter-spacing:-6px}.metric small{font-size:31px;font-weight:800;letter-spacing:0}
  .visual{position:absolute;top:${slide.metric?443:345}px;width:840px;left:-24px;height:${slide.metric?363:470}px;display:flex;align-items:center;justify-content:center}.visual>svg,.visual>img{width:100%;height:100%;object-fit:cover;object-position:center;border-radius:24px}.visual img{mask-image:linear-gradient(transparent,#000 10%,#000 86%,transparent)}.comparisons{width:100%;padding:12px 30px;display:grid;gap:42px}.row{display:flex;justify-content:space-between;gap:20px;font-size:49px}.row strong{font-size:62px}.row small{display:block;font-size:22px;color:#bcb2bf;margin-top:3px}.track{height:19px;background:#ffffff16;border-radius:20px;overflow:hidden;margin-top:12px}.track i{display:block;height:100%;border-radius:20px}
  .body{position:absolute;top:835px;width:792px;height:128px}.body p{margin:0;font-size:37px;line-height:1.19;color:#e3d9e1;text-wrap:pretty;overflow-wrap:break-word}.cue{position:absolute;top:980px;width:792px;height:62px;color:${accent};font-size:${last?29:26}px;line-height:1.12;font-weight:800}.footer{position:absolute;top:1050px;display:flex;justify-content:space-between;align-items:center;width:792px;font-size:19px;color:#aaa8b6}.footer strong{font-size:24px;color:${accent}}.kicker{display:none}
  </style></head><body><main><header><span class="brand">WACAPITAL</span><span>${index+1}/${kit.slides.length}</span></header><div class="headline fit" data-min="52"><h1>${esc(slide.title)}</h1></div>${metric}<div class="visual">${art}</div><div class="body fit" data-min="31"><p>${esc(slide.body)}</p></div><div class="cue fit" data-min="23"><span>${esc(cue)}</span></div><div class="footer"><span>${esc(stamp)} · ${credit}</span><strong>${index+1}/${kit.slides.length}</strong></div></main>
  <script>document.fonts.ready.then(()=>{for(const box of document.querySelectorAll('.fit')){const node=box.firstElementChild;let size=parseFloat(getComputedStyle(node).fontSize);const min=Number(box.dataset.min);while((box.scrollHeight>box.clientHeight||box.scrollWidth>box.clientWidth)&&size>min){size=Math.max(min,size-.5);node.style.fontSize=size+'px';}}document.documentElement.dataset.fits=String([...document.querySelectorAll('.fit')].every(box=>box.scrollHeight<=box.clientHeight&&box.scrollWidth<=box.clientWidth));});</script></body></html>`;
}
