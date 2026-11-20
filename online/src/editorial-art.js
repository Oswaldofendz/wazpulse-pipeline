import {esc} from './data-visuals.js';
import {newsSubjects} from './news-subject.js';

// Original vector artwork, not a market-data chart or a generated photograph.
// Its subject and direction come from the source headline; no prices are invented.
export function editorialArt(brief,{index=0,text=''}={}){
  const art=brief.art||{},subjects=art.subjects?.length?art.subjects:newsSubjects(brief.sourceTitle||brief.title),lead=subjects[0];
  const down=art.priceMove?.direction!=='up',color=art.priceMove?(down?'#ff345a':'#46e7ac'):'#69c7ff';
  const event=art.event||'market',kind=lead?.kind||'news',label=lead?.label||'ACTUALIDAD';
  let figure='';
  if(art.priceMove&&!['coin','gold'].includes(kind)){
    figure='<rect x="110" y="205" width="400" height="235" rx="32" fill="#14263c" stroke="'+color+'" stroke-width="7"/><text x="310" y="345" text-anchor="middle" fill="white" font-family="Arial" font-size="52" font-weight="900">'+esc(label)+'</text><path d="'+(down?'M580 170L700 290 770 255 900 425m-100 0h100v-100':'M580 425L700 305 770 340 900 170m-100 0h100v100')+'" fill="none" stroke="'+color+'" stroke-width="36" stroke-linecap="round" stroke-linejoin="round"/>';
  }else if(kind==='coin'||kind==='gold'){
    const symbol=lead?.label==='BTC'?'₿':lead?.label==='ETH'?'Ξ':lead?.label||'MERCADO';
    const focus=!art.priceMove?'<g fill="#273f5a" stroke="#7fabc9" stroke-width="7"><path d="M590 200L800 280 640 420" fill="none"/><circle cx="590" cy="200" r="50"/><circle cx="800" cy="280" r="65"/><circle cx="640" cy="420" r="50"/></g>':index%3===1&&/tasas?|rates?|fed|etf/i.test(text)?'<g transform="translate(560 175)" fill="#111c2b" stroke="#9ebbd2" stroke-width="5"><path d="M0 80L150 0 300 80Z"/><path d="M25 90v170m80-170v170m90-170v170m80-170v170M0 270h300"/><text x="150" y="330" text-anchor="middle" font-family="Arial" font-size="42" fill="#eef3f8" stroke="none">'+(/etf/i.test(text)?'ETF':'TASAS')+'</text></g>':
      '<path d="'+(down?'M540 170L650 260 730 230 900 420m-100 0h100v-100':'M540 420L650 330 730 360 900 170m-100 0h100v100')+'" fill="none" stroke="'+color+'" stroke-width="38" stroke-linecap="round" stroke-linejoin="round"/>';
    figure='<ellipse cx="375" cy="460" rx="230" ry="45" fill="#0006"/><g transform="rotate(-12 350 320)"><ellipse cx="370" cy="319" rx="174" ry="194" fill="#78401b"/><ellipse cx="346" cy="308" rx="174" ry="194" fill="url(#metal)" stroke="#ffdf99" stroke-width="7"/><ellipse cx="346" cy="308" rx="150" ry="169" fill="none" stroke="#9c6422" stroke-width="10"/><text x="346" y="'+(symbol.length>3?330:373)+'" text-anchor="middle" font-family="Arial" font-weight="900" font-size="'+(symbol.length>3?52:190)+'" fill="#65320e">'+esc(symbol)+'</text></g>'+focus;
  }else if(event==='negotiation'||event==='politics'||kind==='politics'){
    const names=subjects.filter(s=>s.kind==='politics').slice(0,3);
    figure='<ellipse cx="500" cy="420" rx="340" ry="100" fill="#1e344f" stroke="#6da5d1" stroke-width="6"/><path d="M250 420v100m500-100v100M260 265h150m180 0h150" stroke="#72bde9" stroke-width="14"/><g fill="#476689"><circle cx="325" cy="205" r="50"/><path d="M250 335v-35c0-95 150-95 150 0v35Z"/><circle cx="675" cy="205" r="50"/><path d="M600 335v-35c0-95 150-95 150 0v35Z"/></g>'+names.map((s,i)=>'<text x="'+(names.length===1?500:180+i*320)+'" y="125" text-anchor="middle" fill="#d8ecff" font-family="Arial" font-size="38" font-weight="800">'+esc(s.label)+'</text>').join('');
  }else if(kind==='chip'){
    figure='<g transform="translate(290 150)"><rect width="420" height="300" rx="36" fill="#111c2b" stroke="'+color+'" stroke-width="8"/>'+[0,1,2,3,4].map(i=>'<path d="M'+(50+i*80)+' -50v50m0 300v50M-50 '+(35+i*58)+'h50m420 0h50" stroke="#5483a8" stroke-width="12"/>').join('')+'<text x="210" y="165" text-anchor="middle" fill="#f7fbff" font-family="Arial" font-size="'+(label.length>10?39:52)+'" font-weight="900">'+esc(label)+'</text></g>';
  }else if(kind==='bank'){
    figure='<g fill="#1b2c40" stroke="'+color+'" stroke-width="6"><path d="M240 240L500 110 760 240Z"/><path d="M280 270v230m145-230v230m150-230v230m145-230v230M220 510h560"/></g><text x="500" y="225" text-anchor="middle" fill="white" font-family="Arial" font-size="46" font-weight="900">'+esc(label)+'</text>';
  }else if(kind==='exchange'){
    const recovering=event==='recovery';
    figure='<g fill="#14263c" stroke="'+(recovering?'#42e2aa':color)+'" stroke-width="7"><circle cx="300" cy="325" r="120"/><circle cx="740" cy="325" r="120"/><path d="M415 275h190l-40-40m40 140H415l40 40" fill="none" stroke-width="17"/></g><text x="520" y="120" text-anchor="middle" fill="white" font-family="Arial" font-size="54" font-weight="900">'+esc(label)+'</text>'+(event==='interruption'?'<path d="M500 205v230m42-230v230" stroke="#ff4565" stroke-width="20"/>':'');
  }else if(/breach|security|interruption|outflow|recovery/.test(event)||['card','wallet'].includes(kind)){
    figure='<rect x="190" y="170" width="620" height="330" rx="42" fill="#14263c" stroke="'+color+'" stroke-width="7"/><rect x="245" y="235" width="100" height="75" rx="14" fill="#d9a649"/><path d="M255 395h200m-200 50h135" stroke="#82a8c5" stroke-width="12" stroke-linecap="round"/><path d="M560 130l-50 140 90 80-70 170" stroke="'+color+'" stroke-width="18" fill="none"/><text x="520" y="80" text-anchor="middle" fill="#e7f2fc" font-family="Arial" font-size="46" font-weight="900">'+esc(label)+'</text>';
  }else{
    figure='<rect x="170" y="135" width="660" height="390" rx="36" fill="#14263c" stroke="'+color+'" stroke-width="6"/><rect x="225" y="195" width="160" height="150" rx="20" fill="#355470"/><path d="M430 210h330m-330 65h285m-535 125h535m-535 60h450" stroke="#93b5cf" stroke-width="17" stroke-linecap="round"/><text x="500" y="95" text-anchor="middle" fill="white" font-family="Arial" font-size="45" font-weight="900">'+esc(label)+'</text>';
  }
  return '<svg class="art" viewBox="0 0 1000 640" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Ilustración editorial de '+esc(lead?.name||brief.title||'la noticia')+'"><defs><radialGradient id="ed-bg"><stop stop-color="'+(art.priceMove?(down?'#5a102b':'#104a3d'):'#15304b')+'"/><stop offset="1" stop-color="#090e19"/></radialGradient><linearGradient id="metal" x2=".8" y2="1"><stop stop-color="#fff0ba"/><stop offset=".45" stop-color="#d5a452"/><stop offset="1" stop-color="#6f390e"/></linearGradient></defs><rect width="1000" height="640" fill="url(#ed-bg)"/><circle cx="500" cy="330" r="265" fill="none" stroke="'+color+'" stroke-width="2" opacity=".16"/>'+figure+'</svg>';
}
