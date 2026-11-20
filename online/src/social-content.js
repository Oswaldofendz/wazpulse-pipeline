const clean=value=>String(value||'').replace(/<[^>]*>/g,' ').replace(/[\u0000-\u001f]/g,' ').replace(/\s+/g,' ').trim();
const norm=value=>clean(value).normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9]+/g,' ').trim();
const sentences=value=>[...new Intl.Segmenter('es',{granularity:'sentence'}).segment(clean(value))].map(x=>x.segment.trim());
export const newsCopy=value=>clean(value).replace(/https?:\/\/\S+/giu,'').replace(/#[\p{L}\p{N}_]+/gu,'').replace(/^[^\p{L}\p{N}¿¡]+/u,'').replace(/\s+/g,' ').trim();
export const editorialFiller=value=>/^(?:fuente\s*:|revisa\b|comprueba\b|consulta\b|informaci[oó]n educativa|una noticia abre preguntas|la interpretaci[oó]n necesita|qu[eé] pas[oó] y qu[eé] falta|la fuente (?:señala|describe|reporta)|(?:europa|el mercado|bitcoin) cambia de tono|el siguiente detalle|desliza\b)/iu.test(clean(value))||/^(?:#[\p{L}\p{N}_]+\s*)+$/u.test(clean(value));
const filler=editorialFiller;
const similar=(a,b)=>{
  const digits=value=>(value.match(/\d+(?:[.,]\d+)*/g)||[]).join('|');
  if(digits(a)!==digits(b))return false;
  const x=new Set(norm(a).split(' ').filter(w=>w.length>3)),y=new Set(norm(b).split(' ').filter(w=>w.length>3));
  const overlap=[...x].filter(w=>y.has(w)).length;
  return norm(a)===norm(b)||(x.size>0&&y.size>0&&overlap/Math.min(x.size,y.size)>.87);
};
const kinds=new Set(['breach','data','phishing','funds','verification','politics','company','timeline','news','map','summit']);
export function socialScene(text) {
  const value=norm(text);
  if(/3d secure|verificacion|autenticacion|controles|proteccion/.test(value))return 'verification';
  if(/phishing|estafa|fraud|mensaje|suplantacion/.test(value))return 'phishing';
  if(/fondos|saldos|retiros|cartera|wallet/.test(value)&&!/datos|vinculad/.test(value))return 'funds';
  if(/tarjeta|domicilio|direccion|datos personales|identidad/.test(value))return 'data';
  if(/brecha|filtracion|hack|breach|ciber/.test(value))return 'breach';
  if(/trump|vance|presidente|gobierno|eleccion|guerra|politica/.test(value))return 'politics';
  if(/plazo|fecha|noviembre|diciembre|enero|cierre|calendario/.test(value))return 'timeline';
  if(/empresa|compania|servicio|banco|plataforma/.test(value))return 'company';
  return 'news';
}
const genericHeading=/^(?:los detalles del anuncio|el frente politico|que se anuncio|por que puede importar|el dato(?: \d+)?|el contexto|que esta documentado)$/;
function factAnchors(value){
  const text=norm(value),patterns=[['europa',/europa|europeos/],['rusia',/rusia/],['ucrania',/ucrania/],['berlin',/berlin/],['dialogo',/negoci|conversacion|dialogo/],['unidad',/unid[oa]|conjunt[oa]/]];
  return patterns.filter(([,pattern])=>pattern.test(text)).map(([name])=>name).concat((text.match(/\d+/g)||[]).map(n=>'n:'+n));
}
function factCard(value,index){
  const sentence=newsCopy(value),plain=sentence.replace(/[.!?]$/u,'');
  if(plain.length<=85)return {title:plain,body:'',label:'EN CONTEXTO',visual:socialScene(sentence)};
  const clauses=sentence.split(/,\s+|;\s+|\s+y\s+/u),first=clauses[0];
  if(first.length>=22&&first.length<=85&&clauses.length>1){
    const rest=sentence.slice(first.length).replace(/^[,;\s]+(?:y\s+)?/u,'').replace(/^[a-záéíóúñ]/u,c=>c.toUpperCase());
    return {title:first,body:rest,label:'EN CONTEXTO',visual:socialScene(sentence)};
  }
  // A concrete sentence is preferable to a repeated generic heading. The mobile
  // quality gate requests editing if it cannot fit without losing meaning.
  return {title:plain,body:'',label:'EN CONTEXTO',visual:socialScene(sentence)};
}

export function socialPlan(story) {
  const e=typeof story.editorial==='string'?JSON.parse(story.editorial):story.editorial;
  const custom=Array.isArray(e.socialSlides)?e.socialSlides:null;
  let slides;
  if(custom) {
    slides=custom.map((slide,index)=>({title:newsCopy(slide.title),body:newsCopy(slide.body),
      label:clean(slide.label)|| (index===0?'LA NOTICIA':'EL DATO'),
      visual:kinds.has(slide.visual)?slide.visual:socialScene(slide.title+' '+slide.body),...(typeof slide.evidence==='string'?{evidence:clean(slide.evidence)}:{})}));
  }else{
    const title=newsCopy(e.headline),details=[],seenAnchors=new Set(factAnchors(title));
    // Captions contain publishing metadata. Strip it before sentence splitting;
    // analysis/reasoning is never promoted into a reported factual slide.
    for(const value of [e.instagram,e.tweet])for(const sentence of sentences(newsCopy(value))){
      if(sentence.length<22||filler(sentence)||similar(title,sentence)||details.some(x=>similar(x,sentence)))continue;
      const anchors=factAnchors(sentence);
      if(anchors.length>=3&&anchors.every(anchor=>seenAnchors.has(anchor)))continue;
      details.push(sentence);
      for(const anchor of anchors)seenAnchors.add(anchor);
    }
    const text=norm(story.title+' '+title);
    const breach=/breach|filtracion|expone|expos|robo|hack/.test(text)&&/tarjeta|card|wallet|cartera|domicilio|address/.test(text);
    if(breach&&details.length<2){
      const exposed=[];
      if(/tarjeta|card/.test(text))exposed.push('números de tarjeta');
      if(/domicilio|home address/.test(text))exposed.push('domicilios');
      if(exposed.length)details.push('Entre los datos expuestos figuran '+exposed.join(' y ')+'.');
      if(/wallet|cartera/.test(text))details.push('La noticia también identifica direcciones de carteras criptográficas vinculadas a los clientes.');
    }
    const subject=/gnosis\s+pay/i.test(story.title)?'Gnosis Pay':null;
    const cover=breach&&subject?subject+': datos expuestos':title;
    const context=/ending (?:the )?war in ukraine|fin de la guerra en ucrania/i.test(story.title)&&!/ucrania/i.test(cover)?'El objetivo declarado: poner fin a la guerra en Ucrania.':'';
    slides=[{title:cover,body:context,label:'LA NOTICIA',visual:breach?'breach':socialScene(title)},
      ...details.slice(0,3).map(factCard)];
  }
  const issues=[];
  slides=slides.filter((slide,index)=>{
    if(filler(slide.body))slide.body='';
    if(!slide.title||filler(slide.title)||genericHeading.test(norm(slide.title))&&!slide.body)return false;
    if(genericHeading.test(norm(slide.title))&&slide.body){const replacement=factCard(slide.body,index);slide.title=replacement.title;slide.body=replacement.body;}
    if(slide.body&&similar(slide.title,slide.body))slide.body='';
    return !slides.slice(0,index).some(prior=>similar(prior.title+' '+prior.body,slide.title+' '+slide.body));
  });
  if(slides.length<2||slides.length>5)issues.push('insufficient-distinct-facts');
  for(const slide of slides){
    if(!slide.title||slide.title.length>150||slide.body.length>260||filler(slide.body)||filler(slide.title)||/#\p{L}|https?:\/\//u.test(slide.title+' '+slide.body))issues.push('copy-needs-edit');
  }
  const signatures=slides.map(s=>norm(s.title+' '+s.body));
  if(new Set(signatures).size!==signatures.length)issues.push('repeated-slide');
  return {slides,quality:{ready:issues.length===0,issues:[...new Set(issues)]}};
}

export function socialDescription(story,plan) {
  const e=typeof story.editorial==='string'?JSON.parse(story.editorial):story.editorial;
  if(e.socialCaption)return String(e.socialCaption).trim();
  const lines=[clean(e.socialTitle||plan.slides[0]?.title||e.headline)];
  const facts=plan.slides.slice(1,3).map(slide=>slide.body).filter(Boolean);
  if(facts.length)lines.push(facts.join('\n'));
  lines.push('Fuente: '+story.source_url);
  return lines.join('\n\n');
}
