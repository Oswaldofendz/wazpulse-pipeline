const clean=value=>String(value||'').replace(/<[^>]*>/g,' ').replace(/[\u0000-\u001f]/g,' ').replace(/\s+/g,' ').trim();
const norm=value=>clean(value).normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9]+/g,' ').trim();
const sentences=value=>[...new Intl.Segmenter('es',{granularity:'sentence'}).segment(clean(value))].map(x=>x.segment.trim());
const filler=value=>/^(?:fuente\s*:|revisa\b|comprueba\b|consulta\b|informaci[oó]n educativa|una noticia abre preguntas|la interpretaci[oó]n necesita|qu[eé] pas[oó] y qu[eé] falta)/iu.test(clean(value));
const similar=(a,b)=>{
  const x=new Set(norm(a).split(' ').filter(w=>w.length>3)),y=new Set(norm(b).split(' ').filter(w=>w.length>3));
  const overlap=[...x].filter(w=>y.has(w)).length;
  return norm(a)===norm(b)||(x.size>0&&y.size>0&&overlap/Math.min(x.size,y.size)>.87);
};
const kinds=new Set(['breach','data','phishing','funds','verification','politics','company','timeline','news']);
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
const headings={data:'Los datos expuestos',phishing:'El riesgo de estafa',funds:'La situación de los fondos',verification:'Los nuevos controles',breach:'Cómo ocurrió la brecha',politics:'El frente político',timeline:'Las fechas clave',company:'El anuncio de la empresa',news:'Los detalles del anuncio'};

export function socialPlan(story) {
  const e=typeof story.editorial==='string'?JSON.parse(story.editorial):story.editorial;
  const custom=Array.isArray(e.socialSlides)?e.socialSlides:null;
  let slides;
  if(custom) {
    slides=custom.map((slide,index)=>({title:clean(slide.title),body:clean(slide.body),
      label:clean(slide.label)|| (index===0?'LA NOTICIA':'EL DATO'),
      visual:kinds.has(slide.visual)?slide.visual:socialScene(slide.title+' '+slide.body)}));
  }else{
    const title=clean(e.headline),details=[];
    for(const value of [e.instagram,e.tweet,e.angle,e.reasoning])for(const sentence of sentences(value)){
      if(sentence.length<22||filler(sentence)||similar(title,sentence)||details.some(x=>similar(x,sentence)))continue;
      details.push(sentence);
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
    slides=[{title:cover,body:clean(e.hook)&&!filler(e.hook)&&!similar(cover,e.hook)?clean(e.hook):'',label:'LA NOTICIA',visual:breach?'breach':socialScene(title)},
      ...details.slice(0,4).map((body,index)=>{const visual=socialScene(body);return {title:headings[visual],body,label:'EL DATO '+(index+1),visual};})];
  }
  const issues=[];
  if(slides.length<3||slides.length>5)issues.push('insufficient-distinct-facts');
  for(const slide of slides){
    if(!slide.title||slide.title.length>150||slide.body.length>260||filler(slide.body)||filler(slide.title))issues.push('copy-needs-edit');
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
