import {coverSubtitle, xPublication, escapeHtml} from './publication.js';
import {artDirection} from './art-direction.js';
import {composePortraits,portraitCredits} from './portraits.js';

const clean = value => String(value || '').replace(/[\u0000-\u001f]/g,' ').replace(/\s+/g,' ').trim();
const segmenter = new Intl.Segmenter('es',{granularity:'sentence'});
const units = value => [...segmenter.segment(clean(value))].map(x=>x.segment.trim()).filter(Boolean);
const short = (values, fallback) => values.flatMap(units).find(x=>x.length<=220) || fallback;

// Adapt only approved text. No additional LLM call or claim of full-article verification.
export function socialKit(story) {
  const e = typeof story.editorial === 'string' ? JSON.parse(story.editorial) : story.editorial;
  const title=clean(e.headline), subtitle=coverSubtitle(e);
  const context=short([e.angle], 'La interpretación necesita contrastarse con la publicación original.');
  const watch=short(units(e.reasoning).slice(2), 'Comprueba el alcance del anuncio, su fecha y las actualizaciones de la fuente.');
  const slides=[
    {title,body:subtitle || 'Qué pasó, por qué importa y qué falta por confirmar.',label:'LA NOTICIA'},
    {title:'Qué se anunció',body:short([e.instagram,e.tweet],title),label:'01 · LOS DATOS'},
    {title:'Por qué puede importar',body:context,label:'02 · EL CONTEXTO'},
    {title:'Qué falta por confirmar',body:watch,label:'03 · LOS LÍMITES'},
    {title:'La clave está en el contexto',body:'Una noticia abre preguntas; la fuente y sus actualizaciones ayudan a responderlas. ¿Qué dato te falta para entender este caso?',label:'04 · PARA COMENTAR'}
  ];
  const beats=[
    {role:'gancho',text:title,visual:'Portada propia; mostrar fecha y etiqueta de ilustración.'},
    {role:'hecho',text:short([e.instagram,e.tweet],title),visual:'Subtítulos grandes y sujeto de la noticia; no simular una grabación real.'},
    {role:'contexto',text:context,visual:'Resaltar una relación explicada, sin dibujar precios ni cifras inventadas.'},
    {role:'cierre',text:'Consulta la fuente y sus actualizaciones. ¿Qué dato revisarías primero?',visual:'Fuente y pregunta final; sin prometer rentabilidad.'}
  ];
  const words=beats.map(x=>x.text).join(' ').split(/\s+/).length;
  return {version:1,state:'draft',requiresReview:true,storyId:story.id,
    audience:'Hispanohablantes de ambos lados del Atlántico',
    source:{name:story.source,url:story.source_url,publishedAt:new Date(story.published_at).toISOString(),basis:'Titular/extracto y redacción previamente aprobada; no verificación del artículo completo.'},
    title,x:xPublication(story),...(artDirection(story).faceoff?{mediaCredits:portraitCredits}:{}),
    hooks:[{type:'directo',text:title},{type:'contexto',text:'El contexto de este anuncio: '+title},{type:'pregunta',text:'¿Qué cambia con este anuncio? '+title}],
    slides,instagram:{width:1080,height:1350,caption:[title,context,'¿Qué dato te falta para entender mejor esta noticia?','Fuente: '+story.source_url].join('\n\n')},
    tiktok:{width:1080,height:1920,caption:[title,'Qué pasó y qué falta por confirmar.','Fuente: '+story.source_url].join('\n\n')},
    video:{state:'script-only',width:1080,height:1920,beats,wordCount:words,
      estimatedSeconds:Math.ceil(words/140*60),timingBasis:'Estimación de lectura a 140 palabras/minuto; no audio sincronizado.',
      needsEdit:words>105,voice:'Pendiente de grabación o de elegir una voz con cuota gratuita verificada.'}};
}

export function socialText(kit) {
  return ['# WaCapital · Borrador para otras redes',kit.title,'Referencia: '+kit.storyId,
    'Fecha de la noticia: '+kit.source.publishedAt,'Fuente: '+kit.source.url,kit.source.basis,
    'Requiere revisión. No se ha publicado nada. La prueba conserva la fecha original.',
    '## Ganchos alternativos',...kit.hooks.map(x=>x.type+': '+x.text),
    '## Instagram · Cinco láminas',...kit.slides.map((x,i)=>(i+1)+'. '+x.title+'\n'+x.body),
    '## Instagram · Pie de publicación',kit.instagram.caption,
    '## TikTok · Pie de publicación',kit.tiktok.caption,
    '## Reels / TikTok / Shorts · Guion',...kit.video.beats.map(x=>x.role.toUpperCase()+': '+x.text+'\nVisual: '+x.visual),
    'Duración orientativa: '+kit.video.estimatedSeconds+' segundos. '+kit.video.timingBasis,
    kit.video.needsEdit?'Necesita acortarse con edición antes de grabar.':'Revisar redacción y grabar la voz antes del montaje.',
    'Este paquete no incluye un MP4 ni voz generada. Las láminas son borradores separados de los originales aprobados.'].join('\n\n');
}

export function socialDimensions(platform) {
  if (!['instagram','tiktok'].includes(platform)) throw new Error('Invalid social platform');
  return {width:1080,height:platform==='tiktok'?1920:1350};
}

export function socialHtml(kit,image,index,platform) {
  const {height}=socialDimensions(platform), slide=kit.slides[index];
  if (!slide || !Number.isInteger(index) || !/^[A-Za-z0-9+/=]+$/.test(image) || image.length>2_400_000) throw new Error('Invalid social asset');
  const cover=index===0, tall=height===1920;
  const date=new Date(kit.source.publishedAt).toLocaleDateString('es-ES',{timeZone:'UTC',day:'2-digit',month:'short',year:'numeric'});
  const host=new URL(kit.source.url).hostname.replace(/^www\./,'');
  const html = `<!doctype html><html lang="es"><head><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; img-src data:; style-src 'unsafe-inline'; script-src 'unsafe-inline'"><style>
  *{box-sizing:border-box}html,body{margin:0;width:1080px;height:${height}px;overflow:hidden;background:#09171c;color:#f4fbfb;font-family:Arial,Helvetica,sans-serif}
  .top{position:absolute;top:50px;left:56px;right:56px;display:flex;justify-content:space-between;color:#79e5cb;font-size:23px;font-weight:bold;letter-spacing:2px}
  .headline{position:absolute;top:140px;left:56px;right:56px;height:${cover?'225':'265'}px;border-top:5px solid #38d9b0;padding-top:24px}
  h1,p{margin:0;overflow-wrap:anywhere}h1{font-size:70px;line-height:1.09;letter-spacing:-1px}p{font-size:${cover?'39':'55'}px;line-height:1.30}
  .art{position:absolute;top:405px;left:38px;width:1004px;height:${tall?'920':'540'}px;object-fit:cover;border-radius:16px}
  .body{position:absolute;top:${cover?(tall?'1375':'1000'):'485'}px;left:56px;right:56px;height:${cover?(tall?'300':'190'):(tall?'1010':'650')}px;color:${cover?'#9af0d8':'#e0eded'}}
  .section{position:absolute;top:420px;left:56px;color:#79e5cb;font-size:24px;font-weight:bold;letter-spacing:2px}
  .number{position:absolute;right:36px;bottom:175px;font-size:${tall?'360':'230'}px;font-weight:bold;color:#15343b;z-index:0}.body,.headline{z-index:1}
  .footer{position:absolute;bottom:${tall?'96':'20'}px;left:56px;right:56px;border-top:1px solid #52716b;padding-top:14px;font-size:17px;line-height:1.45;color:#adccc6}.brand{font-size:28px;font-weight:bold;color:#e0f7ef}.tag{font-size:16px;color:#a7c6c2}
  </style></head><body><div class="top"><span>WACAPITAL</span><span>${index+1} / 5</span></div>
  <div class="headline fit"><h1>${escapeHtml(slide.title)}</h1></div>
  ${cover?'<img class="art" src="data:image/jpeg;base64,'+image+'" alt="">':'<div class="section">'+escapeHtml(slide.label)+'</div><div class="number">'+String(index+1).padStart(2,'0')+'</div>'}
  <div class="body fit"><p>${escapeHtml(slide.body)}</p></div>
  <div class="footer"><div class="brand">WaCapital</div><div>${escapeHtml(host)} · ${escapeHtml(date)}</div><div class="tag">${cover?'Ilustración con IA · ':''}Borrador para revisión · ${escapeHtml(kit.storyId.slice(0,8))}</div></div>
  <script>for(const box of document.querySelectorAll('.fit')){const text=box.firstElementChild;let n=0;while((box.scrollHeight>box.clientHeight||box.scrollWidth>box.clientWidth)&&n++<130){const size=parseFloat(getComputedStyle(text).fontSize);if(size<=28)break;text.style.fontSize=(size-.5)+'px';}}document.documentElement.dataset.fits=String([...document.querySelectorAll('.fit')].every(x=>x.scrollHeight<=x.clientHeight&&x.scrollWidth<=x.clientWidth));</script></body></html>`;
  return kit.mediaCredits?composePortraits(html).replace('Ilustración con IA · ','Montaje editorial · ').replace('Borrador para revisión · ','Fotos: Daniel Torok / Wikimedia · Borrador · '):html;
}
