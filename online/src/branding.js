import {waCapitalLogo} from '../assets/brand-logo-transparent.js';

// Reuse the logo cutout on every graphic; no per-post image generation or
// remote fetch is needed. Alpha-based shadows keep white lettering legible.
export function withBrandLogo(html,{platform,width=1080,height=1350,legacyMarket=false}={}) {
  if(!['x','tiktok'].includes(platform)||html.includes('data-wa-logo="1"'))return html;
  let placement,spacing='';
  if(platform==='tiktok'){
    placement=`left:76px;top:${legacyMarket?316:330}px;width:${legacyMarket?88:128}px;height:${legacyMarket?88:128}px`;
    spacing=legacyMarket?'.brand,.stamp{left:188px!important}':'.brand b,main>header>.brand{padding-left:160px!important}.chapter{left:160px}main.safe>.eyebrow{left:160px}';
  }else if(height>width){
    placement='left:52px;bottom:24px;width:88px;height:88px';
    spacing='.footer{padding-left:112px;min-height:100px}';
  }else{
    placement=`right:${width>=1600?66:56}px;top:${width>=1600?128:108}px;width:104px;height:104px`;
    if(width>=1600)spacing='.headline{right:210px}';
  }
  const css=`.wc-brand-logo{position:absolute;${placement};z-index:10;display:block;object-fit:contain;border:0;background:none;box-shadow:none;filter:drop-shadow(1px 0 0 #080b12) drop-shadow(-1px 0 0 #080b12) drop-shadow(0 1px 0 #080b12) drop-shadow(0 -1px 0 #080b12) drop-shadow(0 2px 3px #0006);pointer-events:none}${spacing}`;
  const mark='<img class="wc-brand-logo" data-wa-logo="1" src="'+waCapitalLogo+'" alt="Logo de WaCapital" decoding="sync">';
  // Older market HTML did not need images and therefore denied them by CSP.
  html=html.replace(/(<meta http-equiv="Content-Security-Policy" content=")([^"]*)"/,(match,prefix,policy)=>prefix+(/(?:^|;)\s*img-src\b/.test(policy)?policy:policy+"; img-src data:")+'"');
  const end=html.includes('</body>')?'</body>':'</html>';
  return html.replace('</style>',css+'</style>').replace(end,mark+end);
}
