import {trump,vance} from '../assets/portraits.js';
export {trump};
export const portraitCredits={author:'Daniel Torok',kind:'Montaje editorial con retratos oficiales',
  sources:['https://commons.wikimedia.org/wiki/File:Donald_Trump_official_portrait,_2025_(headshot).jpg','https://commons.wikimedia.org/wiki/File:JD_Vance_official_portrait_(headshot).jpg']};
export const portraitArt = () => `<div class="art" style="display:flex;overflow:hidden;position:absolute;background:#141c27">
  <div style="position:relative;width:50%;height:100%;overflow:hidden;border-right:5px solid #ff6a54"><img src="data:image/jpeg;base64,${trump}" alt="Retrato oficial de Donald Trump" style="width:100%;height:100%;object-fit:cover;object-position:50% 25%"><div style="position:absolute;inset:0;background:linear-gradient(0deg,#080c12cc,transparent 30%)"></div><b style="position:absolute;bottom:20px;left:24px;color:white;font:bold 30px Arial;letter-spacing:3px">TRUMP</b></div>
  <div style="position:relative;width:50%;height:100%;overflow:hidden;border-left:5px solid #58c3fa"><img src="data:image/jpeg;base64,${vance}" alt="Retrato oficial de JD Vance" style="width:100%;height:100%;object-fit:cover;object-position:50% 25%"><div style="position:absolute;inset:0;background:linear-gradient(0deg,#080c12cc,transparent 30%)"></div><b style="position:absolute;bottom:20px;right:24px;color:white;font:bold 30px Arial;letter-spacing:3px">VANCE</b></div>
  <div style="position:absolute;left:50%;top:55%;transform:translate(-50%,-50%) rotate(-8deg);background:#080c12;color:white;padding:12px;border:2px solid white;font:italic 900 40px Arial">VS</div></div>`;
export function composePortraits(html) {
  return html.replace(/<img class="art"[^>]*>/,()=>portraitArt())
    .replaceAll('ILUSTRACIÓN CON IA','MONTAJE EDITORIAL')
    .replace('Información, no asesoramiento.','Retratos oficiales · Daniel Torok / Wikimedia Commons');
}
