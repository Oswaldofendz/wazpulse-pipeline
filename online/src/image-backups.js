import {Buffer} from 'node:buffer';
import {readBounded,ServiceError} from './io.js';

export const imageBackupDefinitions=[
  {id:'pollinations',key:'POLLINATIONS_API_KEY',limit:'DAILY_POLLINATIONS_LIMIT',model:'black-forest-labs/flux.1-schnell',url:'https://gen.pollinations.ai/v1/images/generations'},
  {id:'together',key:'TOGETHER_API_KEY',limit:'DAILY_TOGETHER_LIMIT',model:'black-forest-labs/FLUX.2-dev',url:'https://api.together.ai/v1/images/generations'},
  {id:'stability',key:'STABILITY_API_KEY',limit:'DAILY_STABILITY_LIMIT',model:'stable-image-core',url:'https://api.stability.ai/v2beta/stable-image/generate/core'}
];
export const imageLimit=(value,fallback=1)=>/^\d{1,2}$/.test(String(value))?Math.min(20,Number(value)):fallback;
export function configuredImageBackups(env){return imageBackupDefinitions.filter(p=>env.IMAGE_COST_POLICY==='allow-paid'&&env.IMAGE_REMOTE_BACKUPS_ENABLED==='true'&&env[p.key]&&imageLimit(env[p.limit])>0);}
export async function backupImage(provider,brief,env,fetcher){
  const headers={Authorization:'Bearer '+env[provider.key]};let body;
  if(provider.id==='stability'){
    headers.Accept='image/*';body=new FormData();body.set('prompt',brief.prompt);body.set('aspect_ratio','1:1');body.set('output_format','jpeg');
  }else{
    headers['Content-Type']='application/json';body=JSON.stringify({model:provider.model,prompt:brief.prompt,n:1,...(provider.id==='together'?{width:768,height:768,steps:20,response_format:'base64'}:{size:'1024x1024',response_format:'b64_json'})});
  }
  const response=await fetcher(provider.url,{method:'POST',headers,body,redirect:'manual',signal:AbortSignal.timeout(15000)});
  if(!response.ok)throw new ServiceError(provider.id,response.status);
  if(response.headers.get('finish-reason')==='CONTENT_FILTERED')throw new ServiceError('image-filtered');
  let image;
  if(provider.id==='stability'){
    const raw=await readBoundedBytes(response,1800000);image=Buffer.from(raw).toString('base64');
  }else{
    const data=JSON.parse(await readBounded(response,2500000));image=data?.data?.[0]?.b64_json;
  }
  if(typeof image!=='string'||image.length<100||image.length>2400000||!/^[A-Za-z0-9+/=]+$/.test(image))throw new ServiceError('invalid-image');
  const magic=Buffer.from(image.slice(0,24),'base64');
  if(!(magic[0]===255&&magic[1]===216&&magic[2]===255)&&![137,80,78,71,13,10,26,10].every((v,i)=>magic[i]===v))throw new ServiceError('invalid-image');
  return image;
}
async function readBoundedBytes(response,max){
  const reader=response.body?.getReader();if(!reader)throw new ServiceError('empty-image');
  let size=0;const chunks=[];
  try{while(true){const part=await reader.read();if(part.done)break;size+=part.value.length;if(size>max){await reader.cancel();throw new ServiceError('image-size');}chunks.push(part.value);}}
  finally{reader.releaseLock();}
  return Buffer.concat(chunks,size);
}
export function imageProvidersStatus(env){
  return [{id:'cloudflare',configured:Boolean(env.AI),enabled:true,dailyLimit:imageLimit(env.DAILY_VISUAL_LIMIT,3)},
    ...imageBackupDefinitions.map(p=>({id:p.id,configured:Boolean(env[p.key]),enabled:env.IMAGE_COST_POLICY==='allow-paid'&&env.IMAGE_REMOTE_BACKUPS_ENABLED==='true'&&Boolean(env[p.key]),dailyLimit:imageLimit(env[p.limit]),requiresCredentials:true,blockedByFreePolicy:env.IMAGE_COST_POLICY!=='allow-paid'})),
    {id:'horde',configured:true,enabled:env.HORDE_ENABLED==='true',dailyLimit:imageLimit(env.DAILY_HORDE_LIMIT,2),cryptoEligible:false},
    {id:'editorial-local',configured:true,enabled:env.EDITORIAL_FALLBACK_ENABLED!=='false',kind:'original-vector-artwork'}];
}
