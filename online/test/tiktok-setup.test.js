import test from 'node:test';
import assert from 'node:assert/strict';
import {startTikTokSetup} from '../scripts/tiktok-setup.mjs';
import {request as httpRequest} from 'node:http';

const connectUrl='https://wacapital-online.wacapital-online.workers.dev/tiktok/connect#'+(Date.now()+900_000)+'.'+'a'.repeat(64);
const input={clientKey:'fixture-client',clientSecret:'fixture-client-secret',sandbox:true};

test('local setup accepts only its owner link and same-origin requests',async()=>{
  let installs=0;const s=await startTikTokSetup({install:async()=>{installs++;return {connectUrl};}});
  try {
    const auth={Authorization:'Bearer '+s.token};
    assert.equal((await fetch(s.origin+'/status')).status,401);
    const hostStatus=await new Promise((resolve,reject)=>{
      const request=httpRequest(s.origin+'/status',{headers:{...auth,Host:'attacker.example'}},response=>{response.resume();resolve(response.statusCode);});
      request.on('error',reject);request.end();
    });
    assert.equal(hostStatus,403);
    assert.equal((await fetch(s.origin+'/status',{headers:{...auth,Origin:'https://attacker.example'}})).status,403);
    const request=(body,headers={})=>fetch(s.origin+'/install',{method:'POST',headers:{...auth,'Content-Type':'application/json',Origin:s.origin,...headers},body:JSON.stringify(body)});
    assert.equal((await request(input,{Origin:'https://attacker.example'})).status,403);
    assert.equal((await request(input,{'Content-Type':'text/plain'})).status,415);
    assert.equal((await request({...input,sandbox:false})).status,400);
    assert.equal((await request(input,{Origin:''})).status,403);
    assert.equal(installs,0);
    const page=await fetch(s.origin+'/');assert.match(await page.text(),/Cloudflare/);
    assert.match(page.headers.get('Content-Security-Policy'),/frame-ancestors 'none'/);
  } finally {await s.stop();}
});

test('local setup saves only the two named keys once and never returns them in its status',async()=>{
  let release;const pending=new Promise(resolve=>{release=resolve;});const seen=[];
  const s=await startTikTokSetup({install:async(values,onPhase)=>{seen.push(values);onPhase('cloudflare');await pending;return {connectUrl};}});
  try {
    const auth={Authorization:'Bearer '+s.token};
    const post=()=>fetch(s.origin+'/install',{method:'POST',headers:{...auth,Origin:s.origin,'Content-Type':'application/json'},body:JSON.stringify({...input,ADMIN_TOKEN:'untrusted-extra'})});
    const responses=await Promise.all([post(),post()]);
    assert.deepEqual(responses.map(r=>r.status).sort(),[202,409]);
    assert.deepEqual(seen,[{TIKTOK_CLIENT_KEY:input.clientKey,TIKTOK_CLIENT_SECRET:input.clientSecret}]);
    const progress=await (await fetch(s.origin+'/status',{headers:auth})).text();
    assert.doesNotMatch(progress,/fixture-client|untrusted-extra/);
    release();
    let status;
    for(let i=0;i<20;i++){status=await(await fetch(s.origin+'/status',{headers:auth})).json();if(status.state==='ready')break;}
    assert.equal(status.state,'ready');assert.equal(status.connectUrl,connectUrl);
    assert.equal((await post()).status,409);assert.equal(seen.length,1);
  } finally {release();await s.stop();}
});

test('installation errors are sanitized and cannot redirect the owner to another service',async()=>{
  for(const error of [true,false]){
    const s=await startTikTokSetup({install:async()=>{if(error)throw Error(input.clientSecret);return {connectUrl:'https://attacker.example/'};}});
    try {
      const headers={Authorization:'Bearer '+s.token};
      await fetch(s.origin+'/install',{method:'POST',headers:{...headers,Origin:s.origin,'Content-Type':'application/json'},body:JSON.stringify(input)});
      let value;
      for(let i=0;i<20;i++){value=await(await fetch(s.origin+'/status',{headers})).json();if(value.state==='error')break;}
      assert.equal(value.state,'error');assert.doesNotMatch(JSON.stringify(value),/fixture-client|attacker.example/);
    } finally {await s.stop();}
  }
});
