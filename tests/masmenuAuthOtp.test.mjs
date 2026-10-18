// Signed hooks, Firestore/socket fixtures. No messages or external services.
import test from 'node:test';
import assert from 'node:assert/strict';
import { Webhook } from 'standardwebhooks';
import { createMasmenuAuthOtpHandler } from '../services/masmenuAuthOtp.mjs';
const secret=Buffer.alloc(32,9).toString('base64');
const payload={user:{id:'33333333-3333-4333-8333-333333333333',phone:'528112345678'},sms:{otp:'654321',phone:'528112345678'}};
function setup(options={}){
 const records=new Map(),sends=[];let queue=Promise.resolve(),time=Date.now();
 const db={collection:name=>({doc:id=>({key:`${name}/${id}`,set:async(value)=>{if(options.failWrite&&value.status==='sent')throw new Error('db private detail');records.set(`${name}/${id}`,{...records.get(`${name}/${id}`),...value});}})}),runTransaction(callback){const next=queue.then(async()=>{const writes=[];const result=await callback({get:async ref=>({exists:records.has(ref.key),data:()=>records.get(ref.key)}),set:(ref,value)=>writes.push([ref.key,value])});for(const [k,v]of writes)records.set(k,v);return result;});queue=next.catch(()=>{});return next;}};
 const socket={async sendMessage(jid,body,opts){sends.push({jid,body,opts});if(options.sendError)throw new Error('private OTP provider error');return options.noReceipt?{}:{key:{id:opts.messageId}};}};
 const handler=createMasmenuAuthOtpHandler({db,getSocket:()=>options.disconnected?null:socket,env:{MASMENU_AUTH_OTP_ENABLED:'true',MASMENU_AUTH_HOOK_SECRET:'v1,whsec_'+secret,...options.env},now:()=>time});
 return{records,sends,advance:ms=>time+=ms,async call(body=payload,{id='hook-fixture',timestamp=new Date(),tamper=false,signature,raw=true}={}){
  const text=JSON.stringify(body);const headers={'webhook-id':id,'webhook-timestamp':String(Math.floor(timestamp.getTime()/1000)),'webhook-signature':signature??new Webhook(secret).sign(id,timestamp,text)};
  const result={status:200,body:null};const res={set(){return res;},status:n=>(result.status=n,res),json:value=>(result.body=value,result)};
  await handler({body:raw?Buffer.from(text+(tamper?' ':'')):body,get:key=>headers[key]},res);return result;
 }};
}
test('Supabase signature, timestamp and exact raw payload are mandatory',async()=>{
 const f=setup();for(const options of [{signature:''},{signature:'v1,invalid'},{tamper:true},{timestamp:new Date(Date.now()-600000)}])assert.equal((await f.call(payload,options)).status,401);
 assert.equal((await f.call(payload,{raw:false})).status,400);assert.equal(f.sends.length,0);assert.equal(f.records.size,0);
});
test('disabled hook and invalid body never send codes',async()=>{
 assert.equal((await setup({env:{MASMENU_AUTH_OTP_ENABLED:'false'}}).call()).status,503);const f=setup();
 for(const p of [null,{...payload,sms:{otp:'1234',phone:'528112345678'}},{...payload,user:{id:'bad'}},{...payload,sms:{otp:'654321',phone:'12025550100'}}])assert.equal((await f.call(p)).status,400);
 assert.equal(f.sends.length,0);
});
test('reuses primary session, fixes message content and never persists OTP or phone',async()=>{
 const f=setup();assert.equal((await f.call()).status,200);assert.equal(f.sends[0].jid,'528112345678@s.whatsapp.net');assert.ok(f.sends[0].body.text.includes('654321'));assert.match(f.sends[0].opts.messageId,/^MASMENUAUTH_[A-F0-9]{32}$/);
 const stored=JSON.stringify([...f.records]);assert.ok(!stored.includes('654321'));assert.ok(!stored.includes('528112345678'));assert.ok(!stored.includes('Tu código'));
});
test('phone-change uses actual SMS destination instead of old confirmed phone',async()=>{
 const f=setup();assert.equal((await f.call({...payload,user:{...payload.user,phone:'528100000000',phone_change:'528112345678'}})).status,200);assert.equal(f.sends[0].jid,'528112345678@s.whatsapp.net');
});
test('retries/concurrent identical hooks send only once and detect changed signed body',async()=>{
 const f=setup();await Promise.all([f.call(),f.call()]);assert.equal(f.sends.length,1);assert.equal((await f.call()).status,200);assert.equal(f.sends.length,1);
 assert.equal((await f.call({...payload,sms:{...payload.sms,otp:'111111'}})).status,503);assert.equal(f.sends.length,1);
});
test('disconnection, socket rejection, missing receipt and uncertain DB write fail closed without resend',async()=>{
 for(const options of [{disconnected:true},{sendError:true},{noReceipt:true},{failWrite:true}]){const f=setup(options);assert.equal((await f.call()).status,503);await f.call();assert.ok(f.sends.length<=1);}
});
test('persistent recipient limits apply across distinct hook identifiers',async()=>{
 const f=setup();assert.equal((await f.call()).status,200);assert.equal((await f.call(payload,{id:'second'})).status,429);
 for(let i=1;i<10;i++){f.advance(61000);assert.equal((await f.call(payload,{id:`next-${i}`})).status,200);}
 f.advance(61000);assert.equal((await f.call(payload,{id:'eleventh'})).status,429);assert.equal(f.sends.length,10);
});
