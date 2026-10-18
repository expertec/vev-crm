import test from 'node:test';import assert from 'node:assert/strict';
import {createMasmenuPaidAccessHandler} from '../services/masmenuPaidAccess.mjs';
const key='fixture-private-paid-access-key-1234567890';
const payload={purchaseId:'11111111-1111-4111-8111-111111111111',menuId:'22222222-2222-4222-8222-222222222222',slug:'pez-plata',restaurantName:'Pez Plata',phone:'+529991778701',environment:'test',pin:'2159',loginUrl:'https://menu.example.com/acceso?next=%2Fadministrar%2Fpez-plata#activar='+'t'.repeat(43)};
function fixture(options={}){
 const records=new Map(),sends=[];let queue=Promise.resolve();
 const db={collection:name=>({doc:id=>({key:name+'/'+id,set:async(value)=>{records.set(name+'/'+id,{...records.get(name+'/'+id),...value});}})}),runTransaction(fn){const p=queue.then(async()=>{const changes=[];const result=await fn({get:async ref=>({exists:records.has(ref.key),data:()=>records.get(ref.key)}),set:(ref,value)=>changes.push([ref.key,value])});for(const [k,v]of changes)records.set(k,v);return result;});queue=p.catch(()=>{});return p;}};
 const handler=createMasmenuPaidAccessHandler({db,getSocket:()=>options.disconnected?null:{async sendMessage(jid,body,opts){sends.push({jid,body,opts});if(options.fail)throw new Error('private error');return{key:{id:opts.messageId}};}},env:{MASMENU_PAID_ACCESS_ENABLED:'true',MASMENU_CONFIRMATION_API_KEY:key,MASMENU_PUBLIC_APP_URL:'https://menu.example.com',...options.env}});
 return{records,sends,async call(body=payload,authorization='Bearer '+key){const response={status:200,body:null};const res={set(){return res;},status:n=>(response.status=n,res),json:value=>(response.body=value,response)};await handler({body,get:()=>authorization},res);return response;}};
}
test('paid message requires server authentication, fixed recipient/URL and explicit configuration',async()=>{
 const f=fixture();assert.equal((await f.call(payload,'Bearer wrong')).status,401);assert.equal((await fixture({env:{MASMENU_PAID_ACCESS_ENABLED:'false'}}).call()).status,503);
 for(const p of [{...payload,pin:'12345'},{...payload,loginUrl:'https://evil.example/acceso'},{...payload,phone:'invalid'},{...payload,loginUrl:payload.loginUrl.replace('pez-plata','foreign')}])assert.equal((await f.call(p)).status,400);assert.equal(f.sends.length,0);
});
test('after payment template includes restaurant, phone, PIN, own panel, supported features and TEST label',async()=>{
 const f=fixture();assert.equal((await f.call()).status,200);const {body,jid,opts}=f.sends[0];assert.ok(body.text.includes('Pez Plata'));assert.ok(body.text.includes('PIN: *2159*'));assert.ok(body.text.includes('529991778701'));assert.ok(body.text.includes(payload.loginUrl));assert.ok(body.text.includes('No se cobró dinero real'));assert.ok(!body.text.includes('reservas'));assert.match(opts.messageId,/^MASMENUAUTH_/);assert.equal(jid,'529991778701@s.whatsapp.net');
 const persisted=JSON.stringify([...f.records]);for(const secret of ['2159','529991778701',payload.loginUrl])assert.ok(!persisted.includes(secret));
});
test('retries/concurrent notifications do not send or rotate the PIN twice',async()=>{
 const f=fixture();await Promise.all([f.call(),f.call()]);assert.equal(f.sends.length,1);assert.equal((await f.call()).body.status,'already_sent');assert.equal((await f.call({...payload,pin:'1111'})).status,409);assert.equal(f.sends.length,1);
});
test('existing customer message keeps current PIN without inventing or resetting it',async()=>{
 const f=fixture();const p={...payload,pin:null,loginUrl:payload.loginUrl.split('#')[0]};assert.equal((await f.call(p)).status,200);assert.ok(f.sends[0].body.text.includes('Usa tu PIN actual'));assert.ok(!f.sends[0].body.text.includes('2159'));
});
test('provider outage and uncertain send preserve reservation and prevent automatic duplicate',async()=>{
 for(const options of [{disconnected:true},{fail:true}]){const f=fixture(options);assert.ok((await f.call()).status>=400);await f.call();assert.ok(f.sends.length<=1);}
});
