import test from 'node:test';import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {createMasmenuPaidAccessHandler} from '../services/masmenuPaidAccess.mjs';
const key='fixture-private-paid-access-key-1234567890';
const payload={purchaseId:'11111111-1111-4111-8111-111111111111',menuId:'22222222-2222-4222-8222-222222222222',slug:'pez-plata',restaurantName:'Pez Plata',phone:'+529991778701',environment:'test',pin:'2159',loginUrl:'https://menu.example.com/acceso?next=%2Fadministrar%2Fpez-plata#activar='+'t'.repeat(43)};
function fixture(options={}){
 const records=new Map(),sends=[],lookups=[],logs=[],events=new EventEmitter();let queue=Promise.resolve();
 const db={collection:name=>({doc:id=>({key:name+'/'+id,set:async(value)=>{records.set(name+'/'+id,{...records.get(name+'/'+id),...value});}})}),runTransaction(fn){const p=queue.then(async()=>{const changes=[];const result=await fn({get:async ref=>({exists:records.has(ref.key),data:()=>records.get(ref.key)}),set:(ref,value)=>changes.push([ref.key,value])});for(const [k,v]of changes)records.set(k,v);return result;});queue=p.catch(()=>{});return p;}};
 const handler=createMasmenuPaidAccessHandler({db,getSocket:()=>options.disconnected?null:{ev:events,async onWhatsApp(...candidates){lookups.push(candidates);return options.results??[{exists:true,jid:'5219991778701@s.whatsapp.net'}];},async sendMessage(jid,body,opts){sends.push({jid,body,opts});if(options.fail)throw new Error('private error');if(options.ack!==undefined)events.emit('messages.update',[{key:{id:opts.messageId,fromMe:true},update:{status:options.ack}}]);return{key:{id:opts.messageId}};}},receiptTimeoutMs:1,logger:{info:(...entry)=>logs.push(entry),warn:(...entry)=>logs.push(entry)},env:{MASMENU_PAID_ACCESS_ENABLED:'true',MASMENU_CONFIRMATION_API_KEY:key,MASMENU_PUBLIC_APP_URL:'https://menu.example.com',...options.env}});
 return{records,sends,lookups,logs,events,async call(body=payload,authorization='Bearer '+key){const response={status:200,body:null};const res={set(){return res;},status:n=>(response.status=n,res),json:value=>(response.body=value,response)};await handler({body,get:()=>authorization},res);return response;}};
}
test('paid message requires server authentication, fixed recipient/URL and explicit configuration',async()=>{
 const f=fixture();assert.equal((await f.call(payload,'Bearer wrong')).status,401);assert.equal((await fixture({env:{MASMENU_PAID_ACCESS_ENABLED:'false'}}).call()).status,503);
 for(const p of [{...payload,pin:'12345'},{...payload,loginUrl:'https://evil.example/acceso'},{...payload,phone:'invalid'},{...payload,loginUrl:payload.loginUrl.replace('pez-plata','foreign')}])assert.equal((await f.call(p)).status,400);assert.equal(f.sends.length,0);
});
test('after payment template includes restaurant, phone, PIN, own panel, supported features and TEST label',async()=>{
 const f=fixture();assert.equal((await f.call()).status,200);const {body,jid,opts}=f.sends[0];assert.ok(body.text.includes('Pez Plata'));assert.ok(body.text.includes('PIN: *2159*'));assert.ok(body.text.includes('529991778701'));assert.ok(body.text.includes(payload.loginUrl));assert.ok(body.text.includes('No se cobró dinero real'));assert.ok(!body.text.includes('reservas'));assert.match(opts.messageId,/^MASMENUAUTH_/);assert.equal(jid,'5219991778701@s.whatsapp.net');
 const persisted=JSON.stringify([...f.records])+JSON.stringify(f.logs);for(const secret of ['"pin"','529991778701',payload.loginUrl])assert.ok(!persisted.includes(secret));
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
test('MX lookup uses registered 521 or 52 destination and rejects absent/foreign contacts without sending',async()=>{
 for(const jid of ['5219991778701@s.whatsapp.net','529991778701@s.whatsapp.net']){
  const f=fixture({results:[{exists:true,jid}]});assert.equal((await f.call()).status,200);assert.equal(f.sends[0].jid,jid);
  assert.deepEqual(f.lookups[0],['5219991778701','529991778701']);
 }
 for(const results of [[],[{exists:false,jid:'5219991778701@s.whatsapp.net'}],[{exists:true,jid:'5218112345678@s.whatsapp.net'}]]){
  const f=fixture({results});assert.equal((await f.call()).body.code,'ACCESS_RECIPIENT_NOT_REGISTERED');assert.equal(f.sends.length,0);
 }
});
test('a failed recipient lookup can retry after the rate window without an uncertain-send dead end',async()=>{
 const options={results:[]},f=fixture(options);
 assert.equal((await f.call()).body.code,'ACCESS_RECIPIENT_NOT_REGISTERED');assert.equal(f.sends.length,0);
 options.results=[{exists:true,jid:'5219991778701@s.whatsapp.net'}];
 for(const [id,value]of f.records)if(id.startsWith('masmenuPaidAccessLimits/'))value.attempts=value.attempts.map(at=>at-61000);
 assert.equal((await f.call()).status,200);assert.equal(f.sends.length,1);
});
test('only recipient delivery receipts confirm arrival; acceptance/server ack do not, listeners are removed',async()=>{
 for(const ack of [undefined,2,3,4]){
  const f=fixture({ack});const response=await f.call();assert.equal(response.status,200);assert.equal(response.body.deliveryConfirmed,ack>=3);
  assert.equal(f.events.listenerCount('messages.update'),0);assert.equal((await f.call()).body.deliveryConfirmed,ack>=3);
 }
 const rejected=fixture({ack:0});assert.equal((await rejected.call()).status,503);assert.equal(rejected.events.listenerCount('messages.update'),0);
});
test('explicit resend has its own idempotency key, retains credentials and respects recipient limits',async()=>{
 const f=fixture();await f.call();
 const retry={...payload,resendRequestId:'33333333-3333-4333-8333-333333333333'};
 assert.equal((await f.call(retry)).status,429);assert.equal(f.sends.length,1);
 for(const [id,value]of f.records)if(id.startsWith('masmenuPaidAccessLimits/'))value.attempts=value.attempts.map(at=>at-61000);
 assert.equal((await f.call(retry)).status,200);assert.equal(f.sends.length,2);
 assert.equal((await f.call(retry)).body.status,'already_sent');assert.equal(f.sends.length,2);
 assert.equal(f.sends[0].body.text,f.sends[1].body.text);
});

test('production product flow accepts a normal PIN link and promises the complete editor without activation gates',async()=>{
 const f=fixture(),p={...payload,loginUrl:payload.loginUrl.split('#')[0]};assert.equal((await f.call(p)).status,200);
 const text=f.sends[0].body.text;assert.ok(text.includes('Crear y editar combos'));assert.ok(text.includes('Cambiar tu logo'));assert.ok(text.includes('Entra con tu teléfono y PIN'));assert.ok(!text.includes('7 días'));assert.ok(!text.includes('no se habilitó administración'));
});
