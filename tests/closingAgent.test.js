import test from 'node:test';
import assert from 'node:assert/strict';
import { sanitizeClosingConfig, nextClosingTime, offerPlanId } from '../services/closingConfig.js';
import { createClosingAgent, closingReply } from '../services/closingAgent.js';
import { createClosingFulfillment, addPlanYears } from '../services/closingFulfillment.js';
import { closingDb } from './helpers/closingDb.js';
const offer = { id: 'year', enabled: true, name: 'Emprendedor', totalCents: 99000, depositCents: 49500, plan: 'basic', years: 1, terms: 'Página y WhatsApp', domain: '.com', renewal: '$990 anuales', delivery: '24 horas desde el anticipo', balanceDue: 'a la entrega' };
const config = () => sanitizeClosingConfig({ enabled: true, defaultOfferId: 'year', startHour: 0, endHour: 24, firstDelayMinutes: 15, followupHours: [24, 48], offers: [offer] });
function fixture(overrides = {}) {
  const h=closingDb({ 'leads/a': { nombre:'Ana Lopez', telefono:'528112345678', sampleAgent: { enabled:true }, email:'ana@example.com' } });
  let clock=Date.parse('2026-10-31T16:00:00Z'), cfg=config();
  const sends=[],payments=[],assets=[];
  const agent=createClosingAgent({ db:h.db, store:{ get:async()=>cfg }, bucket:{file:path=>({getSignedUrl:async()=>['https://storage.example/'+path]})}, send:async(...args)=>sends.push(args), sendImage:async(...args)=>assets.push(args), sendAudio:async(...args)=>assets.push(args), cancelSequences:async()=>{}, preparePayment:async(id,body)=>{payments.push(body);return {requestId:'pay'};}, sendPayment:async(...args)=>sends.push(args), now:()=>clock, interpret:async()=>({action:'payment',method:'card',email:'',customerName:'',resumeAt:''}), ...overrides });
  return {...h,agent,sends,payments,assets,ref:h.db.collection('leads').doc('a'),advance:ms=>{clock+=ms;},setConfig:c=>{cfg=c;}};
}
test('ofertas validan anticipo, vigencia, archivos y configuración incompleta',()=>{
 assert.throws(()=>sanitizeClosingConfig({offers:[{...offer,depositCents:100000}]}));
 assert.throws(()=>sanitizeClosingConfig({enabled:true,defaultOfferId:'none',offers:[offer]}));
 assert.throws(()=>sanitizeClosingConfig({offers:[{...offer,imagePath:'https://evil/image.png'}]}));
 assert.throws(()=>sanitizeClosingConfig({offers:[{...offer,renewal:''}]}));
 assert.equal(config().offers[0].depositCents,49500);
 assert.notEqual(offerPlanId(config().offers[0]),offerPlanId(sanitizeClosingConfig({offers:[{...offer,totalCents:149000}]}).offers[0]));
});
test('horario evita madrugada y año bisiesto conserva fin de mes',()=>{
 const cfg={timezone:'America/Monterrey',startHour:9,endHour:20};
 assert.equal(nextClosingTime(Date.parse('2026-10-31T11:30:00Z'),cfg).toISOString(),'2026-10-31T15:00:00.000Z');
 assert.equal(addPlanYears(new Date('2024-02-29T16:00:00Z'),1).toISOString(),'2025-02-28T16:00:00.000Z');
});
test('apertura única conserva oferta; espera inicial y máximo de seguimientos; responde después del último',async()=>{
 const h=fixture();assert.equal(await h.agent.start(h.ref),true);assert.equal(await h.agent.start(h.ref),false);
 assert.equal(h.read('leads/a').sampleAgent.enabled,false);
 await h.agent.tick();assert.equal(h.sends.length,0);
 h.advance(16*60000);await h.agent.tick();assert.equal(h.sends.length,1,JSON.stringify(h.read("leads/a")));
 h.advance(25*3600000);await h.agent.tick();assert.equal(h.sends.length,2);
 h.advance(49*3600000);await h.agent.tick();assert.equal(h.sends.length,3);assert.equal(h.read('ClosingJobs/a'),undefined);
 await h.agent.receive({leadRef:h.ref,message:{content:'Quiero pagar con tarjeta'},messageId:'m1'});h.advance(9000);await h.agent.tick();assert.equal(h.payments.length,1);assert.equal(h.payments[0].amount,'495.00');
});
test('intervención humana pausa, pago confirmado detiene y rechazo no genera cobros',async()=>{
 const h=fixture();await h.agent.start(h.ref);await h.agent.pause(h.ref);h.advance(3600000);await h.agent.tick();assert.equal(h.sends.length,0);
 assert.equal(await h.agent.receive({leadRef:h.ref,message:{content:'hola'},messageId:'m2'}),true);assert.equal(h.read('ClosingJobs/a'),undefined);
 await h.agent.resume(h.ref);await h.ref.update({'planDeposit.status':'paid'});h.advance(120000);await h.agent.tick();assert.equal(h.sends.length,0);
 const stop=fixture({interpret:async()=>({action:'stop',method:'',email:'',customerName:'',resumeAt:''})});await stop.agent.start(stop.ref);await stop.agent.receive({leadRef:stop.ref,message:{content:'no me interesa'},messageId:'stop'});stop.advance(9000);await stop.agent.tick();assert.equal(stop.read('leads/a').closing.state,'stopped');assert.equal(stop.payments.length,0);
});
test('reglas de cambios y descuentos son respuestas fijas; no interpretan instrucciones para editar',()=>{
 assert.match(closingReply('changes',offer,['card']),/una vez confirmado tu anticipo/);
 assert.match(closingReply('discount',offer,['card']),/990/);
 assert.doesNotMatch(closingReply('changes',offer,['card']),/ya hice|ya cambié/);
});
test('pago confirmado activa una sola vez, mantiene saldo, envía ticket y credenciales sin duplicados',async()=>{
 const id='a'.repeat(64), record={status:'paid', amountReceivedCents:49500,amountCents:49500,totalCents:99000, closingOffer:config().offers[0], leadId:'a',negocioId:'shop',phone:'528112345678',customerName:'Ana Lopez',planName:'Emprendedor',planId:'year',serviceId:'sale',createdBy:'seller',paidAt:new Date('2026-10-31T16:00:00Z')};
 const h=closingDb({'leads/a':{nombre:'Ana',assignedTo:'seller',closing:{state:'active'}},'Negocios/shop':{leadId:'a',companyInfo:'Ana Web'},['PlanDepositRequests/'+id]:record});
 const files=new Map(),sends=[];
 const bucket={file:path=>({exists:async()=>[files.has(path)],save:async buffer=>files.set(path,buffer),getSignedUrl:async()=>['https://storage.example/'+path]})};
 const fulfillment=createClosingFulfillment({db:h.db,bucket,store:{get:async()=>config()},send:async(...args)=>sends.push(args),sendImage:async(...args)=>sends.push(args),cancelSequences:async()=>{}});
 await fulfillment.enqueue(id,record);await fulfillment.enqueue(id,record);await fulfillment.tick();
 const first=h.read('Negocios/shop');assert.equal(first.plan,'basic');assert.equal(first.expiresAt.toISOString(),'2027-10-31T16:00:00.000Z');assert.match(first.pin,/^\d{4}$/);assert.equal(sends.length,2);assert.match(sends[0][2],/Saldo pendiente: \$495/);assert.match(sends[1][1],/PIN/);assert.equal([...files.values()][0].subarray(1,4).toString(),'PNG');
 await fulfillment.enqueue(id,record);await fulfillment.tick();assert.equal(sends.length,2);assert.equal(h.read('Negocios/shop').pin,first.pin);assert.equal(h.read('ClosingFulfillmentJobs/'+id).state,'done');assert.equal(h.read('tasks/closing_'+id).assignedTo,'seller');
});
test('un fallo de envío se retiene para revisión y no repite el ticket automáticamente',async()=>{
 const id='b'.repeat(64),record={status:'paid',amountReceivedCents:49500,amountCents:49500,totalCents:99000,closingOffer:config().offers[0],leadId:'a',negocioId:'shop',phone:'528112345678',customerName:'Ana Lopez',planName:'Plan',serviceId:'sale',createdBy:'seller'};
 const h=closingDb({'leads/a':{closing:{state:'active'}},'Negocios/shop':{leadId:'a'},['PlanDepositRequests/'+id]:record});let count=0;
 const f=createClosingFulfillment({db:h.db,bucket:{file:()=>({exists:async()=>[true],getSignedUrl:async()=>['https://storage/ticket']})},store:{get:async()=>config()},cancelSequences:async()=>{},send:async()=>{},sendImage:async()=>{count++;throw Error('Sin confirmación');}});
 await f.enqueue(id,record);await f.tick();await f.tick();assert.equal(count,1);assert.equal(h.read('ClosingFulfillmentJobs/'+id).state,'uncertain');
});

test('cambio de oferta no altera una oferta enviada y las ofertas vencidas no cobran', async () => {
 const h=fixture();await h.agent.start(h.ref);h.setConfig(sanitizeClosingConfig({enabled:true,defaultOfferId:'year',offers:[{...offer,totalCents:149000,depositCents:74500}]}));
 assert.equal(h.read('leads/a').closing.offer.depositCents,49500);
 const c=h.read('leads/a').closing;await h.ref.update({'closing.offer':{...c.offer,endsAt:'2020-01-01T00:00:00Z'}});
 h.advance(20*60000);await h.agent.tick();assert.equal(h.read('leads/a').closing.state,'expired');assert.equal(h.payments.length,0);
});
test('la intervención durante la interpretación impide enviar una respuesta obsoleta',async()=>{
 let resolve;const interpreted=new Promise(r=>{resolve=r;});
 const h=fixture({interpret:()=>interpreted});await h.agent.start(h.ref);await h.agent.receive({leadRef:h.ref,message:{content:'Quiero pagar'},messageId:'m3'});h.advance(9000);
 const ticking=h.agent.tick();await new Promise(r=>setTimeout(r,10));await h.agent.pause(h.ref);resolve({action:'payment',method:'card',email:'',customerName:'',resumeAt:''});await ticking;assert.equal(h.sends.length,0);assert.equal(h.payments.length,0);
});
