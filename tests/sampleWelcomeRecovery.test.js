import test from 'node:test';
import assert from 'node:assert/strict';
import { createSampleWelcomeRecovery } from '../services/sampleWelcomeRecovery.js';
import { sanitizeClosingConfig } from '../services/closingConfig.js';
import { closingDb } from './helpers/closingDb.js';
const initial = () => ({ nombre: 'Cliente', estado: 'nuevo', sampleAgent: { enabled: true, status: 'collecting', version: 'v1', inboxRevision: 1, brief: {}, audioMomentsSent: ['welcome'], history: [{ role: 'customer', text: 'Quiero información de la página' }, { role: 'assistant', text: 'Bienvenida' }], updatedAt: new Date('2026-10-31T16:00:00Z') } });
function harness(extra = {}) {
 const h = closingDb({ 'leads/a': initial() }); let clock = Date.parse('2026-10-31T16:00:00Z'), cfg = sanitizeClosingConfig({}); const sends = [];
 const service = createSampleWelcomeRecovery({ db: h.db, store: { get: async () => cfg }, send: async (...args) => sends.push(args), now: () => clock, ...extra });
 return { ...h, service, sends, ref: h.db.collection('leads').doc('a'), advance: v => { clock += v; }, configure: patch => { cfg = { ...cfg, ...patch }; }, enroll: args => service.enroll({ leadRef: h.db.collection('leads').doc('a'), version: 'v1', revision: 1, ...args }) };
}
test('envía pregunta por texto, recordatorio a dos horas y último al día siguiente, sin repetir audio', async () => {
 const h = harness(); assert.equal(await h.enroll(), true); assert.equal(await h.enroll(), false);
 await h.service.tick(); assert.equal(h.sends.length, 1); assert.match(h.sends[0][1], /cómo se llama tu negocio/);
 h.advance(119 * 60000); await h.service.tick(); assert.equal(h.sends.length, 1);
 h.advance(60000); await h.service.tick(); assert.equal(h.sends.length, 2); assert.match(h.sends[1][1], /a qué se dedica/);
 h.advance(22 * 3600000); await h.service.tick(); assert.equal(h.sends.length, 3); assert.equal(h.read('WelcomeRecoveryJobs/a'), undefined); assert.equal(h.read('leads/a').welcomeRecovery.state, 'finished');
 h.advance(48 * 3600000); await h.service.tick(); assert.equal(h.sends.length, 3);
});
test('si ya recibió pregunta por texto no la vuelve a enviar', async () => {
 const h = harness(); await h.enroll({ textAlreadySent: true }); await h.service.tick(); assert.equal(h.sends.length, 0);
 h.advance(2 * 3600000); await h.service.tick(); assert.equal(h.sends.length, 1); assert.match(h.sends[0][1], /a qué se dedica/);
});
test('respuesta nueva cancela dentro de la transacción de entrada, sin cambiar el asistente de muestras', async () => {
 const h = harness(); await h.enroll();
 await h.db.runTransaction(async tx => { const lead = (await tx.get(h.ref)).data(); h.service.cancelInTransaction(h.ref, lead, tx); tx.update(h.ref, { 'sampleAgent.inboxRevision': 2 }); });
 await h.service.tick(); assert.equal(h.sends.length, 0); assert.equal(h.read('leads/a').welcomeRecovery.state, 'replied'); assert.equal(h.read('leads/a').sampleAgent.enabled, true);
});
test('no programa si ya respondió durante la bienvenida, y cancela al avanzar muestra o intervenir humano', async () => {
 const h = harness(); await h.ref.update({ 'sampleAgent.inboxRevision': 2 }); assert.equal(await h.enroll(), false);
 await h.ref.update({ 'sampleAgent.inboxRevision': 1 }); await h.enroll(); await h.ref.update({ 'sampleAgent.brief.companyName': 'Taller' }); await h.service.tick(); assert.equal(h.sends.length, 0); assert.equal(h.read('WelcomeRecoveryJobs/a'), undefined);
 const manual = harness(); await manual.enroll(); await manual.service.cancel(manual.ref); await manual.service.tick(); assert.equal(manual.sends.length, 0); assert.equal(manual.read('leads/a').welcomeRecovery.state, 'human');
});
test('recuperación respeta horario y puede desactivarse independientemente del cierre', async () => {
 const h = harness(); h.configure({ startHour: 11, endHour: 20 }); await h.enroll(); await h.service.tick(); assert.equal(h.sends.length, 0);
 h.advance(3600000); await h.service.tick(); assert.equal(h.sends.length, 1);
 h.configure({ welcomeRecoveryEnabled: false }); h.advance(2 * 3600000); await h.service.tick(); assert.equal(h.sends.length, 1);
});
test('error de envío no se reintenta sin saber si el cliente recibió el mensaje', async () => {
 let sends = 0; const h = harness({ send: async () => { sends++; throw Error('Sin confirmación'); } }); await h.enroll(); await h.service.tick(); await h.service.tick(); assert.equal(sends, 1); assert.equal(h.read('leads/a').welcomeRecovery.state, 'review');
});
test('no recontacta compradores o contactos con cierre activo', async () => {
 for (const patch of [{ estado: 'compro' }, { estado: 'no interesado' }, { humanControl: true }, { archived: true }, { closing: { state: 'active' } }, { stopSequences: true }, { 'sampleAgent.status': 'completed' }]) { const h = harness(); await h.ref.update(patch); assert.equal(await h.enroll(), false); }
});


test('recuperación de últimas 48 horas incluye solo bienvenidas sin respuesta y no duplica programaciones', async () => {
 const h = harness();
 await h.db.collection('leads').doc('responded').set({ ...initial(), sampleAgent: { ...initial().sampleAgent, history: [{role:'customer',text:'hola'},{role:'assistant',text:'bienvenido'},{role:'customer',text:'ya respondi'}] } });
 await h.db.collection('leads').doc('old').set({ ...initial(), sampleAgent: { ...initial().sampleAgent, updatedAt: new Date('2026-10-01T16:00:00Z') } });
 const first = await h.service.recoverRecent(); assert.equal(first.scheduled, 1); assert.equal(h.sends.length, 0);
 assert.equal((await h.service.recoverRecent()).scheduled, 0);
 await h.service.tick(); assert.equal(h.sends.length, 1);
});
