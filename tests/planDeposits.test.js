import test from 'node:test';
import assert from 'node:assert/strict';
import { registerPlanDepositRoutes, handlePlanDepositEvent, depositAmountCents } from '../services/planDeposits.js';

function harness(extra = {}) {
  let rows = new Map([
    ['leads/a', { nombre: 'Ana Lopez', telefono: '528112345678', assignedTo: 'seller', email: 'ana@example.com' }],
    ['leads/b', { nombre: 'Otro Cliente', assignedTo: 'other' }],
    ['Negocios/shop', { leadId: 'a', plan: 'sin_plan' }],
  ]);
  const sessions = new Map();
  const routes = new Map(), intents = new Map(), customers = new Map(), sends = [], creates = [], cancels = [];
  const apply = (map, path, patch) => { map.set(path, { ...map.get(path), ...structuredClone(patch) }); };
  const snapshot = (map, path) => ({ exists: map.has(path), data: () => structuredClone(map.get(path)) });
  const doc = path => ({ id: path.split('/').at(-1), path, get: async () => snapshot(rows, path), update: async patch => apply(rows, path, patch), set: async data => rows.set(path, structuredClone(data)) });
  let chain = Promise.resolve();
  const db = { collection: name => ({ doc: id => doc(`${name}/${id}`) }), runTransaction(callback) {
    const next = chain.then(async () => {
      const draft = new Map(structuredClone([...rows]));
      const result = await callback({ get: async ref => snapshot(draft, ref.path), update: (ref, patch) => apply(draft, ref.path, patch), set: (ref, data) => draft.set(ref.path, structuredClone(data)) });
      rows = draft; return result;
    }); chain = next.catch(() => {}); return next;
  } };
  const stripe = {
    checkout: { sessions: {
      retrieve: async id => structuredClone(sessions.get(id)),
      create: async (data, options) => {
        const previous = [...sessions.values()].find(s => s.key === options.idempotencyKey);
        if (previous) return structuredClone(previous);
        const session = { ...data, id: `cs_${sessions.size + 1}`, key: options.idempotencyKey, status: 'open', payment_status: 'unpaid', url: 'https://checkout.stripe.com/card', expires_at: Math.floor(Date.now() / 1000) + 86400 };
        sessions.set(session.id, session); return structuredClone(session);
      },
    } },
    customers: { create: async (data, options) => { if (!customers.has(options.idempotencyKey)) customers.set(options.idempotencyKey, { id: `cus_${customers.size + 1}`, ...data }); return customers.get(options.idempotencyKey); } },
    paymentIntents: {
      retrieve: async id => structuredClone(intents.get(id)),
      cancel: async id => { cancels.push(id); intents.get(id).status = 'canceled'; },
      create: async (data, options) => {
        const reused = [...intents.values()].find(intent => intent.key === options.idempotencyKey);
        if (reused) return structuredClone(reused);
        creates.push(structuredClone(data));
        const next_action = data.payment_method_types[0] === 'oxxo'
          ? { oxxo_display_details: { number: '123456789', hosted_voucher_url: 'https://payments.stripe.com/oxxo/voucher', expires_after: Math.floor(Date.now() / 1000) + 86400 } }
          : { display_bank_transfer_instructions: { reference: 'REF123', hosted_instructions_url: 'https://payments.stripe.com/bank/instructions', financial_addresses: [{ spei: { clabe: '123456789012345678', bank_name: 'Banco', account_holder_name: 'NEGOCIOS WEB' } }] } };
        const intent = { ...data, id: `pi_${intents.size + 1}`, key: options.idempotencyKey, status: 'requires_action', amount_received: 0, next_action };
        intents.set(intent.id, intent); return structuredClone(intent);
      },
    },
  };
  registerPlanDepositRoutes({ get: (url, fn) => routes.set(`GET ${url.split('/').at(-1)}`, fn), post: (url, fn) => routes.set(`POST ${url.split('/').at(-1)}`, fn) }, {
    db, stripe, readUser: async req => req.user, canAccess: (user, lead) => user.role === 'admin' || lead.assignedTo === user.uid,
    loadPlans: async () => [{ id: 'basic', name: 'Plan Web', totalCents: 99000 }], resolveBusiness: async () => 'shop', send: async (...args) => sends.push(args), ...extra,
  });
  const user = { uid: 'seller', role: 'salesAgent' };
  const call = async (path, body = {}, as = user) => {
    const response = { code: 200, status(code) { this.code = code; return this; }, json(payload) { this.body = payload; return this; }, set() {} };
    await routes.get(`${path === 'plans' ? 'GET' : 'POST'} ${path}`)({ body, query: body, user: as }, response); return response;
  };
  const prepare = (extra = {}) => call('prepare', { leadId: 'a', planId: 'basic', method: 'spei', amount: '495.00', ...extra });
  const pay = async (id = 'pi_1') => { const intent = intents.get(id); intent.status = 'succeeded'; intent.amount_received = intent.amount; return handlePlanDepositEvent(db, { type: 'payment_intent.succeeded', data: { object: structuredClone(intent) } }); };
  return { db, stripe, sessions, intents, creates, customers, sends, cancels, call, prepare, pay, read: path => rows.get(path), list: name => [...rows.entries()].filter(([path]) => path.startsWith(`${name}/`)) };
}

test('SPEI genera CLABE por el anticipo, reutiliza el cobro y no envía ni registra ingresos al preparar', async () => {
  const h = harness();
  const first = await h.prepare(), second = await h.prepare();
  assert.equal(first.code, 200); assert.equal(second.body.requestId, first.body.requestId);
  assert.match(first.body.message, /CLABE: 123456789012345678/); assert.equal(first.body.remaining, 495);
  assert.match(first.body.message, /^Hola Ana 👋/);
  assert.match(first.body.message, /💰 Anticipo: \$495\.00 MXN/);
  assert.doesNotMatch(first.body.message, /https?:|Instrucciones:|Ana Lopez|Este cobro corresponde/);
  assert.equal(h.creates.length, 1); assert.equal(h.customers.size, 1); assert.equal(h.creates[0].amount, 49500);
  assert.equal(h.creates[0].metadata.paymentType, 'plan_deposit');
  assert.equal(h.sends.length, 0); assert.equal(h.list('CrmFinanceTransactions').length, 0);
  assert.equal(h.read('Negocios/shop').plan, 'sin_plan');
});

test('OXXO genera ficha y referencia con importe, vencimiento y datos reales del cliente', async () => {
  const h = harness(); const result = await h.prepare({ method: 'oxxo' });
  assert.equal(result.code, 200); assert.equal(result.body.instructions.number, '123456789');
  assert.match(result.body.message, /Ficha y código de barras: https:\/\/payments.stripe.com/);
  assert.match(result.body.message, /Vence:/); assert.equal(h.creates[0].payment_method_data.billing_details.email, 'ana@example.com');
  assert.equal(h.creates[0].payment_method_options.oxxo.expires_after_days, 3);
  assert.equal((await h.prepare({ method: 'oxxo', email: 'invalid' })).code, 400);
  assert.equal((await h.prepare({ method: 'oxxo', customerName: 'Ana' })).code, 400);
});

test('validación del anticipo, método, plan y permisos antes de llamar a Stripe', async () => {
  const h = harness();
  for (const body of [{ amount: '-1' }, { amount: '990.01' }, { amount: '3' }, { amount: '4oops' }, { amount: '10.999' }, { amount: 'Infinity' }, { method: 'unsupported' }, { planId: 'fake' }]) assert.equal((await h.prepare(body)).code, 400);
  assert.throws(() => depositAmountCents('10000.01', 2000000, 'oxxo'), /10,000/);
  assert.equal((await h.call('prepare', { leadId: 'a' }, null)).code, 401);
  assert.equal((await h.call('plans', { leadId: 'b' })).code, 403);
  assert.equal(h.creates.length, 0);
});

test('envío separado usa las instrucciones guardadas y evita doble clic y contactos ajenos', async () => {
  const h = harness(), reference = (await h.prepare()).body;
  assert.equal((await h.call('send', { leadId: 'a', requestId: reference.requestId })).code, 200);
  assert.equal((await h.call('send', { leadId: 'a', requestId: reference.requestId })).body.duplicate, true);
  assert.deepEqual(h.sends, [['a', reference.message]]);
  assert.equal((await h.call('send', { leadId: 'b', requestId: reference.requestId }, { uid: 'admin', role: 'admin' })).code, 403);
});

test('el webhook registra una sola vez el ingreso y saldo sin activar el plan', async () => {
  const h = harness(), reference = (await h.prepare()).body;
  await h.pay(); await h.pay();
  await handlePlanDepositEvent(h.db, { type: 'payment_intent.payment_failed', data: { object: h.intents.get('pi_1') } });
  assert.equal(h.list('CrmFinanceTransactions').length, 1);
  const service = h.list('CrmFinanceServices')[0][1];
  assert.equal(service.totalAmount, 990); assert.equal(service.paidAmount, 495); assert.equal(service.pendingAmount, 495);
  assert.equal(service.status, 'en_proceso'); assert.equal(h.read('Negocios/shop').plan, 'sin_plan');
  assert.equal(h.read('PlanDepositRequests/' + reference.requestId).status, 'paid');
  assert.equal(h.read('leads/a').planDeposit.pendingAmount, 495);
  assert.equal((await h.prepare()).code, 409);
});

test('pagos parciales SPEI siguen pendientes y los importes discordantes no generan ingresos', async () => {
  const h = harness(); await h.prepare(); const intent = h.intents.get('pi_1');
  intent.amount_received = 20000;
  await handlePlanDepositEvent(h.db, { type: 'payment_intent.partially_funded', data: { object: intent } });
  assert.equal(h.list('CrmFinanceTransactions').length, 0);
  const reference = (await h.prepare()).body;
  assert.match(reference.message, /Pendiente del anticipo: \$295\.00 MXN/);
  assert.equal((await h.call('send', { leadId: 'a', requestId: reference.requestId })).code, 200);
  assert.match(h.sends[0][1], /Pendiente del anticipo: \$295\.00 MXN/);
  intent.status = 'succeeded';
  await assert.rejects(handlePlanDepositEvent(h.db, { type: 'payment_intent.succeeded', data: { object: intent } }), /importe completo/);
  intent.currency = 'usd';
  await assert.rejects(handlePlanDepositEvent(h.db, { type: 'payment_intent.succeeded', data: { object: intent } }), /no coincide/);
  assert.equal(h.list('CrmFinanceTransactions').length, 0);
});

test('OXXO vencido genera nueva ficha, cancela la anterior e ignora eventos del intento anterior', async () => {
  const h = harness(); const first = await h.prepare({ method: 'oxxo' });
  const old = h.intents.get('pi_1'); old.next_action.oxxo_display_details.expires_after = 1;
  assert.equal((await h.prepare({ method: 'oxxo' })).code, 200);
  assert.equal(h.creates.length, 2); assert.deepEqual(h.cancels, ['pi_1']);
  assert.equal(h.read('PlanDepositRequests/' + first.body.requestId).attempt, 2);
  await handlePlanDepositEvent(h.db, { type: 'payment_intent.payment_failed', data: { object: old } });
  assert.equal(h.read('PlanDepositRequests/' + first.body.requestId).status, 'pending');
  await h.pay('pi_2'); assert.equal(h.list('CrmFinanceTransactions').length, 1);
});

test('el envío rechaza una ficha vencida o un pago ya completado', async () => {
  const h = harness(), first = (await h.prepare({ method: 'oxxo' })).body;
  await h.db.collection('PlanDepositRequests').doc(first.requestId).update({ instructions: { ...first.instructions, expiresAt: '2020-01-01T00:00:00Z' } });
  assert.equal((await h.call('send', { leadId: 'a', requestId: first.requestId })).code, 409);
  await h.pay();
  assert.equal((await h.call('send', { leadId: 'a', requestId: first.requestId })).code, 409);
  assert.equal(h.sends.length, 0);
});

test('solicitudes simultáneas crean un solo cobro', async () => {
  const h = harness(); const results = await Promise.all([h.prepare(), h.prepare()]);
  assert.deepEqual(results.map(result => result.code).sort(), [200, 409]); assert.equal(h.creates.length, 1);
});

test('OXXO envía la ficha real como imagen con mensaje breve y sin enlace', async () => {
  const images = [], renders = [];
  const h = harness({ renderVoucher: async args => { renders.push(args); return 'https://storage.googleapis.com/private/voucher.png'; }, sendImage: async (...args) => images.push(args) });
  const reference = (await h.prepare({ method: 'oxxo' })).body;
  const result = await h.call('send', { leadId: 'a', requestId: reference.requestId });
  assert.equal(result.code, 200); assert.equal(result.body.imageSent, true);
  assert.equal(h.sends.length, 0); assert.equal(images.length, 1);
  assert.equal(renders[0].paymentIntentId, 'pi_1'); assert.equal(renders[0].instructions.number, '123456789');
  assert.equal(images[0][0], 'a'); assert.equal(images[0][1], 'https://storage.googleapis.com/private/voucher.png');
  assert.match(images[0][2], /Referencia: 123456789/); assert.match(images[0][2], /Muestra esta ficha en caja/); assert.doesNotMatch(images[0][2], /https?:/);
});

test('no envía una imagen si no se pudo capturar o el pago se confirmó durante la captura', async () => {
  let count = 0;
  const h = harness({ renderVoucher: async () => { throw Error('No se pudo capturar la ficha'); }, sendImage: async () => { count++; } });
  const reference = (await h.prepare({ method: 'oxxo' })).body;
  assert.equal((await h.call('send', { leadId: 'a', requestId: reference.requestId })).code, 500);
  assert.equal(count, 0); assert.equal(h.read('PlanDepositRequests/' + reference.requestId).sentAt, undefined);
  const paid = harness({ renderVoucher: async () => { await paid.pay(); return 'https://storage.googleapis.com/private/voucher.png'; }, sendImage: async () => { count++; } });
  const next = (await paid.prepare({ method: 'oxxo' })).body;
  assert.equal((await paid.call('send', { leadId: 'a', requestId: next.requestId })).code, 409);
  assert.equal(count, 0);
});

test('SPEI envía la plantilla con datos exactos y conserva la CLABE copiable en el texto', async () => {
  const images = [], renders = [];
  const h = harness({ renderSpei: async args => { renders.push(args); return 'https://storage.googleapis.com/private/spei.png'; }, sendImage: async (...args) => images.push(args) });
  const reference = (await h.prepare()).body;
  const result = await h.call('send', { leadId: 'a', requestId: reference.requestId });
  assert.equal(result.code, 200); assert.equal(result.body.imageSent, true); assert.equal(h.sends.length, 0);
  assert.equal(renders[0].record.planName, 'Plan Web'); assert.equal(renders[0].record.amountCents, 49500);
  assert.equal(renders[0].instructions.clabe, '123456789012345678');
  assert.equal(images[0][1], 'https://storage.googleapis.com/private/spei.png');
  assert.match(images[0][2], /CLABE: 123456789012345678/); assert.doesNotMatch(images[0][2], /https?:/);
});


test('tarjeta reutiliza Checkout, envía un enlace y registra el anticipo solo al confirmarlo Stripe', async () => {
  const h = harness(), first = (await h.prepare({ method: 'card' })).body;
  assert.match(first.message, /Pago con tarjeta/); assert.equal(h.sessions.size, 1);
  assert.equal((await h.prepare({ method: 'card' })).body.requestId, first.requestId); assert.equal(h.sessions.size, 1);
  const session = h.sessions.get('cs_1');
  assert.equal(session.line_items[0].price_data.unit_amount, 49500);
  assert.deepEqual(session.payment_method_types, ['card']);
  assert.equal((await h.call('send', { leadId: 'a', requestId: first.requestId })).code, 200);
  assert.match(h.sends[0][1], /https:\/\/checkout.stripe.com/);
  assert.equal(h.list('CrmFinanceTransactions').length, 0);
  const intent = { id: 'pi_card', currency: 'mxn', amount: 49500, amount_received: 49500, status: 'succeeded', metadata: session.payment_intent_data.metadata };
  h.intents.set(intent.id, intent); session.payment_intent = intent.id; session.status = 'complete'; session.payment_status = 'paid';
  await handlePlanDepositEvent(h.db, { type: 'payment_intent.succeeded', data: { object: intent } });
  await handlePlanDepositEvent(h.db, { type: 'payment_intent.succeeded', data: { object: intent } });
  assert.equal(h.list('CrmFinanceTransactions').length, 1); assert.equal(h.list('CrmFinanceTransactions')[0][1].paymentMethod, 'stripe_card');
  assert.equal(h.list('CrmFinanceServices')[0][1].pendingAmount, 495);
  assert.equal((await h.prepare({ method: 'card' })).code, 409);
});

test('tarjeta vencida renueva el enlace; sesiones completadas no se vuelven a enviar', async () => {
  const h = harness(), first = (await h.prepare({ method: 'card' })).body;
  h.sessions.get('cs_1').status = 'expired';
  await h.prepare({ method: 'card' }); assert.equal(h.sessions.size, 2);
  h.sessions.get('cs_2').status = 'complete';
  assert.equal((await h.call('send', { leadId: 'a', requestId: first.requestId })).code, 409); assert.equal(h.sends.length, 0);
});
