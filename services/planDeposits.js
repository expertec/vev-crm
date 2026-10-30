import { createHash, randomUUID } from 'node:crypto';

const fail = (message, status = 400) => Object.assign(new Error(message), { status });
const text = (value, max = 200) => String(value || '').trim().slice(0, max);
const millis = value => value?.toMillis?.() || new Date(value || 0).getTime();
const key = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const money = cents => (cents / 100).toLocaleString('es-MX', { style: 'currency', currency: 'MXN' });
const refFor = (db, id) => db.collection('PlanDepositRequests').doc(id);

export function depositAmountCents(value, totalCents, method) {
  if (!/^(?:\d+)(?:\.\d{1,2})?$/.test(String(value ?? '').trim())) throw fail('Escribe un anticipo válido con hasta dos decimales.');
  const cents = Math.round(Number(value) * 100);
  if (!Number.isSafeInteger(cents) || cents < 1000 || cents > totalCents) throw fail('El anticipo debe ser de al menos $10 MXN y no superar el precio del plan.');
  if (method === 'oxxo' && cents > 1000000) throw fail('OXXO admite hasta $10,000 MXN por referencia.');
  return cents;
}

function instructionsFor(intent, record) {
  const next = intent.next_action || {};
  if (record.method === 'oxxo') {
    const details = next.oxxo_display_details;
    if (!details?.number || !details.hosted_voucher_url) throw fail('Stripe no devolvió la ficha OXXO. Verifica que OXXO esté habilitado en tu cuenta.', 502);
    return { number: details.number, url: details.hosted_voucher_url, expiresAt: details.expires_after ? new Date(details.expires_after * 1000).toISOString() : null };
  }
  const details = next.display_bank_transfer_instructions;
  const spei = details?.financial_addresses?.find(item => item.spei)?.spei;
  if (!spei?.clabe) throw fail('Stripe no devolvió una CLABE. Verifica que las transferencias bancarias en MXN estén habilitadas.', 502);
  return { remainingCents: Math.max(0, record.amountCents - Number(intent.amount_received || 0)), clabe: spei.clabe, bank: spei.bank_name || '', holder: spei.account_holder_name || '', reference: details.reference || '', url: details.hosted_instructions_url || '', expiresAt: null };
}

export function depositMessage(record, instructions, { voucherImage = false } = {}) {
  const firstName = record.customerName.split(/\s+/)[0];
  const amountLabel = instructions.remainingCents != null && instructions.remainingCents < record.amountCents ? 'Pendiente del anticipo' : 'Anticipo';
  const lines = [`Hola ${firstName} 👋`, `Te comparto los datos para iniciar tu *${record.planName}* ✨`, '',
    `💰 ${amountLabel}: ${money(instructions.remainingCents ?? record.amountCents)} MXN`,
    `📌 Saldo después del anticipo: ${money(record.totalCents - record.amountCents)} MXN`, ''];
  if (record.method === 'oxxo') {
    lines.push('🏪 Pago en OXXO', `Referencia: ${instructions.number}`);
    if (instructions.expiresAt) lines.push(`⏳ Vence: ${new Date(instructions.expiresAt).toLocaleString('es-MX', { timeZone: 'America/Mexico_City' })}`);
    lines.push(voucherImage ? '🎟️ Muestra esta ficha en caja para pagar.' : `🎟️ Ficha y código de barras: ${instructions.url}`, '', '✅ Te avisamos cuando se confirme el pago; en OXXO puede tardar hasta el siguiente día hábil.');
  } else {
    lines.push(instructions.bank ? `🏦 SPEI · ${instructions.bank}` : '🏦 Transferencia SPEI', `CLABE: ${instructions.clabe}`);
    if (instructions.holder) lines.push(`A nombre de: ${instructions.holder}`);
    if (instructions.reference) lines.push(`Referencia: ${instructions.reference}`);
    lines.push('', '✅ En cuanto se confirme tu pago, te avisamos por aquí. ¡Gracias!');
  }
  return lines.join('\n');
}

// Called only after verifying Stripe's webhook signature (or retrieving the intent from Stripe).
export async function handlePlanDepositEvent(db, event) {
  const intent = event.data?.object;
  if (intent?.metadata?.paymentType !== 'plan_deposit') return false;
  const status = ({ 'payment_intent.succeeded': 'paid', 'payment_intent.partially_funded': 'partially_funded', 'payment_intent.payment_failed': 'failed', 'payment_intent.canceled': 'cancelled' })[event.type];
  if (!status) return true;
  const id = intent.metadata.depositId;
  if (!/^[a-f0-9]{64}$/.test(id || '')) throw fail('Referencia de anticipo inválida.', 500);
  await db.runTransaction(async tx => {
    const ref = refFor(db, id), record = (await tx.get(ref)).data();
    if (!record) throw fail('No se encontró el anticipo del evento Stripe.', 500);
    if (String(record.attempt) !== intent.metadata.attempt) return;
    if (record.leadId !== intent.metadata.leadId || intent.currency !== 'mxn' || intent.amount !== record.amountCents) throw fail('El pago no coincide con el anticipo registrado.', 500);
    if (record.status === 'paid') return;
    if (status !== 'paid') {
      tx.update(ref, { status, amountReceivedCents: Math.max(Number(record.amountReceivedCents || 0), Number(intent.amount_received || 0)), updatedAt: new Date() }); return;
    }
    if (intent.status !== 'succeeded' || intent.amount_received < record.amountCents) throw fail('Stripe no confirmó el importe completo del anticipo.', 500);
    const serviceRef = db.collection('CrmFinanceServices').doc(record.serviceId);
    const service = (await tx.get(serviceRef)).data();
    const incomeRef = db.collection('CrmFinanceTransactions').doc(`stripe_${intent.id}`);
    const income = (await tx.get(incomeRef)).data();
    const leadRef = db.collection('leads').doc(record.leadId);
    const lead = (await tx.get(leadRef)).data();
    if (income) { tx.update(ref, { status: 'paid', updatedAt: new Date() }); return; }
    const received = intent.amount_received / 100;
    const paidAmount = Math.round(((service?.paidAmount || 0) + received) * 100) / 100;
    const totalAmount = service?.totalAmount ?? record.totalCents / 100;
    const pendingAmount = Math.max(0, Math.round((totalAmount - paidAmount) * 100) / 100);
    const now = new Date();
    tx.set(serviceRef, { ...(service || {}), clientName: record.customerName, serviceName: record.planName,
      leadId: record.leadId, leadPhone: record.phone, negocioId: record.negocioId || '', catalogItemId: record.planId,
      catalogItemName: record.planName, currency: 'MXN', totalAmount,
      advanceAmount: record.amountCents / 100, advancePercent: record.amountCents / record.totalCents * 100,
      paidAmount, pendingAmount, estimatedCostAmount: service?.estimatedCostAmount || 0, actualCostAmount: service?.actualCostAmount || 0,
      status: pendingAmount > 0 ? 'en_proceso' : 'pagado', lastPaymentAt: now, createdAt: service?.createdAt || now, updatedAt: now, createdBy: record.createdBy,
    });
    tx.set(incomeRef, { type: 'income', category: 'anticipo', amount: received, currency: 'MXN',
      paymentMethod: record.method === 'oxxo' ? 'stripe_oxxo' : 'stripe_spei', reference: intent.id,
      serviceId: serviceRef.id, serviceName: record.planName, clientName: record.customerName, leadId: record.leadId,
      leadPhone: record.phone, negocioId: record.negocioId || '', isAdvance: true, occurredAt: now, createdAt: now, updatedAt: now, createdBy: record.createdBy });
    tx.update(ref, { status: 'paid', paymentIntentId: intent.id, amountReceivedCents: intent.amount_received, paidAt: now, updatedAt: now });
    if (lead) tx.update(leadRef, { planDeposit: { requestId: id, serviceId: serviceRef.id, planId: record.planId, planName: record.planName, paidAmount, pendingAmount, status: 'paid', updatedAt: now } });
    // Subscription/plan activation is intentionally separate from an advance payment.
  });
  return true;
}

export function registerPlanDepositRoutes(app, { db, stripe, readUser, canAccess, loadPlans, resolveBusiness, send, paymentsReady = true, renderVoucher, renderSpei, sendImage }) {
  const contact = async (req, id) => {
    const user = await readUser(req);
    if (!user) throw fail('Inicia sesión para solicitar anticipos.', 401);
    if (typeof id !== 'string' || !id || id.includes('/') || id.length > 200) throw fail('Selecciona un contacto válido.');
    const ref = db.collection('leads').doc(id), snapshot = await ref.get();
    if (!snapshot.exists) throw fail('Contacto no encontrado.', 404);
    const lead = snapshot.data();
    if (!canAccess(user, lead)) throw fail('No tienes acceso a este contacto.', 403);
    return { user, ref, lead };
  };
  const route = (method, path, handler) => app[method](`/api/crm/plan-deposits/${path}`, async (req, res) => {
    res.set?.('Cache-Control', 'no-store');
    try { return res.json(await handler(req)); }
    catch (error) { return res.status(error.status || (error.type?.startsWith('Stripe') ? 502 : 500)).json({ error: error.message || 'No se pudo procesar el anticipo.' }); }
  });
  route('get', 'plans', async req => {
    const { lead } = await contact(req, req.query?.leadId);
    return { plans: await loadPlans(), customerName: lead.nombre || '', email: lead.email || lead.contactEmail || '' };
  });
  route('post', 'prepare', async req => {
    const body = req.body || {}, { user, ref: leadRef, lead } = await contact(req, body.leadId);
    if (!paymentsReady) throw fail('Configura STRIPE_WEBHOOK_SECRET antes de solicitar anticipos.', 503);
    const plan = (await loadPlans()).find(item => item.id === body.planId);
    if (!plan || !Number.isSafeInteger(plan.totalCents) || plan.totalCents <= 0) throw fail('Selecciona un plan vigente del catálogo.');
    const method = body.method;
    if (!['spei', 'oxxo'].includes(method)) throw fail('Selecciona SPEI u OXXO.');
    const amountCents = depositAmountCents(body.amount, plan.totalCents, method);
    const customerName = text(body.customerName || lead.nombre, 160), email = text(body.email || lead.email || lead.contactEmail, 200).toLowerCase();
    if (!customerName) throw fail('Escribe el nombre del cliente.');
    if (method === 'oxxo' && (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || customerName.split(/\s+/).filter(part => part.length >= 2).length < 2)) throw fail('OXXO requiere nombre y apellido (mínimo dos letras cada uno) y un correo válido.');
    if (email && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) throw fail('El correo no es válido.');
    const negocioId = await resolveBusiness(leadRef.id, lead);
    const id = key([leadRef.id, plan.id, plan.totalCents, amountCents, method, customerName, email]);
    const ref = refFor(db, id), owner = randomUUID();
    let record = await db.runTransaction(async tx => {
      const existing = (await tx.get(ref)).data();
      if (existing?.status === 'paid') throw fail('Este anticipo ya está pagado. Consulta su saldo en Finanzas.', 409);
      if (millis(existing?.lockUntil) > Date.now()) throw fail('La referencia se está generando. Espera unos segundos y vuelve a intentar.', 409);
      const next = { ...existing, leadId: leadRef.id, negocioId, phone: String(lead.telefono || '').replace(/\D/g, ''),
        planId: plan.id, planName: plan.name, totalCents: plan.totalCents, amountCents, method, customerName, email,
        serviceId: `stripe_plan_${key([leadRef.id, plan.id, plan.totalCents])}`, createdBy: existing?.createdBy || user.uid,
        attempt: existing?.attempt || 1, status: existing?.status || 'creating', createdAt: existing?.createdAt || new Date(),
        owner, lockUntil: new Date(Date.now() + 60000), updatedAt: new Date() };
      tx.set(ref, next); return next;
    });
    try {
      let intent = record.paymentIntentId ? await stripe.paymentIntents.retrieve(record.paymentIntentId) : null;
      if (intent?.status === 'succeeded') {
        await handlePlanDepositEvent(db, { type: 'payment_intent.succeeded', data: { object: intent } });
        throw fail('Este anticipo ya está pagado. Consulta su saldo en Finanzas.', 409);
      }
      const expired = method === 'oxxo' && intent?.next_action?.oxxo_display_details?.expires_after * 1000 <= Date.now();
      if (intent && (expired || ['canceled', 'requires_payment_method'].includes(intent.status))) {
        if (intent.status !== 'canceled') await stripe.paymentIntents.cancel(intent.id);
        record = await db.runTransaction(async tx => {
          const current = (await tx.get(ref)).data();
          if (current.owner !== owner) throw fail('La solicitud cambió. Vuelve a intentar.', 409);
          const next = { ...current, attempt: current.attempt + 1, paymentIntentId: '', status: 'creating' };
          tx.set(ref, next); return next;
        });
        intent = null;
      }
      if (!intent) {
        const customerId = record.customerId || (await stripe.customers.create({ name: customerName, ...(email ? { email } : {}), ...(record.phone ? { phone: `+${record.phone}` } : {}), metadata: { leadId: leadRef.id } }, { idempotencyKey: `deposit-customer-${id}` })).id;
        const options = method === 'spei' ? { customer_balance: { funding_type: 'bank_transfer', bank_transfer: { type: 'mx_bank_transfer' } } } : { oxxo: { expires_after_days: 3 } };
        intent = await stripe.paymentIntents.create({ amount: amountCents, currency: 'mxn', customer: customerId, confirm: true,
          description: `Anticipo de ${plan.name}`.slice(0, 500),
          payment_method_types: [method === 'spei' ? 'customer_balance' : 'oxxo'],
          payment_method_data: method === 'spei' ? { type: 'customer_balance' } : { type: 'oxxo', billing_details: { name: customerName, email } },
          payment_method_options: options,
          metadata: { paymentType: 'plan_deposit', depositId: id, leadId: leadRef.id, planId: plan.id, attempt: String(record.attempt) },
        }, { idempotencyKey: `deposit-${id}-${record.attempt}` });
        // Persist the Stripe ID before processing instructions or a fast payment confirmation.
        await ref.update({ paymentIntentId: intent.id, customerId, updatedAt: new Date() });
      }
      if (intent.status === 'succeeded') {
        await handlePlanDepositEvent(db, { type: 'payment_intent.succeeded', data: { object: intent } });
        throw fail('Stripe ya acreditó el anticipo. Consulta su saldo en Finanzas.', 409);
      }
      const instructions = instructionsFor(intent, record), message = depositMessage(record, instructions);
      await db.runTransaction(async tx => {
        const current = (await tx.get(ref)).data();
        if (current.owner !== owner) throw fail('La solicitud cambió. Vuelve a intentar.', 409);
        tx.update(ref, { instructions, message, paymentIntentId: intent.id, status: current.status === 'paid' ? 'paid' : 'pending', updatedAt: new Date() });
      });
      return { requestId: id, message, instructions, planName: plan.name, amount: amountCents / 100, total: plan.totalCents / 100, remaining: (plan.totalCents - amountCents) / 100 };
    } finally {
      await db.runTransaction(async tx => { const current = (await tx.get(ref)).data(); if (current?.owner === owner) tx.update(ref, { lockUntil: new Date(0) }); });
    }
  });
  route('post', 'send', async req => {
    const { leadId, requestId } = req.body || {};
    await contact(req, leadId);
    if (!/^[a-f0-9]{64}$/.test(requestId || '')) throw fail('Genera primero la referencia.');
    const ref = refFor(db, requestId), owner = randomUUID();
    const record = await db.runTransaction(async tx => {
      const current = (await tx.get(ref)).data();
      if (!current || current.leadId !== leadId) throw fail('La referencia no pertenece a este cliente.', 403);
      if (current.status !== 'pending' && current.status !== 'partially_funded') throw fail('La referencia no está pendiente de pago.', 409);
      if (millis(current.sendLockUntil) > Date.now()) throw fail('El envío ya está en proceso.', 409);
      if (millis(current.sentAt) > Date.now() - 60000) return null;
      if (current.instructions?.expiresAt && millis(current.instructions.expiresAt) <= Date.now()) throw fail('La referencia venció. Genera una nueva.', 409);
      if (!current.message) throw fail('La referencia no tiene instrucciones disponibles.', 409);
      tx.update(ref, { sendOwner: owner, sendLockUntil: new Date(Date.now() + 60000) }); return current;
    });
    if (!record) return { success: true, duplicate: true };
    try {
      const intent = await stripe.paymentIntents.retrieve(record.paymentIntentId);
      if (intent.status !== 'requires_action') throw fail('El estado del cobro cambió. Genera o consulta la referencia nuevamente.', 409);
      const instructions = instructionsFor(intent, record);
      const renderer = record.method === 'oxxo' ? renderVoucher : renderSpei;
      const imageSent = Boolean(renderer && sendImage);
      const message = depositMessage(record, instructions, { voucherImage: imageSent });
      if (imageSent) {
        const imageUrl = await renderer({ requestId, paymentIntentId: intent.id, record, instructions });
        // Recheck after rendering: the voucher may expire or be paid while Chrome is loading it.
        const latest = await stripe.paymentIntents.retrieve(intent.id);
        if (latest.status !== 'requires_action' || (instructions.expiresAt && millis(instructions.expiresAt) <= Date.now())) throw fail('La ficha ya no está disponible para pagar. Consulta la referencia nuevamente.', 409);
        await sendImage(leadId, imageUrl, message);
      } else await send(leadId, message);
      await ref.update({ instructions, message, imageSent, sentAt: new Date() });
      return { success: true, imageSent };
    } finally {
      await db.runTransaction(async tx => { const current = (await tx.get(ref)).data(); if (current?.sendOwner === owner) tx.update(ref, { sendLockUntil: new Date(0) }); });
    }
  });
}
