import { randomInt, randomUUID } from 'node:crypto';
import sharp from 'sharp';
import QRCode from 'qrcode';
import { generarMensajeCredenciales } from '../pinUtils.js';
import { clean, ms, money } from './closingConfig.js';
const escape = v => String(v ?? '').replace(/[<>&"']/g, c => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;', "'": '&apos;' })[c]);
const farFuture = () => new Date('9999-01-01T00:00:00Z');
export function addPlanYears(date, years) { const result = new Date(date); const month = result.getUTCMonth(); result.setUTCFullYear(result.getUTCFullYear() + years); if (result.getUTCMonth() !== month) result.setUTCDate(0); return result; }
export async function renderClosingTicket({ bucket, record, id, issuer }) {
  const file = bucket.file(`closing-tickets/${id}.png`);
  if (!(await file.exists())[0]) {
    const paid = record.amountCents, pending = record.totalCents - paid;
    const qr = await QRCode.toDataURL(JSON.stringify({ folio: id, total: record.totalCents / 100, cobrado: paid / 100, saldo: pending / 100 }), { width: 150, margin: 1 });
    const line = (value, x, y, size = 24, color = '#0b1b35', weight = 'normal') => `<text x="${x}" y="${y}" font-family="sans-serif" font-size="${size}" font-weight="${weight}" fill="${color}">${escape(value)}</text>`;
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="1240" height="1080"><rect width="1240" height="1080" fill="#f2f7fc"/><rect x="35" y="35" width="1170" height="1010" rx="30" fill="white" stroke="#dce6f4"/>
      ${line('negocios web.mx', 75, 105, 34, '#0b1b35', 'bold')}${line('TICKET DE VENTA', 540, 100, 42, '#0b1b35', 'bold')}${line('Comprobante interno · NO FISCAL', 540, 140, 23)}
      ${line(issuer.issuerName, 75, 155, 22)}${line(issuer.issuerRfc ? `RFC: ${issuer.issuerRfc}` : '', 75, 188, 20)}
      <rect x="35" y="215" width="1170" height="8" fill="#17c5ad"/>
      ${line('Cliente', 75, 280)}${line(clean(record.customerName, 48), 75, 325, 32, '#0b1b35', 'bold')}
      ${line(`Folio: ${id.slice(0, 24)}`, 700, 280, 20)}${line(`Emitido: ${new Date().toLocaleString('es-MX', { timeZone: 'America/Monterrey' })}`, 700, 320, 18)}
      ${line('Concepto del pago', 75, 410)}${line('ANTICIPO DE SERVICIOS', 75, 457, 32, '#0b1b35', 'bold')}${line(clean(record.planName, 65), 75, 505, 26)}
      <rect x="75" y="565" width="340" height="140" rx="16" fill="#edf3ff"/><rect x="450" y="565" width="340" height="140" rx="16" fill="#ecfcf3"/><rect x="825" y="565" width="340" height="140" rx="16" fill="#fff4ed"/>
      ${line('Total de la venta', 95, 605, 23)}${line(money(record.totalCents), 95, 660, 40, '#0b1b35', 'bold')}
      ${line('Anticipo recibido', 470, 605, 23)}${line(money(paid), 470, 660, 40, '#176b3f', 'bold')}
      ${line('Saldo por cobrar', 845, 605, 23)}${line(money(pending), 845, 660, 40, '#a33710', 'bold')}
      ${line(`Método: ${{ card: 'Tarjeta', spei: 'SPEI', oxxo: 'OXXO' }[record.method] || record.method}`, 75, 760, 23)}
      ${line('QR del comprobante', 75, 820, 25, '#0b1b35', 'bold')}${line(`Folio completo: ${id.slice(0, 32)}`, 75, 860, 18)}${line(id.slice(32), 75, 890, 18)}
      <image href="${qr}" x="1005" y="765" width="150" height="150"/>
      ${line('Documento informativo. No sustituye una factura CFDI.', 75, 990, 22)}</svg>`;
    await file.save(await sharp(Buffer.from(svg)).png().toBuffer(), { resumable: false, contentType: 'image/png' });
  }
  return (await file.getSignedUrl({ action: 'read', expires: new Date(Date.now() + 7 * 86400000) }))[0];
}
export function createClosingFulfillment({ db, bucket, store, send, sendImage, cancelSequences, now = Date.now }) {
  const jobs = db.collection('ClosingFulfillmentJobs');
  async function enqueue(id, record) {
    await db.runTransaction(async tx => {
      const ref = jobs.doc(id), previous = (await tx.get(ref)).data();
      if (!previous) tx.set(ref, { depositId: id, leadId: record.leadId, state: 'pending', nextAt: new Date(), leaseUntil: new Date(0), updatedAt: new Date(), createdAt: new Date() });
    });
  }
  async function activate(id, record) {
    if (!record.negocioId) throw Error('El pago no tiene un negocio asociado. Revisa el contacto.');
    const cfg = await store.get();
    const businessRef = db.collection('Negocios').doc(record.negocioId), leadRef = db.collection('leads').doc(record.leadId), jobRef = jobs.doc(id);
    return db.runTransaction(async tx => {
      const business = (await tx.get(businessRef)).data(), job = (await tx.get(jobRef)).data();
      const lead = (await tx.get(leadRef)).data();
      if (!business || !lead || business.leadId && business.leadId !== record.leadId) throw Error('El negocio no coincide con el cliente del pago.');
      if (!record.closingOffer || !['basic', 'pro', 'premium', 'ventas'].includes(record.closingOffer.plan)) throw Error('El pago no tiene un plan válido para activar.');
      if (job.activatedAt) return business;
      const start = new Date(record.paidAt?.toDate?.() || record.paidAt || now()), end = addPlanYears(start, record.closingOffer.years || 1);
      const pin = /^\d{4}$/.test(String(business.pin || '')) ? String(business.pin) : String(randomInt(1000, 10000));
      const patch = { plan: record.closingOffer.plan, pin, pinCreatedAt: business.pinCreatedAt || start, planStartDate: start, planStartAt: start, planRenewalDate: end, planExpiresAt: end, expiresAt: end, status: 'Activo', planActivatedAt: start, planDurationDays: Math.round((end - start) / 86400000), closingActivationId: id, ...(record.email ? { contactEmail: record.email } : {}), updatedAt: new Date() };
      tx.update(businessRef, patch);
      tx.update(leadRef, { estado: 'compro', 'closing.state': 'paid', 'closing.updatedAt': new Date(), 'sampleAgent.enabled': false });
      tx.update(jobRef, { activatedAt: new Date(), updatedAt: new Date() });
      tx.set(db.collection('tasks').doc(`closing_${id}`), { leadId: record.leadId, negocioId: record.negocioId, title: `Anticipo confirmado: trabajar cambios de ${record.planName}`, description: `Venta ${record.serviceId}. Revisar cambios solicitados y saldo pendiente. ${lead.closing?.requestedChanges || ''}`, status: 'pendiente', assignedTo: lead.assignedTo || cfg.ownerUid || record.createdBy, assignedToName: lead.assignedToName || 'Asesor', createdBy: record.createdBy, createdByName: 'Agente de cierre', leadName: record.customerName, leadPhone: record.phone, source: 'closing_agent', serviceId: record.serviceId, active: true, dueDate: '', dueTime: '', createdAt: new Date(), updatedAt: new Date() }, { merge: true });
      return { ...business, ...patch };
    });
  }
  let ticking = false;
  async function tick() {
    if (ticking) return; ticking = true;
    try {
      const due = await jobs.where('nextAt', '<=', new Date(now())).orderBy('nextAt').limit(10).get();
      for (const doc of due.docs) {
        const owner = randomUUID();
        const claimed = await db.runTransaction(async tx => {
          const job = (await tx.get(doc.ref)).data();
          if (!job || ['done', 'uncertain'].includes(job.state) || ms(job.leaseUntil) > now()) return false;
          if (job.sending) { tx.update(doc.ref, { state: 'uncertain', error: 'El envío quedó sin confirmar. Revisa el chat antes de reintentar.', nextAt: farFuture(), updatedAt: new Date() }); return false; }
          tx.update(doc.ref, { owner, state: 'processing', leaseUntil: new Date(now() + 300000) }); return true;
        });
        if (!claimed) continue;
        try {
          const record = (await db.collection('PlanDepositRequests').doc(doc.id).get()).data();
          if (record?.status !== 'paid' || record.amountReceivedCents < record.amountCents) throw Error('El anticipo no está confirmado.');
          const business = await activate(doc.id, record);
          await cancelSequences(record.leadId);
          await db.collection('ClosingJobs').doc(record.leadId).delete();
          const step = async (name, fn) => {
            const latest = (await doc.ref.get()).data(); if (latest[`${name}SentAt`]) return;
            if (latest.owner !== owner || latest.sending) throw Error('La entrega requiere revisión.');
            await doc.ref.update({ sending: name });
            await fn();
            await doc.ref.update({ [`${name}SentAt`]: new Date(), sending: '', updatedAt: new Date() });
          };
          const issuer = await store.get();
          await step('ticket', async () => {
            const url = await renderClosingTicket({ bucket, record, id: doc.id, issuer });
            await sendImage(record.leadId, url, `Ticket de venta no fiscal · ${record.planName}\nTotal: ${money(record.totalCents)} MXN\nAnticipo recibido: ${money(record.amountCents)} MXN\nSaldo pendiente: ${money(record.totalCents - record.amountCents)} MXN`);
          });
          await step('credentials', () => send(record.leadId, generarMensajeCredenciales({ companyName: business.companyInfo || record.customerName, pin: business.pin, phone: record.phone, plan: record.closingOffer.plan, loginUrl: process.env.CLIENT_PANEL_URL || 'https://negociosweb.mx/cliente-login' })));
          await doc.ref.update({ state: 'done', error: '', sending: '', leaseUntil: new Date(0), nextAt: farFuture(), updatedAt: new Date() });
        } catch (e) {
          const latest = (await doc.ref.get()).data();
          await doc.ref.update({ state: latest.sending ? 'uncertain' : 'error', error: clean(e.message, 400), leaseUntil: new Date(0), nextAt: farFuture(), updatedAt: new Date() });
        }
      }
    } finally { ticking = false; }
  }
  return { enqueue, tick };
}
