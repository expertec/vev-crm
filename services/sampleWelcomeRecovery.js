import { randomUUID } from 'node:crypto';
import { agentCanReply } from './sampleConversationAgent.js';
import { ms, nextClosingTime } from './closingConfig.js';
const messages = [
  'Para preparar tu muestra gratuita, ¿cómo se llama tu negocio?',
  'Hola 😊 Podemos empezar tu muestra con unos datos sencillos. Cuéntame a qué se dedica tu negocio y te voy guiando.',
  '¿Todavía te gustaría ver cómo quedaría la página de tu negocio? Si ahora estás ocupado, podemos retomarlo cuando te quede mejor.',
];
export function eligibleWelcomeRecovery(lead) {
  const a = lead?.sampleAgent;
  return agentCanReply(lead) && ['collecting', 'awaiting_confirmation'].includes(a.status)
    && !a.brief?.companyName && !a.brief?.activity && !a.confirmationHash
    && !lead.closing && lead.estado !== 'compro' && lead.planDeposit?.status !== 'paid'
    && !lead.stopSequences && !lead.queue?.claimedBy && lead.queue?.status !== 'claimed'
    && !lead.sampleSubmittedAt && !lead.webSales?.sample?.sentAt && !lead.sampleLinkSentAt && !lead.webLinkSentAt;
}
export function createSampleWelcomeRecovery({ db, store, send, now = Date.now }) {
  const jobs = db.collection('WelcomeRecoveryJobs'), sending = new Set();
  async function enroll({ leadRef: ref, version = '', revision = 0, textAlreadySent = false }) {
    const cfg = await store.get(); if (cfg.welcomeRecoveryEnabled === false) return false;
    return db.runTransaction(async tx => {
      const lead = (await tx.get(ref)).data(), previous = (await tx.get(jobs.doc(ref.id))).data();
      if (!eligibleWelcomeRecovery(lead) || lead.welcomeRecovery || previous || (lead.sampleAgent.version || '') !== version || (lead.sampleAgent.inboxRevision || 0) !== revision) return false;
      const at = new Date(now()), stage = textAlreadySent ? 1 : 0;
      tx.update(ref, { welcomeRecovery: { state: 'active', stage, version, revision, startedAt: at, updatedAt: at } });
      tx.set(jobs.doc(ref.id), { leadId: ref.id, stage, version, revision, firstTextAt: textAlreadySent ? at : null, nextAt: nextClosingTime(now() + (textAlreadySent ? cfg.welcomeRecoveryFirstHours * 3600000 : 0), cfg), leaseUntil: new Date(0), updatedAt: at });
      return true;
    });
  }
  function cancelInTransaction(ref, lead, tx) {
    if (lead?.welcomeRecovery?.state !== 'active') return;
    tx.update(ref, { 'welcomeRecovery.state': 'replied', 'welcomeRecovery.updatedAt': new Date(now()) });
    tx.delete(jobs.doc(ref.id));
  }
  async function cancel(ref, state = 'human') {
    await db.runTransaction(async tx => {
      const lead = (await tx.get(ref)).data();
      if (lead?.welcomeRecovery?.state !== 'active') return;
      tx.update(ref, { 'welcomeRecovery.state': state, 'welcomeRecovery.updatedAt': new Date(now()) });
      tx.delete(jobs.doc(ref.id));
    });
  }
  async function recoverRecent() {
    const cfg = await store.get(); if (cfg.welcomeRecoveryEnabled === false) throw Error('Activa la recuperación de bienvenidas antes de programarla.');
    const snap = await db.collection('leads').where('sampleAgent.updatedAt', '>=', new Date(now() - 48 * 3600000)).orderBy('sampleAgent.updatedAt', 'desc').limit(100).get();
    let scheduled = 0;
    for (const doc of snap.docs) {
      const lead = doc.data(), a = lead.sampleAgent;
      if (!eligibleWelcomeRecovery(lead) || !a.audioMomentsSent?.includes('welcome') || a.history?.filter(m => m.role === 'customer').length !== 1) continue;
      if (await enroll({ leadRef: doc.ref, version: a.version || '', revision: a.inboxRevision || 0 })) scheduled++;
    }
    return { scheduled, checked: snap.docs.length };
  }
  let ticking = false;
  async function tick() {
    if (ticking) return; ticking = true;
    try {
      const cfg = await store.get(); if (cfg.welcomeRecoveryEnabled === false) return;
      const snap = await jobs.where('nextAt', '<=', new Date(now())).orderBy('nextAt').limit(15).get();
      for (const doc of snap.docs) {
        const ref = db.collection('leads').doc(doc.id), owner = randomUUID();
        const acquired = await db.runTransaction(async tx => {
          const lead = (await tx.get(ref)).data(), job = (await tx.get(doc.ref)).data();
          if (!job || ms(job.leaseUntil) > now()) return null;
          if (!eligibleWelcomeRecovery(lead) || lead.welcomeRecovery?.state !== 'active' || (lead.sampleAgent.version || '') !== job.version || (lead.sampleAgent.inboxRevision || 0) !== job.revision) {
            if (lead?.welcomeRecovery?.state === 'active') tx.update(ref, { 'welcomeRecovery.state': 'cancelled', 'welcomeRecovery.updatedAt': new Date(now()) });
            tx.delete(doc.ref); return null;
          }
          if (job.sending) { tx.update(ref, { 'welcomeRecovery.state': 'review', 'welcomeRecovery.error': 'Envío sin confirmar; revisa el chat antes de contactar de nuevo.', 'welcomeRecovery.updatedAt': new Date(now()) }); tx.delete(doc.ref); return null; }
          tx.update(doc.ref, { owner, leaseUntil: new Date(now() + 180000) }); return job;
        });
        if (!acquired) continue;
        try {
          const allowed = nextClosingTime(now(), cfg);
          if (+allowed > now() + 1000) { await doc.ref.update({ nextAt: allowed, leaseUntil: new Date(0) }); continue; }
          if ((await store.get()).welcomeRecoveryEnabled === false) continue;
          const canSend = await db.runTransaction(async tx => {
            const lead = (await tx.get(ref)).data(), job = (await tx.get(doc.ref)).data();
            if (!job || job.owner !== owner || !eligibleWelcomeRecovery(lead) || lead.welcomeRecovery?.state !== 'active' || (lead.sampleAgent.inboxRevision || 0) !== job.revision || (lead.sampleAgent.version || '') !== job.version) return false;
            tx.update(doc.ref, { sending: true }); return true;
          });
          if (!canSend) continue;
          // The same process recognizes these outbound echoes as automation, not a human taking over.
          sending.add(ref.id); try { await send(ref.id, messages[acquired.stage]); } finally { sending.delete(ref.id); }
          await db.runTransaction(async tx => {
            const lead = (await tx.get(ref)).data(), job = (await tx.get(doc.ref)).data();
            if (!job || job.owner !== owner || lead.welcomeRecovery?.state !== 'active') return;
            if ((lead.sampleAgent.inboxRevision || 0) !== job.revision) { tx.delete(doc.ref); tx.update(ref, { 'welcomeRecovery.state': 'replied', 'welcomeRecovery.updatedAt': new Date(now()) }); return; }
            const stage = job.stage + 1, firstTextAt = job.firstTextAt || new Date(now());
            tx.update(ref, { 'welcomeRecovery.stage': stage, 'welcomeRecovery.state': stage >= messages.length ? 'finished' : 'active', 'welcomeRecovery.lastSentAt': new Date(now()), 'welcomeRecovery.updatedAt': new Date(now()) });
            if (stage >= messages.length) tx.delete(doc.ref);
            else tx.update(doc.ref, { stage, firstTextAt, sending: false, leaseUntil: new Date(0), nextAt: nextClosingTime(stage === 1 ? now() + cfg.welcomeRecoveryFirstHours * 3600000 : Math.max(ms(firstTextAt) + cfg.welcomeRecoveryLastHours * 3600000, now() + cfg.welcomeRecoveryFirstHours * 3600000), cfg), updatedAt: new Date(now()) });
          });
        } catch (e) {
          await db.runTransaction(async tx => {
            const job = (await tx.get(doc.ref)).data(), lead = (await tx.get(ref)).data();
            if (!job || job.owner !== owner) return;
            if (lead?.welcomeRecovery?.state === 'active') tx.update(ref, { 'welcomeRecovery.state': 'review', 'welcomeRecovery.error': String(e.message).slice(0, 300), 'welcomeRecovery.updatedAt': new Date(now()) });
            tx.delete(doc.ref);
          });
        } finally {
          await db.runTransaction(async tx => { const job = (await tx.get(doc.ref)).data(); if (job?.owner === owner) tx.update(doc.ref, { leaseUntil: new Date(0) }); });
        }
      }
    } finally { ticking = false; }
  }
  return { enroll, cancel, cancelInTransaction, recoverRecent, tick, isSending: id => sending.has(id) };
}
