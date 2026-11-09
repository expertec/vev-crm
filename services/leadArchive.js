import { createHmac, randomBytes, randomUUID, timingSafeEqual } from 'node:crypto';

const DAY = 86400000;
const PREVIEW_PAGE_SIZE = 20;
const PREVIEW_CONCURRENCY = 4;
const clean = value => String(value || '').trim().toLowerCase();
const closed = value => ['done', 'completed', 'completado', 'completada', 'cancelled', 'canceled', 'cancelado', 'cancelada', 'closed', 'finished', 'sent', 'enviado'].includes(clean(value));
export const isLeadArchived = lead => Boolean(lead?.archived || lead?.isArchived || lead?.archivedAt);
export function archiveMillis(value) {
  if (value?.toMillis) return value.toMillis();
  if (value?.seconds !== undefined) return Number(value.seconds) * 1000;
  if (value?._seconds !== undefined) return Number(value._seconds) * 1000;
  if (!value) return 0;
  const result = value instanceof Date ? +value : typeof value === 'number' ? value : Date.parse(value);
  return Number.isFinite(result) ? result : 0;
}
const fail = (message, status = 400) => Object.assign(Error(message), { status });
export function canManageLeadArchive(user) {
  return Boolean(user && user.active !== false && ['admin', 'superadmin'].includes(clean(user.role).replace(/_/g, '')));
}
function canAccess(user, lead) {
  return canManageLeadArchive(user) || Boolean(user?.active !== false && user?.uid
    && ['salesagent', 'sales_agent'].includes(clean(user.role))
    && (lead.assignedTo === user.uid || lead.salesOwner === user.uid));
}

// An outbound follow-up is deliberately not a customer interaction.
export function inactivityArchiveReason(lead, { now, days = 30, lastInboundAt, manual = false } = {}) {
  if (isLeadArchived(lead)) return 'Ya archivado';
  if (lead.mergedInto) return 'Contacto fusionado';
  const status = clean(lead.estado).normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  const tags = (Array.isArray(lead.etiquetas) ? lead.etiquetas : []).map(clean);
  if (['compro', 'cliente', 'ganado', 'won', 'closed_won', 'cerrado_ganado', 'pagado'].includes(status)
    || tags.some(t => ['compro', 'cliente', 'pagado'].includes(t)) || lead.isCustomer) return 'Cliente o venta registrada';
  if (lead.planDeposit || lead.paymentPending || lead.paidAt || lead.subscriptionId) return 'Pago o suscripción registrada';
  if (lead.queue?.status === 'claimed' || lead.queue?.claimedBy) return 'Atención en curso';
  if (lead.humanControl || lead.salesBrainHumanControl) return 'Atención manual';
  if (['pending', 'scheduled', 'active'].includes(clean(lead.followUp?.status))
    || archiveMillis(lead.followUp?.nextAt) > now || archiveMillis(lead.nextFollowUpAt) > now
    || archiveMillis(lead.appointmentAt) > now) return 'Cita o seguimiento pendiente';
  if (['generating', 'queued', 'running', 'sending', 'review', 'awaiting_review'].includes(clean(lead.sampleAgent?.status))
    || ['queued', 'running', 'sending', 'review', 'awaiting_review'].includes(clean(lead.aiSampleJob?.state))
    || archiveMillis(lead.sampleAgent?.leaseUntil) > now || archiveMillis(lead.sequenceLockUntil) > now
    || archiveMillis(lead.sequenceLock) > now - 5 * 60000) return 'Muestra o procesamiento en curso';
  if (lead.closing?.state && !['stopped', 'cancelled', 'finished'].includes(clean(lead.closing.state))) return 'Cierre comercial pendiente';
  if (!manual) {
    const at = Math.max(archiveMillis(lead.lastInboundAt), archiveMillis(lastInboundAt));
    if (!at || at > now) return 'Sin fecha de respuesta confiable';
    if (at > now - days * DAY) return 'Interacción reciente';
    if (Number(lead.unreadCount || 0) > 0) return 'Mensajes sin atender';
  }
  return '';
}

function activeTask(task) { return task.active !== false && !closed(task.status); }
function protectedRelated(context, now) {
  if (context.overflow) return 'Historial extenso: requiere revisión individual';
  if (context.tasks.some(activeTask)) return 'Tarea pendiente';
  if (context.deposits.some(d => !['cancelled', 'canceled', 'failed', 'expired'].includes(clean(d.status)))) return 'Pago pendiente o recibido';
  if (context.businesses.some(b => ['activo', 'active', 'pagado', 'cliente'].includes(clean(b.status))
    || b.planActivatedAt || b.subscriptionId || b.paidAt
    || ['queued', 'running', 'sending', 'review', 'awaiting_review'].includes(clean(b.aiSampleJob?.state))
    || ['sin procesar', 'procesando'].includes(clean(b.status)))) return 'Negocio activo o muestra pendiente';
  if (context.jobs.some(j => clean(j.jobType) === 'reminder' && !closed(j.status))) return 'Recordatorio pendiente';
  if (context.jobs.some(j => archiveMillis(j.leaseUntil) > now || ['processing', 'sending'].includes(clean(j.status)))) return 'Envío en curso';
  return '';
}

export function createLeadArchiveService({ db, FieldValue, now = Date.now,
  secret = process.env.LEAD_ARCHIVE_PREVIEW_SECRET || randomBytes(32).toString('hex') }) {
  const sign = data => createHmac('sha256', secret).update(data).digest('base64url');
  const tokenFor = (id, days, at) => {
    const data = Buffer.from(JSON.stringify({ id, days, at, expires: now() + 30 * 60000 })).toString('base64url');
    return `${data}.${sign(data)}`;
  };
  function verify(token, { allowExpired = false } = {}) {
    if (typeof token !== 'string' || token.length > 2048) throw fail('Vista previa inválida.');
    const [data, signature, extra] = token.split('.');
    const expected = Buffer.from(sign(data || '')), actual = Buffer.from(signature || '');
    if (extra || expected.length !== actual.length || !timingSafeEqual(expected, actual)) throw fail('Actualiza la vista previa.', 409);
    let decoded; try { decoded = JSON.parse(Buffer.from(data, 'base64url').toString()); } catch { throw fail('Vista previa inválida.'); }
    if ((!allowExpired && decoded.expires < now()) || decoded.days !== 30 || typeof decoded.id !== 'string' || !decoded.id || decoded.id.includes('/')) throw fail('Actualiza la vista previa.', 409);
    return decoded;
  }
  const query = (name, field, value) => db.collection(name).where(field, '==', value).limit(101);
  async function contextFor(reader, ref, lead) {
    const read = source => reader.get(source);
    const businessId = lead.sampleAgent?.negocioId || lead.negocioId;
    // Older tasks and businesses sometimes use the phone instead of a leadId.
    const phone = String(lead.telefono || '').replace(/\D/g, '');
    const empty = { docs: [] };
    const [messages, inbox, tasks, deposits, businesses, jobs, direct, phoneTasks, phoneBusinesses, phoneJobs] = await Promise.all([
      read(ref.collection('messages').orderBy('timestamp', 'desc').limit(80)),
      read(ref.collection('sampleAgentInbox').where('state', '==', 'pending').limit(101)),
      read(query('tasks', 'leadId', ref.id)),
      read(query('PlanDepositRequests', 'leadId', ref.id)),
      read(query('Negocios', 'leadId', ref.id)),
      read(query('sequenceQueue', 'leadId', ref.id)),
      businessId && !String(businessId).includes('/') ? read(db.collection('Negocios').doc(String(businessId))) : null,
      phone ? read(query('tasks', 'leadPhone', phone)) : empty,
      phone ? read(query('Negocios', 'leadPhone', phone)) : empty,
      phone ? read(query('sequenceQueue', 'phone', phone)) : empty,
    ]);
    const snapshots = [tasks, deposits, businesses, jobs, phoneTasks, phoneBusinesses, phoneJobs, inbox];
    const dedupe = items => [...new Map(items.map(d => [d.ref.path, d])).values()];
    return {
      lastInboundAt: Math.max(0, ...messages.docs.filter(doc => doc.data().sender === 'lead').map(doc => archiveMillis(doc.data().timestamp))),
      inboxDocs: inbox.docs,
      tasks: dedupe([...tasks.docs, ...phoneTasks.docs]).map(d => d.data()),
      deposits: deposits.docs.map(d => d.data()),
      businesses: dedupe([...businesses.docs, ...phoneBusinesses.docs, ...(direct?.exists ? [direct] : [])]).map(d => d.data()),
      jobDocs: dedupe([...jobs.docs, ...phoneJobs.docs]),
      jobs: dedupe([...jobs.docs, ...phoneJobs.docs]).map(d => d.data()),
      overflow: snapshots.some(s => s.docs.length === 101),
    };
  }
  const plainReader = { get: ref => ref.get() };
  async function preview({ cursor = '' } = {}, user) {
    if (!canManageLeadArchive(user)) throw fail('Solo administradores pueden archivar por lotes.', user ? 403 : 401);
    if (typeof cursor !== 'string' || cursor.includes('/') || cursor.length > 200) throw fail('Página inválida.');
    let source = db.collection('leads').orderBy('__name__');
    if (cursor) source = source.startAfter(cursor);
    const snapshot = await source.limit(PREVIEW_PAGE_SIZE).get();
    // Bound database load while avoiding a full page of serial network round trips.
    const reviewed = [];
    for (let offset = 0; offset < snapshot.docs.length; offset += PREVIEW_CONCURRENCY) {
      reviewed.push(...await Promise.all(snapshot.docs.slice(offset, offset + PREVIEW_CONCURRENCY).map(async doc => {
        const lead = doc.data();
        let reason = inactivityArchiveReason(lead, { now: now(), lastInboundAt: lead.lastInboundAt });
        if (reason && reason !== 'Sin fecha de respuesta confiable') return { reason };
        // Failure to verify a related record excludes the contact; it never authorizes archiving.
        try {
          const context = await contextFor(plainReader, doc.ref, lead);
          const at = Math.max(archiveMillis(lead.lastInboundAt), context.lastInboundAt);
          reason = inactivityArchiveReason(lead, { now: now(), lastInboundAt: at }) || protectedRelated(context, now());
          if (!reason) return { candidate: { id: doc.id, name: lead.nombre || lead.businessName || lead.telefono || doc.id,
            phone: lead.telefono || '', lastInboundAt: at, reason: '30 días sin respuesta del cliente', token: tokenFor(doc.id, 30, at) } };
        } catch { reason = 'No se pudo verificar: revisar individualmente'; }
        return { reason };
      })));
    }
    const candidates = [], excluded = {};
    for (const { candidate, reason } of reviewed) {
      if (candidate) candidates.push(candidate);
      if (reason) excluded[reason] = (excluded[reason] || 0) + 1;
    }
    return { candidates, excluded, scanned: snapshot.size ?? snapshot.docs.length,
      nextCursor: snapshot.docs.length === PREVIEW_PAGE_SIZE ? snapshot.docs.at(-1).id : null };
  }
  async function archiveOne(id, user, { manual = false, expectedAt } = {}) {
    if (typeof id !== 'string' || !id || id.includes('/') || id.length > 200) throw fail('Contacto inválido.');
    const ref = db.collection('leads').doc(id);
    return db.runTransaction(async tx => {
      const snap = await tx.get(ref);
      if (!snap.exists) throw fail('Contacto no encontrado.', 404);
      const lead = snap.data();
      if (!canAccess(user, lead)) throw fail('No tienes acceso a este contacto.', user ? 403 : 401);
      if (isLeadArchived(lead)) return { id, archived: false, reason: 'Ya archivado' };
      const context = await contextFor(tx, ref, lead);
      const at = Math.max(archiveMillis(lead.lastInboundAt), context.lastInboundAt);
      const reason = inactivityArchiveReason(lead, { now: now(), lastInboundAt: at, manual }) || protectedRelated(context, now());
      if (reason || !manual && at !== expectedAt) return { id, archived: false, reason: reason || 'El cliente volvió a responder; actualiza la vista previa' };
      const date = new Date(now()), history = ref.collection('archiveHistory').doc(randomUUID());
      tx.set(history, { archivedAt: date, archivedBy: user.uid, reason: manual ? 'manual' : 'inactive_30_days',
        lastInboundAt: at ? new Date(at) : null, previous: {
          stopSequences: lead.stopSequences === true, seqPaused: lead.seqPaused === true,
          secuenciasActivas: lead.secuenciasActivas || [], sampleAgentEnabled: lead.sampleAgent?.enabled === true,
          sequenceRecoveredReason: lead.sequenceRecoveredReason || null, sequenceBlockedReason: lead.sequenceBlockedReason || null,
          sequencePausedReason: lead.sequencePausedReason || null, queueStatus: lead.queue?.status || null,
        } });
      tx.update(ref, {
        archived: true, isArchived: true, archivedAt: date, archivedBy: user.uid,
        archiveReason: manual ? 'manual' : 'inactive_30_days',
        archiveMeta: { historyId: history.id, previousStopSequences: lead.stopSequences === true },
        stopSequences: true, seqPaused: true, hasActiveSequences: false, secuenciasActivas: null,
        nextSequenceRunAt: FieldValue.delete(), sequenceRecoveredReason: FieldValue.delete(),
        sequenceBlockedReason: FieldValue.delete(), sequencePausedReason: FieldValue.delete(),
        'sampleAgent.enabled': false, 'sampleAgent.deliveryPaused': true, 'sampleAgent.status': 'archived',
        'sampleAgent.version': randomUUID(), 'sampleAgent.leaseOwner': '', 'sampleAgent.leaseUntil': new Date(0),
        'welcomeRecovery.state': 'cancelled', 'closing.state': 'stopped',
        'queue.status': 'closed', 'routing.status': 'closed', 'salesBrainCurrent.routing.status': 'closed',
        'salesState.qualification.readyForSales': false, 'salesState.readyForSales': false,
        'salesState.qualification.qualificationStatus': 'archived', updatedAt: date,
      });
      tx.delete(db.collection('WelcomeRecoveryJobs').doc(id));
      tx.delete(db.collection('ClosingJobs').doc(id));
      for (const entry of context.inboxDocs) tx.update(entry.ref, { state: 'archived', archivedAt: date });
      for (const job of context.jobDocs) if (!closed(job.data().status)) tx.update(job.ref, { status: 'canceled', processedAt: date, error: 'Lead archivado' });
      return { id, archived: true };
    });
  }
  async function commit({ tokens } = {}, user) {
    if (!canManageLeadArchive(user)) throw fail('Solo administradores pueden archivar por lotes.', user ? 403 : 401);
    if (!Array.isArray(tokens) || !tokens.length || tokens.length > 25) throw fail('Selecciona entre 1 y 25 contactos por lote.');
    const verified = tokens.map(verify);
    const results = [];
    const unique = [...new Map(verified.map(v => [v.id, v])).values()];
    for (let offset = 0; offset < unique.length; offset += 4) {
      results.push(...await Promise.all(unique.slice(offset, offset + 4).map(async item => {
        try { return await archiveOne(item.id, user, { expectedAt: item.at }); }
        catch (error) { return { id: item.id, archived: false, reason: error.status === 403 ? error.message : 'No se pudo verificar o archivar; vuelve a revisar' }; }
      })));
    }
    return { results };
  }
  // Long analyses can outlive preview tokens. Renew only their signed original
  // interaction date; commit still checks all current protections transactionally.
  async function refresh({ tokens } = {}, user) {
    if (!canManageLeadArchive(user)) throw fail('Solo administradores pueden archivar por lotes.', user ? 403 : 401);
    if (!Array.isArray(tokens) || !tokens.length || tokens.length > 25) throw fail('Selecciona entre 1 y 25 contactos por lote.');
    return { tokens: tokens.map(token => {
      const item = verify(token, { allowExpired: true });
      return tokenFor(item.id, item.days, item.at);
    }) };
  }
  async function restore(id, user) {
    if (typeof id !== 'string' || !id || id.includes('/') || id.length > 200) throw fail('Contacto inválido.');
    const ref = db.collection('leads').doc(id);
    return db.runTransaction(async tx => {
      const lead = (await tx.get(ref)).data();
      if (!lead) throw fail('Contacto no encontrado.', 404);
      if (!canAccess(user, lead)) throw fail('No tienes acceso a este contacto.', user ? 403 : 401);
      if (!isLeadArchived(lead)) return { id, restored: false };
      tx.update(ref, leadRestorePatch(lead, new Date(now()), user.uid));
      return { id, restored: true };
    });
  }
  return { preview, commit, refresh, archiveOne, restore };
}

export function leadRestorePatch(lead, date, by) {
  return { archived: false, isArchived: false, archivedAt: null, archivedBy: '', archiveReason: null,
    restoredAt: date, restoredBy: by, stopSequences: lead.archiveMeta ? lead.archiveMeta.previousStopSequences === true : lead.stopSequences === true,
    seqPaused: true, humanControl: true, salesBrainHumanControl: true,
    'sampleAgent.enabled': false, 'sampleAgent.deliveryPaused': true,
    'queue.status': 'waiting', 'routing.status': 'ready_for_agent', updatedAt: date };
}

export async function restoreArchivedLeadOnInbound({ db, leadRef, messageId, receivedAt }) {
  const received = archiveMillis(receivedAt);
  if (!messageId || !received) return false;
  return db.runTransaction(async tx => {
    const snap = await tx.get(leadRef), lead = snap.data();
    if (!lead || !isLeadArchived(lead) || received <= archiveMillis(lead.archivedAt)) return false;
    const known = await tx.get(leadRef.collection('messages').doc(messageId));
    if (known.exists) return false;
    tx.update(leadRef, { ...leadRestorePatch(lead, receivedAt, 'customer_reply'), lastInboundAt: receivedAt });
    return true;
  });
}

export function registerLeadArchiveRoutes(app, { service, readUser }) {
  const route = (path, handler) => app.post(`/api/crm/lead-archive/${path}`, async (req, res) => {
    try { const user = await readUser(req); if (!user || user.active === false) throw fail('Inicia sesión con una cuenta activa.', 401);
      res.json(await handler(req.body || {}, user));
    } catch (error) { res.status(error.status || 500).json({ error: error.status ? error.message : 'No se pudo completar el archivado. Intenta de nuevo.' }); }
  });
  route('preview', (body, user) => service.preview(body, user));
  route('refresh', (body, user) => service.refresh(body, user));
  route('commit', (body, user) => service.commit(body, user));
  route('archive', (body, user) => service.archiveOne(body.leadId, user, { manual: true }));
  route('restore', (body, user) => service.restore(body.leadId, user));
}
