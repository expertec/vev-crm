import { randomUUID } from 'node:crypto';
import { canGenerateAiSample } from './aiSampleAccess.js';
import { syncAiSampleFailure } from './aiSampleFailure.js';
import { canManageSampleAudios } from './sampleAgentAudioLibrary.js';
import { hasDeliveredSample } from './sampleAgentReactivation.js';
import { summarizeSampleProgress } from './sampleProgress.js';
import { recoverySalesQualification, DEFAULT_RENEWAL_DIRECTION } from './sampleRecovery.js';
import { isWebsiteCampaignIntro } from '../utils/websiteCampaignContext.js';
import { manualSampleContext, cropManualSampleLogo } from './sampleManualContext.js';
import { normalizeConversationBrief, sampleBriefHash, agentCanReply } from './sampleConversationAgent.js';

const DAY = 86400_000;
const clean = (value, max = 500) => String(value || '').trim().slice(0, max);
export const funnelMillis = (value) => value?.toMillis?.() || (value ? +new Date(value) : 0) || 0;
const fail = (message, status = 409) => Object.assign(new Error(message), { status });
const normalize = (value) => clean(value, 4000).normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
export const DEFAULT_FUNNEL_SETTINGS = {
  promotion: '', applyPromotionToRenewal: false, designDirection: DEFAULT_RENEWAL_DIRECTION,
  renewalMessage: 'Preparamos una nueva versión de la página de {{negocio}}, con un diseño más visual y aprovechando lo que nos compartiste. Te envío el enlace para que veas cómo quedó. ¿Qué te parece esta propuesta?',
};
export async function recordProposalDelivery(db, leadRef, cycleId) {
  if (!cycleId) return;
  await db.runTransaction(async (tx) => {
    const lead = (await tx.get(leadRef)).data() || {};
    if (lead.sampleAgent?.regenerationId !== cycleId) return;
    if (lead.sampleProposalCycle?.id === cycleId && lead.sampleProposalCycle.sentAt) return;
    tx.update(leadRef, { sampleProposalCycle: { id: cycleId, requestedAt: lead.sampleAgent.regenerationRequestedAt || new Date(), sentAt: new Date(), openedAt: null } });
  });
}
export async function recordProposalOpen(db, leadRef, cycleId) {
  if (!cycleId) return;
  await db.runTransaction(async (tx) => {
    const lead = (await tx.get(leadRef)).data() || {};
    if (lead.sampleProposalCycle?.id !== cycleId || lead.sampleAgent?.regenerationId !== cycleId || !lead.sampleProposalCycle.sentAt) return;
    tx.update(leadRef, { 'sampleProposalCycle.openedAt': lead.sampleProposalCycle.openedAt || new Date(), 'sampleProposalCycle.lastOpenedAt': new Date() });
  });
}
export function normalizeFunnelSettings(value = {}) {
  return { promotion: clean(value.promotion, 1200), applyPromotionToRenewal: value.applyPromotionToRenewal === true,
    designDirection: clean(value.designDirection, 2000) || DEFAULT_RENEWAL_DIRECTION,
    renewalMessage: clean(value.renewalMessage, 1600) || DEFAULT_FUNNEL_SETTINGS.renewalMessage };
}

export function sampleInterestEvidence(lead, messages = []) {
  if (lead.sampleFunnelOverride?.interested === true) return { reason: 'Interés confirmado por el asesor', text: clean(lead.sampleFunnelOverride.note, 180), at: funnelMillis(lead.sampleFunnelOverride.at) };
  const form = lead.webSales?.sample || {};
  if (form.formCompletedAt || lead.sampleSubmittedAt) return { reason: 'Completó datos para la muestra', at: funnelMillis(form.formCompletedAt || lead.sampleSubmittedAt) };
  if (form.formStartedAt) return { reason: 'Comenzó el formulario', at: funnelMillis(form.formStartedAt) };
  let evidence = null;
  let preceding = '';
  for (const message of messages) {
    if (message.sender === 'business') { preceding = normalize(message.transcription || message.content); continue; }
    if (message.sender !== 'lead') continue;
    const text = normalize(message.transcription || message.content);
    if (isWebsiteCampaignIntro(message.content)) continue;
    const explicit = /\b(?:quiero|quisiera|me interesa|me gustaria|puedes|podrias|hazme|hagamos|prepara|preparame|generar|genera)\b.{0,70}\b(?:muestra|propuesta|demostracion)\b/.test(text)
      || /\b(?:si|ok|adelante)[,! .]*(?:genera|haz|prepara)(?:la|me)?\b/.test(text);
    const accepted = /\b(muestra|formulario|propuesta de (?:tu|la) (?:web|pagina))\b/.test(preceding)
      && /^(?:si|claro|ok|adelante|me interesa|por supuesto|perfecto)(?:[ .,!]|$)/.test(text);
    const assets = /\b(logo|fotos|imagenes)\b/.test(preceding) && /\b(muestra|pagina|web)\b/.test(preceding)
      && ['image', 'video', 'document'].includes(message.mediaType);
    if (explicit || accepted || assets) evidence = { reason: assets ? 'Envió material para su página' : 'Pidió o aceptó preparar una muestra', text: clean(message.transcription || message.content, 180), at: funnelMillis(message.timestamp) };
  }
  if (evidence) return evidence;
  const customerMessages = messages.filter((message) => message.sender === 'lead');
  const onlyGreetings = customerMessages.length && customerMessages.every((message) => isWebsiteCampaignIntro(message.content) || /^(hola|buenos dias|buenas tardes|informacion|info)[! .]*$/.test(normalize(message.content)));
  if (!onlyGreetings && ['wants_sample', 'needs_help_with_sample', 'missing_assets'].includes(lead.salesState?.intent)) return { reason: 'Solicitud de muestra identificada por Sales Brain', at: funnelMillis(lead.salesState.updatedAt) };
  const brief = lead.sampleAgent?.brief || {};
  if (brief.companyName && brief.activity && (brief.objective || brief.offerings?.length)) return { reason: 'Compartió datos concretos para preparar su web', at: 0 };
  return null;
}

export function sampleDeliveryInHistory(messages = []) {
  let previousBusiness = '', result = null;
  const reserved = new Set(['muestra', 'brief', 'form', 'formulario', 'gracias', 'cliente', 'login', 'registro', 'precios', 'planes', 'contacto', 'admin']);
  for (const message of messages) {
    if (message.sender !== 'business') continue;
    const text = clean(message.content, 4000);
    const context = normalize(`${previousBusiness} ${text}`);
    for (const raw of text.match(/https?:\/\/(?:app\.|www\.)?negociosweb\.mx\/[^\s<>]+/gi) || []) {
      try {
        const url = new URL(raw.replace(/[).,!]+$/, ''));
        const path = url.pathname.replace(/\/$/, '').slice(1);
        if (reserved.has(path)) continue;
        const samplePath = /^m\/[A-Za-z0-9_-]{10,24}$/.test(path) || /^[a-z0-9][a-z0-9-]*$/i.test(path);
        if (samplePath && /\b(muestra|propuesta|pagina.{0,30}lista|web.{0,30}lista)\b/.test(context)) result = { url: url.toString(), at: funnelMillis(message.timestamp) };
      } catch { /* Un enlace incompleto no demuestra entrega. */ }
    }
    previousBusiness = text;
  }
  return result;
}

export function classifySampleFunnel(id, lead, messages = [], now = Date.now()) {
  const state = normalize(lead.estado);
  if (lead.archived || lead.archivedAt || lead.isArchived || lead.stopSequences || lead.sampleFunnelOverride?.excluded || ['compro', 'cliente', 'no interesado'].includes(state)
    || ['won', 'deposit_paid'].includes(lead.webSales?.stage) || lead.webSales?.closing?.paidAt || lead.webSales?.closing?.depositPaidAt) return null;
  const inbound = messages.filter((message) => message.sender === 'lead');
  const last = inbound.at(-1);
  const stop = /\b(no me interesa|no me (?:escribas|contactes)|no quiero (?:la |una )?(?:pagina|web|muestra))\b/;
  const decision = [...inbound].reverse().find((message) => stop.test(normalize(message.transcription || message.content)) || /\b(?:quiero|me interesa|genera|prepara).{0,40}\bmuestra\b/.test(normalize(message.transcription || message.content)));
  if (decision && stop.test(normalize(decision.transcription || decision.content))) return null;
  // lastMessageAt puede pertenecer a una secuencia; nunca se usa como respuesta del cliente.
  const lastInboundAt = Math.max(funnelMillis(last?.timestamp), funnelMillis(lead.lastInboundAt));
  const evidence = sampleInterestEvidence(lead, messages);
  const agent = lead.sampleAgent || {};
  const cycle = lead.sampleProposalCycle?.id === agent.regenerationId ? lead.sampleProposalCycle : null;
  const historicalDelivery = sampleDeliveryInHistory(messages);
  const openAt = funnelMillis(cycle?.id ? cycle.lastOpenedAt || cycle.openedAt : lead.webSales?.sample?.lastOpenedAt || lead.linkOpenedAt);
  const lastCustomerAt = Math.max(lastInboundAt, openAt, funnelMillis(lead.webSales?.sample?.formStartedAt), funnelMillis(lead.webSales?.sample?.formCompletedAt));
  const delivered = cycle?.id ? Boolean(cycle.sentAt || agent.status === 'completed') : hasDeliveredSample(lead) || Boolean(!agent.regenerationId && historicalDelivery);
  const sentAt = funnelMillis(cycle?.sentAt || lead.webSales?.sample?.sentAt || lead.siteSentAt || historicalDelivery?.at || (delivered ? lead.sampleLinkSentAt || lead.webLinkSentAt : null));
  const working = agent.status === 'generating';
  let stage;
  if (working || (!delivered && agent.status === 'error' && agent.negocioId)) stage = 'process';
  else if (delivered) stage = 'proposals';
  else if (['collecting', 'awaiting_confirmation', 'paused', 'error'].includes(agent.status) && lastCustomerAt && now - lastCustomerAt < DAY) stage = 'process';
  else if (!evidence) return null;
  else if (!lastCustomerAt) return null;
  else if (funnelMillis(agent.regenerationRequestedAt) > now - DAY) stage = 'process';
  else stage = now - lastCustomerAt >= DAY ? 'interested' : 'process';
  const progress = summarizeSampleProgress(id, lead, now);
  const objection = lead.salesState?.objection;
  const labels = { price: 'Consultó el precio', trust: 'Dudas de confianza', time: 'Lo dejó para después', needs_approval: 'Pendiente de autorización', not_ready: 'No estaba listo' };
  const outcome = ['sample_change_request', 'concern_about_final_quality'].includes(lead.salesState?.intent) ? 'Pidió cambios' : labels[objection] || (openAt ? 'Abrió la muestra, sin compra' : 'Sin apertura registrada');
  const human = Boolean(lead.humanControl || lead.salesBrainHumanControl);
  return { id, stage, name: clean(lead.nombre || lead.telefono || id, 120), phone: clean(lead.telefono, 30),
    business: clean(agent.brief?.companyName || lead.briefWeb?.companyName || lead.briefWeb?.companyInfo, 120),
    lastCustomerAt, lastInboundAt, sentAt, openAt, delivered, evidence,
    qualification: recoverySalesQualification(lead), progress: progress?.progress || 0, readiness: progress?.readiness || null,
    pending: working ? 'Generando muestra' : delivered ? outcome : progress?.pending || 'Recuperar contexto y completar datos',
    status: human ? 'Atención humana' : agent.status === 'error' ? 'Requiere revisión' : agent.status === 'paused' ? 'Pausado' : working ? 'Generando' : delivered ? 'Muestra enviada' : 'Recopilando',
    canGenerate: !delivered && !human && Boolean(progress?.canForce),
    canResume: !working && !human && !delivered && funnelMillis(agent.leaseUntil) <= now,
    canRenew: delivered && !working && !human,
    sampleUrl: /^https:\/\/(?:app\.)?negociosweb\.mx\//.test(lead.sampleLinkSentUrl || '') && !/\/muestra\//.test(lead.sampleLinkSentUrl) ? lead.sampleLinkSentUrl : historicalDelivery?.url || '',
    error: clean(agent.error, 300), lastReply: progress?.lastReply || '',
  };
}

const dayFormatter = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Monterrey', year: 'numeric', month: '2-digit', day: '2-digit' });
const dayKey = (timestamp) => timestamp ? dayFormatter.format(new Date(timestamp)) : '';
export function sortSampleFunnel(rows, order = 'priority') {
  return [...rows].sort((a, b) => {
    const score = (b.qualification?.score ?? -1) - (a.qualification?.score ?? -1);
    const recent = b.lastCustomerAt - a.lastCustomerAt;
    if (order === 'score') return score || recent || a.id.localeCompare(b.id);
    if (order === 'recent') return recent || score || a.id.localeCompare(b.id);
    return dayKey(b.lastCustomerAt).localeCompare(dayKey(a.lastCustomerAt)) || score || recent || a.id.localeCompare(b.id);
  });
}

export function registerSampleFunnelRoutes(app, { db, readUser, reactivate, renew, cancelSequences = async () => {}, resolveLead, prepareLogo }) {
  const historyCache = new Map(), listCache = new Map();
  const authorize = async (req) => { const user = await readUser(req); if (!user) throw fail('Inicia sesión para consultar el embudo.', 401); if (!canGenerateAiSample(user, { assignedTo: user.uid })) throw fail('No tienes acceso al embudo.', 403); return user; };
  const route = (method, name, handler) => app[method](`/api/crm/sample-funnel/${name}`, async (req, res) => {
    try { const user = await authorize(req); res.set?.('Cache-Control', 'no-store'); return res.json(await handler(req, user)); }
    catch (error) { return res.status(error.status || 500).json({ error: error.message || 'No se pudo completar la acción.' }); }
  });
  const contact = async (id, user) => {
    if (!id || id.includes('/') || id.length > 200) throw fail('Contacto no válido.', 400);
    const ref = db.collection('leads').doc(id), snap = await ref.get();
    if (!snap.exists) throw fail('Contacto no encontrado.', 404);
    const lead = snap.data();
    if (!canGenerateAiSample(user, lead)) throw fail('No tienes acceso a este contacto.', 403);
    return { ref, lead };
  };
  const history = async (ref, lead) => {
    const key = `${ref.id}:${funnelMillis(lead.lastMessageAt)}:${funnelMillis(lead.lastInboundAt)}`;
    if (historyCache.has(key)) return historyCache.get(key);
    const snap = await ref.collection('messages').orderBy('timestamp', 'desc').limit(50).get();
    const messages = snap.docs.map((doc) => doc.data()).reverse();
    if (historyCache.size > 3000) historyCache.clear();
    historyCache.set(key, messages); return messages;
  };
  const getSettings = async () => normalizeFunnelSettings((await db.collection('config').doc('sampleFunnelSettings').get()).data());
  const contextVersion = (lead) => sampleBriefHash({ brief: lead.sampleAgent?.brief || {}, images: lead.sampleAgent?.images || [], version: lead.sampleAgent?.version || '', inboxRevision: lead.sampleAgent?.inboxRevision || 0, lastInboundAt: funnelMillis(lead.lastInboundAt) });
  route('get', 'context', async (req, user) => {
    const { lead } = await contact(String(req.query.leadId || ''), user);
    return { revision: contextVersion(lead), brief: normalizeConversationBrief(lead.sampleAgent?.brief || {}, lead.sampleAgent?.images || []), images: (lead.sampleAgent?.images || []).map((image, index) => ({ index, url: image.sourceUrl || image.url, caption: image.caption || lead.sampleAgent?.brief?.images?.find((item) => item.index === index)?.description || `Imagen ${index + 1}` })) };
  });
  route('post', 'context', async (req, user) => {
    const { ref, lead: initial } = await contact(String(req.body.leadId || ''), user);
    const assertEditable = (lead) => {
      if (!canGenerateAiSample(user, lead)) throw fail('No tienes acceso a este contacto.', 403);
      if (!lead.sampleAgent || hasDeliveredSample(lead) || !agentCanReply({ ...lead, sampleAgent: { ...lead.sampleAgent, enabled: true } })) throw fail('Este contacto no admite cambios de datos para la muestra.');
      if (lead.sampleAgent.status === 'generating' || funnelMillis(lead.sampleAgent.leaseUntil) > Date.now()) throw fail('Espera a que termine el proceso actual antes de editar.');
      if (req.body.revision !== contextVersion(lead)) throw fail('El contexto cambió. Cierra y vuelve a abrir los datos para conservar la información reciente.');
    };
    assertEditable(initial);
    manualSampleContext(initial, req.body.fields || {});
    if (req.body.logoCrop && req.body.fields?.withoutLogo) throw fail('Elige usar el recorte o continuar sin logo.', 400);
    const cropped = req.body.logoCrop ? await cropManualSampleLogo(initial, req.body.logoCrop, prepareLogo) : null;
    await db.runTransaction(async (tx) => {
      const lead = (await tx.get(ref)).data() || {};
      assertEditable(lead);
      const brief = manualSampleContext(cropped || lead, req.body.fields || {});
      tx.update(ref, { 'sampleAgent.brief': brief, ...(cropped ? { 'sampleAgent.images': cropped.sampleAgent.images } : {}), 'sampleAgent.version': randomUUID(), 'sampleAgent.enabled': false, 'sampleAgent.status': 'paused', 'sampleAgent.confirmationHash': '', 'sampleAgent.error': '', 'sampleAgent.updatedAt': new Date(), 'sampleAgent.manualContextUpdatedAt': new Date(), 'sampleAgent.manualContextUpdatedBy': user.uid });
    });
    await cancelSequences(ref.id);
    listCache.clear();
    return { success: true };
  });
  route('get', 'settings', async () => ({ settings: await getSettings() }));
  route('post', 'settings', async (req, user) => {
    if (!canManageSampleAudios(user)) throw fail('Solo un administrador puede configurar los mensajes.', 403);
    const settings = normalizeFunnelSettings(req.body || {});
    await db.collection('config').doc('sampleFunnelSettings').set({ ...settings, updatedBy: user.uid, updatedAt: new Date() });
    return { settings };
  });
  route('get', 'list', async (req, user) => {
    const days = [7, 30, 90, 365, 3650].includes(Number(req.query.days)) ? Number(req.query.days) : 90;
    const cacheKey = `${user.uid}:${days}`;
    if (req.query.refresh === '1') listCache.delete(cacheKey);
    let cached = listCache.get(cacheKey);
    if (!cached || cached.expires < Date.now()) {
      const snap = await db.collection('leads').where('lastMessageAt', '>=', new Date(Date.now() - days * DAY)).orderBy('lastMessageAt', 'desc').limit(2001).get();
      const docs = snap.docs.slice(0, 2000).filter((doc) => canGenerateAiSample(user, doc.data()));
      const rows = [];
      // Solo consulta contexto guardado; no llama a IA ni recalifica al abrir la tabla.
      for (let index = 0; index < docs.length; index += 20) {
        const batch = await Promise.all(docs.slice(index, index + 20).map(async (doc) => {
          let lead = doc.data();
          if (lead.archived || lead.archivedAt || lead.stopSequences || ['compro', 'cliente', 'no interesado'].includes(normalize(lead.estado))) return null;
          if (lead.sampleAgent?.status === 'generating' && lead.sampleAgent?.negocioId) lead = await syncAiSampleFailure(db, doc.ref);
          return classifySampleFunnel(doc.id, lead, await history(doc.ref, lead));
        }));
        rows.push(...batch.filter(Boolean));
      }
      cached = { rows, truncated: snap.docs.length > 2000, expires: Date.now() + 30_000 };
      if (listCache.size > 20) listCache.clear();
      listCache.set(cacheKey, cached);
    }
    const stage = ['process', 'interested', 'proposals'].includes(req.query.stage) ? req.query.stage : 'process';
    const search = normalize(req.query.search), minimum = req.query.minimum === '' || req.query.minimum == null ? -1 : Number(req.query.minimum);
    const filtered = cached.rows.filter((row) => (!search || normalize(`${row.name} ${row.business} ${row.phone}`).includes(search)) && (row.qualification.score ?? -1) >= minimum);
    const counts = Object.fromEntries(['process', 'interested', 'proposals'].map((value) => [value, filtered.filter((row) => row.stage === value).length]));
    const rows = sortSampleFunnel(filtered.filter((row) => row.stage === stage), req.query.order);
    const page = Math.max(0, Math.floor(Number(req.query.page) || 0));
    return { rows: rows.slice(page * 25, (page + 1) * 25), counts, total: rows.length, page, hasMore: (page + 1) * 25 < rows.length, truncated: cached.truncated };
  });
  route('post', 'action', async (req, user) => {
    const body = req.body || {};
    const { ref, lead } = await contact(clean(body.leadId, 200), user);
    let result;
    if (body.action === 'takeover') {
      if (lead.stopSequences || ['compro', 'cliente', 'no interesado'].includes(normalize(lead.estado)) || ['won', 'deposit_paid'].includes(lead.webSales?.stage) || lead.webSales?.closing?.paidAt || lead.webSales?.closing?.depositPaidAt) throw fail('Este contacto no admite seguimiento desde el embudo.');
      await ref.update({ 'sampleAgent.enabled': false, 'sampleAgent.version': randomUUID(), 'sampleAgent.status': 'paused', 'sampleAgent.deliveryPaused': true, 'sampleAgent.pauseReason': 'manual_conversation', 'sampleAgent.leaseOwner': '', 'sampleAgent.leaseUntil': new Date(0), salesBrainMode: 'copilot' });
      await cancelSequences(ref.id);
      result = { success: true };
    } else {
      const row = classifySampleFunnel(ref.id, lead, await history(ref, lead));
      if (!row) throw fail('Este contacto ya no está disponible en el embudo.');
      const settings = await getSettings();
      if (body.action === 'reactivate' && row.stage === 'interested' && row.canResume) result = await reactivate(ref.id, user, settings);
      else if (body.action === 'renew' && row.canRenew) result = await renew(ref.id, user, settings);
      else if (body.action === 'exclude') {
        await ref.update({ sampleFunnelOverride: { excluded: true, by: user.uid, at: new Date() }, 'sampleAgent.enabled': false, 'sampleAgent.version': randomUUID(), 'sampleAgent.status': 'paused', 'sampleAgent.deliveryPaused': true });
        await cancelSequences(ref.id); result = { success: true };
      } else if (body.action === 'interest') {
        await ref.update({ sampleFunnelOverride: { interested: true, at: new Date(), by: user.uid, note: clean(body.note, 180) } }); result = { success: true };
      } else throw fail('La etapa cambió o esta acción no está disponible. Actualiza la tabla.');
    }
    listCache.clear();
    return result;
  });
  route('post', 'include', async (req, user) => {
    const identity = clean(req.body?.identity, 200);
    if (!identity) throw fail('Indica el teléfono del contacto.', 400);
    const found = resolveLead ? await resolveLead(identity) : { id: identity };
    if (!found?.id) throw fail('No se encontró el contacto.', 404);
    const { ref, lead } = await contact(found.id, user);
    if (lead.stopSequences || lead.archived || ['compro', 'cliente', 'no interesado'].includes(normalize(lead.estado)) || ['won', 'deposit_paid'].includes(lead.webSales?.stage) || lead.webSales?.closing?.paidAt || lead.webSales?.closing?.depositPaidAt) throw fail('Este contacto no admite seguimiento.');
    await ref.update({ sampleFunnelOverride: { interested: true, at: new Date(), by: user.uid, note: 'Incluido manualmente por el asesor' } });
    listCache.clear(); return { success: true };
  });
  return { invalidate: () => listCache.clear() };
}
