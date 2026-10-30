import { randomUUID, createHash } from 'node:crypto';
import { canGenerateAiSample } from './aiSampleAccess.js';
import { agentCanReply, normalizeConversationBrief, missingBriefQuestion, briefSummary, extractConversationBrief } from './sampleConversationAgent.js';
import { readReactivationMessages, reactivationSource, buildReactivationPreview, hasDeliveredSample } from './sampleAgentReactivation.js';
import { manualSampleInput, summarizeSampleProgress } from './sampleProgress.js';
import { publicImageUrl } from './aiSampleGenerator.js';

const clean = (value, max = 2000) => String(value || '').trim().slice(0, max);
const millis = (value) => value?.toMillis?.() || (value ? +new Date(value) : 0) || 0;
const hash = (value) => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const fail = (message, status = 409) => Object.assign(new Error(message), { status });
export const DEFAULT_RENEWAL_DIRECTION = 'Renueva la muestra con el diseño actual: marca protagonista, hero de gran impacto, servicios concretos, iconos y CTA visibles. Genera tres imágenes coherentes con el negocio y aprovecha las fotos reales. Conserva los colores de la marca, los datos y las correcciones del cliente. No inventes testimonios, cifras, promociones del negocio ni garantías.';

export function recoverySalesQualification(lead) {
  const state = lead.salesState || {};
  const raw = state.leadScore;
  const score = (typeof raw === 'number' || typeof raw === 'string' && raw.trim() !== '') && Number.isFinite(Number(raw))
    ? Math.max(0, Math.min(100, Number(raw))) : null;
  return { score, readyForSales: (state.qualification?.readyForSales ?? state.readyForSales ?? lead.salesBrainCurrent?.readyForSales) === true,
    status: clean(state.qualification?.qualificationStatus, 60), evaluatedAt: millis(state.updatedAt || lead.salesScoreState?.updatedAt) };
}

export function assertSampleRecoveryAllowed(user, lead) {
  if (!canGenerateAiSample(user, lead)) throw fail('No tienes acceso a este contacto.', 403);
  if (!agentCanReply({ ...lead, sampleAgent: { ...lead.sampleAgent, enabled: true } })) throw fail('Este contacto canceló, está bloqueado o está en atención humana.');
  if (lead.sampleAgent?.status === 'generating' || millis(lead.sampleAgent?.leaseUntil) > Date.now()) throw fail('El contacto ya está siendo procesado.');
}

export function previousSampleFacts(lead, business = {}) {
  const previous = lead.briefWeb || {};
  return {
    companyInfo: clean(business.companyInfo || previous.companyInfo || previous.companyName, 120),
    businessStory: clean(business.businessStory || previous.businessStory, 4000),
    businessObjective: clean(business.businessObjective || previous.businessObjective || previous.objective, 500),
    keyItems: (Array.isArray(business.keyItems) ? business.keyItems : Array.isArray(previous.keyItems) ? previous.keyItems : []).map((value) => clean(value, 100)).slice(0, 8),
    logoURL: publicImageUrl(business.logoURL || previous.logoURL),
    photoURLs: (Array.isArray(business.photoURLs) ? business.photoURLs : Array.isArray(previous.photoURLs) ? previous.photoURLs : []).map(publicImageUrl).filter(Boolean).slice(0, 3),
    colorPreference: clean(business.colorPreference || previous.colorPreference, 500),
    brandPalette: (Array.isArray(business.brandPalette) ? business.brandPalette : Array.isArray(previous.brandPalette) ? previous.brandPalette : []).filter((value) => /^#[0-9a-f]{6}$/i.test(value)).slice(0, 5),
  };
}

export async function buildSampleRecoveryPreview({ lead, business, messages, transcribe, extract = extractConversationBrief }) {
  const previous = previousSampleFacts(lead, business);
  const images = [...(lead.sampleAgent?.images || [])];
  const raw = { companyName: previous.companyInfo, activity: previous.businessStory, objective: previous.businessObjective,
    offerings: previous.keyItems, colorPreference: previous.colorPreference, palette: previous.brandPalette, ...lead.sampleAgent?.brief };
  const roles = [...(raw.images || [])];
  for (const [url, role] of [[previous.logoURL, 'logo'], ...previous.photoURLs.map((url) => [url, 'product'])]) {
    if (!url || images.some((image) => image.url === url)) continue;
    const index = images.length;
    if (index >= 12) break;
    images.push({ url, caption: 'Material guardado en la muestra anterior; revisar uso y contenido.' });
    roles.push({ index, role, description: 'Material de la muestra anterior; revisar contenido.' });
  }
  raw.images = roles;
  const seeded = { ...lead, sampleAgent: { ...lead.sampleAgent, images, brief: normalizeConversationBrief(raw, images) } };
  const extractPrevious = (params) => extract({ ...params, message: { ...params.message, previousSample: previous } });
  let preview;
  if (messages.some((message) => message.sender === 'lead')) {
    preview = await buildReactivationPreview({ lead: seeded, messages, transcribe, extract: extractPrevious });
  } else {
    if (!previous.companyInfo && !raw.companyName) throw fail('No hay conversación ni datos de negocio para recuperar.', 400);
    const decision = await extractPrevious({ brief: seeded.sampleAgent.brief, images, history: [], audioOptions: [], message: { reactivation: true, text: '', mediaType: 'reactivation' } });
    if (decision.action === 'pause') throw fail('El historial indica que no debemos contactar a este cliente.');
    const brief = normalizeConversationBrief(decision.brief, images);
    preview = { brief, images, history: [], reply: '', failedAudios: 0, videoCount: 0 };
  }
  for (let index = 0; index < preview.images.length; index++) if (!preview.brief.images.some((image) => image.index === index)) preview.brief.images.push({ index, role: 'unclassified', description: 'Material recuperado pendiente de clasificar' });
  // Explicit advisor corrections take precedence over extraction from older messages.
  if (lead.sampleAgent?.manualContextUpdatedAt) {
    preview.brief = normalizeConversationBrief({ ...preview.brief, ...lead.sampleAgent.brief }, preview.images);
  }
  const missing = missingBriefQuestion(preview.brief);
  return { ...preview, status: missing ? 'collecting' : 'awaiting_confirmation', pending: missing || 'Confirmar la nueva muestra',
    summary: briefSummary(preview.brief), canGenerate: Boolean(preview.brief.companyName && preview.brief.activity), previous };
}

export function registerSampleRecoveryRoutes(app, { db, readUser, readBusiness, extract, transcribe, sendAudio, send, generate, cancelSequences = async () => {} }) {
  const handlers = {};
  const contact = async (id, user) => {
    if (!id || id.includes('/') || id.length > 200) throw fail('Selecciona un contacto válido.', 400);
    const ref = db.collection('leads').doc(id), snap = await ref.get();
    if (!snap.exists) throw fail('Contacto no encontrado.', 404);
    const lead = snap.data(); assertSampleRecoveryAllowed(user, lead);
    return { ref, lead };
  };
  const route = (name, handler) => {
    handlers[name] = handler;
    app.post(`/api/crm/sample-agent/recovery/${name}`, async (req, res) => {
    try {
      const user = await readUser(req);
      if (!user) throw fail('Inicia sesión para recuperar contactos.', 401);
      if (!canGenerateAiSample(user, { assignedTo: user.uid })) throw fail('No tienes acceso a los contactos.', 403);
      return res.json(await handler(req.body || {}, user));
    } catch (error) { return res.status(error.status || 500).json({ error: error.message || 'No se pudo recuperar el contacto.' }); }
    });
  };
  route('candidates', async (body, user) => {
    const from = new Date(body.from), to = new Date(body.to);
    if (!Number.isFinite(+from) || !Number.isFinite(+to) || to < from || to - from > 3660 * 86400_000) throw fail('Selecciona un rango válido de hasta diez años.', 400);
    let query = db.collection('leads').where('lastMessageAt', '>=', from).where('lastMessageAt', '<=', to).orderBy('lastMessageAt', 'desc');
    if (body.cursor) {
      if (typeof body.cursor !== 'string' || body.cursor.includes('/') || body.cursor.length > 200) throw fail('Página no válida.', 400);
      const cursor = await db.collection('leads').doc(body.cursor).get();
      if (!cursor.exists) throw fail('Actualiza la búsqueda.', 400);
      query = query.startAfter(cursor);
    }
    const snap = await query.limit(100).get();
    const leads = snap.docs.flatMap((doc) => {
      const lead = doc.data();
      try { assertSampleRecoveryAllowed(user, lead); } catch { return []; }
      const delivered = hasDeliveredSample(lead);
      if (body.kind === 'delivered' && !delivered || body.kind === 'pending' && delivered) return [];
      return [{ id: doc.id, name: lead.nombre || lead.telefono || doc.id, phone: lead.telefono || '', business: lead.sampleAgent?.brief?.companyName || lead.briefWeb?.companyName || '', delivered, lastMessageAt: millis(lead.lastMessageAt), salesQualification: recoverySalesQualification(lead) }];
    });
    return { leads, nextCursor: snap.docs.length === 100 ? snap.docs.at(-1).id : '' };
  });
  route('preview', async (body, user) => {
    const { ref, lead } = await contact(clean(body.leadId, 200), user);
    const mode = body.mode === 'renew' ? 'renew' : 'audio';
    const audio = mode === 'audio' ? (await db.collection('sampleAgentAudios').doc('reactivation').get()).data() : null;
    if (mode === 'audio' && (!audio?.enabled || !audio.script)) throw fail('Graba y activa primero el audio de recuperación.', 400);
    const messages = await readReactivationMessages(ref);
    const business = await readBusiness(ref.id, lead);
    const preview = await buildSampleRecoveryPreview({ lead, business, messages, transcribe, extract });
    if (mode === 'renew' && !preview.canGenerate) throw fail('Faltan nombre y actividad. Reactiva la conversación para completarlos.', 400);
    const draft = { ...preview, mode, reviewBeforeSend: mode === 'renew' && body.reviewBeforeSend !== false, promotion: clean(body.promotion, 1200), designDirection: clean(body.designDirection, 2000) || DEFAULT_RENEWAL_DIRECTION, deliveryMessage: clean(body.deliveryMessage, 2800),
      audioScript: audio?.script || '', audioVersion: audio?.storagePath || '', regenerationId: randomUUID(),
      source: reactivationSource(lead, messages), businessSource: hash(previousSampleFacts(lead, business)),
      createdBy: user.uid, createdAt: new Date(), expiresAt: new Date(Date.now() + 30 * 60_000), state: 'prepared' };
    const draftRef = ref.collection('sampleRecoveryDrafts').doc(randomUUID());
    await draftRef.set(draft);
    return { draftId: draftRef.id, mode, summary: preview.summary, pending: preview.pending, failedAudios: preview.failedAudios,
      audioScript: draft.audioScript, promotion: draft.promotion, designDirection: draft.designDirection,
      imageCount: preview.images.length, videoCount: preview.videoCount, canGenerate: preview.canGenerate };
  });
  route('execute', async (body, user) => {
    const { ref, lead } = await contact(clean(body.leadId, 200), user);
    if (!/^[a-f0-9-]{36}$/.test(body.draftId || '')) throw fail('Prepara primero la vista previa.', 400);
    const draftRef = ref.collection('sampleRecoveryDrafts').doc(body.draftId);
    const messages = await readReactivationMessages(ref);
    const businessSource = hash(previousSampleFacts(lead, await readBusiness(ref.id, lead)));
    const pending = await ref.collection('sampleAgentInbox').where('state', '==', 'pending').get();
    const owner = randomUUID();
    const draft = await db.runTransaction(async (tx) => {
      const current = (await tx.get(ref)).data() || {};
      const saved = (await tx.get(draftRef)).data();
      if (!saved || saved.createdBy !== user.uid) throw fail('Vista previa no encontrada.', 404);
      if (saved.state === 'sent') return null;
      assertSampleRecoveryAllowed(user, current);
      if (saved.state !== 'prepared') throw fail('Esta operación ya se intentó. Revisa el chat antes de repetirla.');
      if (millis(saved.expiresAt) < Date.now() || saved.source !== reactivationSource(current, messages) || saved.businessSource !== businessSource) throw fail('El contacto cambió o venció la vista previa. Prepárala nuevamente.');
      tx.update(draftRef, { state: 'processing', attemptedAt: new Date() });
      for (const doc of pending.docs) if (millis(doc.data().receivedAt) <= millis(saved.createdAt)) tx.update(doc.ref, { state: 'done', recoveredBy: draftRef.id });
      tx.update(ref, { sampleAgent: { ...current.sampleAgent, enabled: false, status: saved.mode === 'renew' ? 'generating' : 'collecting',
        version: owner, brief: saved.brief, images: saved.images, history: saved.history, confirmationHash: '', deliveryPaused: false, error: '', pauseReason: '',
        reviewBeforeSend: saved.reviewBeforeSend === true, regenerationId: saved.regenerationId, regenerationRequestedAt: new Date(), designDirection: saved.designDirection, deliveryMessage: saved.deliveryMessage || '',
        recoveryOffer: [saved.audioScript, saved.promotion].filter(Boolean).join('\n'), leaseOwner: owner, leaseUntil: new Date(Date.now() + 5 * 60_000), updatedAt: new Date() } });
      if (saved.mode === 'renew') tx.update(ref, { sampleProposalCycle: { id: saved.regenerationId, requestedAt: new Date(), sentAt: null, openedAt: null } });
      return saved;
    });
    if (!draft) return { success: true, duplicate: true };
    let generation;
    const isCurrent = async () => {
      const latest = (await ref.get()).data() || {};
      return latest.sampleAgent?.version === owner && !latest.sampleAgent.deliveryPaused && agentCanReply({ ...latest, sampleAgent: { ...latest.sampleAgent, enabled: true } });
    };
    try {
      await cancelSequences(ref.id);
      if (!await isCurrent()) throw fail('El contacto cambió. Revisa el chat.');
      if (draft.mode === 'audio') {
        const audio = (await db.collection('sampleAgentAudios').doc('reactivation').get()).data();
        if (!audio?.enabled || audio.storagePath !== draft.audioVersion) throw fail('Cambió el audio. Prepara otra vista previa.');
        if (!await sendAudio({ leadId: ref.id, moment: 'reactivation', expectedStoragePath: draft.audioVersion, isCurrent })) throw fail('No se pudo enviar el audio de recuperación.', 503);
        await draftRef.update({ audioSent: true });
        if (draft.promotion) { if (!await isCurrent()) throw fail('El contacto cambió.'); await send(ref.id, draft.promotion); await draftRef.update({ promotionSent: true }); }
      } else {
        const current = (await ref.get()).data();
        generation = await generate({ leadRef: ref, leadId: ref.id, lead: current, input: { ...manualSampleInput(current), designDirection: draft.designDirection, regenerationId: draft.regenerationId } });
        await draftRef.update({ generation });
      }
      await db.runTransaction(async (tx) => {
        const latest = (await tx.get(ref)).data() || {};
        tx.update(draftRef, { state: 'sent', sentAt: new Date() });
        if (latest.sampleAgent?.version !== owner) return;
        const history = [...draft.history, ...(draft.mode === 'audio' ? [{ role: 'assistant', text: draft.audioScript }, ...(draft.promotion ? [{ role: 'assistant', text: draft.promotion }] : [])] : [])].slice(-24);
        tx.update(ref, { 'sampleAgent.enabled': draft.mode === 'audio', 'sampleAgent.history': history,
          'sampleAgent.status': generation ? generation.ready ? 'completed' : latest.sampleAgent.status === 'completed' ? 'completed' : 'generating' : draft.status,
          ...(generation ? { 'sampleAgent.negocioId': generation.negocioId } : {}),
          'sampleAgent.audioMomentsSent': [...new Set([...(latest.sampleAgent.audioMomentsSent || []), 'welcome'])],
          'sampleAgent.leaseOwner': '', 'sampleAgent.leaseUntil': new Date(0), 'sampleAgent.updatedAt': new Date() });
      });
      return { success: true, generated: Boolean(generation), row: summarizeSampleProgress(ref.id, (await ref.get()).data()) };
    } catch (error) {
      await db.runTransaction(async (tx) => {
        const latest = (await tx.get(ref)).data() || {};
        tx.update(draftRef, { state: 'failed', error: clean(error.message, 300) });
        if (latest.sampleAgent?.version === owner) tx.update(ref, { 'sampleAgent.enabled': false, 'sampleAgent.status': generation ? 'generating' : 'error', 'sampleAgent.error': clean(error.message, 300), 'sampleAgent.deliveryPaused': !generation, 'sampleAgent.leaseOwner': '', 'sampleAgent.leaseUntil': new Date(0) });
      });
      throw error;
    }
  });
  return handlers;
}
