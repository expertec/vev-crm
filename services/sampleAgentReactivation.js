import { createHash, randomUUID } from 'node:crypto';
import { agentCanReply, briefSummary, extractConversationBrief, missingBriefQuestion, normalizeConversationBrief, sampleBriefHash, sampleInputFromConversation } from './sampleConversationAgent.js';
import { canGenerateAiSample } from './aiSampleAccess.js';
import { publicImageUrl } from './aiSampleGenerator.js';
import { hasUnreadWebsiteCampaign } from '../utils/websiteCampaignContext.js';

const millis = (value) => value?.toMillis?.() || (value ? new Date(value).getTime() : 0) || 0;
const hash = (value) => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const error = (message, status = 409) => Object.assign(new Error(message), { status });
export function hasDeliveredSample(lead) {
  if (lead.sampleAgent?.regenerationId && lead.sampleAgent?.regenerationRequestedAt && lead.sampleAgent.status !== 'completed') return false;
  if (lead.sampleAgent?.status === 'completed' || lead.webSales?.sample?.sentAt || lead.siteSentAt) return true;
  // These legacy dates also mark form invitations and restored samples, not just delivered pages.
  if (!lead.sampleLinkSentAt && !lead.webLinkSentAt) return false;
  try {
    const url = new URL(lead.sampleLinkSentUrl || lead.sampleFlow?.sampleUrl || '');
    if (!['negociosweb.mx', 'app.negociosweb.mx'].includes(url.hostname)) return false;
    return /^\/m\/[A-Za-z0-9_-]{10,24}\/?$/.test(url.pathname)
      || (/^\/[a-z0-9][a-z0-9-]*\/?$/i.test(url.pathname) && !/^\/(muestra|brief|form|formulario|gracias)\/?$/i.test(url.pathname));
  } catch { return false; }
}
export function assertReactivationAllowed(user, lead) {
  if (!canGenerateAiSample(user, lead)) throw error('No tienes acceso a este contacto.', 403);
  if (!agentCanReply({ ...lead, sampleAgent: { ...lead.sampleAgent, enabled: true } })) throw error('El contacto está bloqueado, canceló o está en atención humana.');
  if (hasDeliveredSample(lead)) throw error('Este contacto ya recibió su muestra.');
  if (lead.sampleAgent?.status === 'generating' || millis(lead.sampleAgent?.leaseUntil) > Date.now()) throw error('El asistente está procesando este contacto. Espera a que termine.');
}

export function reactivationSource(lead, messages) {
  return hash({ version: lead.sampleAgent?.version || '', revision: lead.sampleAgent?.inboxRevision || 0, lastMessageAt: millis(lead.lastMessageAt), brief: lead.sampleAgent?.brief || {}, messages: messages.map((m) => [m.id, m.sender, m.content || '', m.mediaType || '', m.mediaUrl || '']) });
}

export async function readReactivationMessages(leadRef) {
  const snap = await leadRef.collection('messages').orderBy('timestamp', 'desc').limit(80).get();
  return snap.docs.map((doc) => ({ ...doc.data(), id: doc.id, ref: doc.ref })).reverse().filter((m) => ['lead', 'business'].includes(m.sender));
}

export async function buildReactivationPreview({ lead, messages, transcribe, extract = extractConversationBrief }) {
  if (!messages.some((m) => m.sender === 'lead')) {
    if (!hasUnreadWebsiteCampaign(lead)) throw error('No hay mensajes anteriores del cliente para retomar.', 400);
    return { brief: normalizeConversationBrief({}), images: [], history: [], reply: '¡Hola! Te ayudo a preparar una muestra de tu página web. ¿Cómo se llama tu negocio y a qué se dedica?', status: 'collecting', failedAudios: 0, videoCount: 0 };
  }
  const images = [...(lead.sampleAgent?.images || [])];
  const history = [];
  let audioCount = 0;
  let failedAudios = 0;
  for (const message of messages) {
    let text = String(message.content || '').slice(0, 2000);
    if (message.sender === 'lead' && message.mediaType === 'image' && publicImageUrl(message.mediaUrl) && images.length < 12 && !images.some((image) => image.url === message.mediaUrl)) images.push({ url: message.mediaUrl, caption: text.slice(0, 300) });
    if (message.sender === 'lead' && ['audio', 'audio_ptt', 'ptt'].includes(message.mediaType)) {
      text = message.transcription?.text || message.transcription || '';
      if (!text && transcribe && audioCount++ < 8) {
        try {
          text = await transcribe({ mediaUrl: message.mediaUrl });
          if (text && message.ref) await message.ref.update({ transcription: text });
        } catch { failedAudios++; }
      } else if (!text) failedAudios++;
      if (!text) text = '[Audio sin transcribir: no inferir su contenido]';
    }
    if (text || message.mediaType) history.push({ role: message.sender === 'lead' ? 'customer' : 'assistant', text: String(text).slice(0, 2000), mediaType: message.mediaType || 'text' });
  }
  const decision = await extract({ brief: lead.sampleAgent?.brief || {}, images, history, audioOptions: [], message: { reactivation: true, briefReviewedByAdvisor: Boolean(lead.sampleAgent?.manualContextUpdatedAt), text: '', mediaType: 'reactivation' } });
  if (decision.action === 'pause') throw error('El historial indica que no debemos reactivar este contacto.');
  const brief = normalizeConversationBrief(decision.brief, images);
  for (let index = 0; index < images.length; index++) if (!brief.images.some((image) => image.index === index)) brief.images.push({ index, role: 'unclassified', description: '' });
  let missing = missingBriefQuestion(brief);
  if (brief.photosChoice === 'pending' && images.length && missing.startsWith('Envíame hasta tres fotos')) missing = `Ya tengo ${images.length} imagen${images.length === 1 ? '' : 'es'} que compartiste. ¿Quieres añadir alguna otra o son todas?`;
  const intro = `Hola, retomamos tu muestra${brief.companyName ? ` de ${brief.companyName}` : ' de tu página web'}.`;
  const reply = missing ? `${intro} Ya revisé lo que me compartiste. ${missing}` : `${intro}\n\n${briefSummary(brief)}`;
  return { brief, images, history: history.slice(-24), reply, status: missing ? 'collecting' : 'awaiting_confirmation', failedAudios, videoCount: Math.min(2, messages.filter((m) => m.sender === 'lead' && m.mediaType === 'video').length) };
}

export function registerSampleReactivationRoutes(app, { db, readUser, transcribe, send, cancelSequences = async () => {}, extract, generate, loadAudioOptions = async () => [], sendFlowAudio }) {
  const route = (path, handler) => app.post(path, async (req, res) => {
    try {
      const user = await readUser(req);
      if (!user) throw error('Inicia sesión para retomar conversaciones.', 401);
      await handler(req, res, user);
    } catch (err) { res.status(err.status || 500).json({ error: err.message || 'No se pudo retomar la conversación.' }); }
  });
  const contact = async (leadId, user) => {
    if (typeof leadId !== 'string' || !leadId || leadId.includes('/')) throw error('Selecciona un contacto válido.', 400);
    const ref = db.collection('leads').doc(leadId);
    const snap = await ref.get();
    if (!snap.exists) throw error('Contacto no encontrado.', 404);
    const lead = snap.data(); assertReactivationAllowed(user, lead);
    return { ref, lead };
  };
  route('/api/crm/sample-agent/reactivation/candidates', async (req, res, user) => {
    const from = new Date(req.body.from), to = new Date(req.body.to);
    if (!Number.isFinite(+from) || !Number.isFinite(+to) || to < from || to - from > 7 * 86400_000) throw error('Elige un rango de hasta siete días.', 400);
    const snap = await db.collection('leads').where('lastMessageAt', '>=', from).orderBy('lastMessageAt', 'desc').limit(100).get();
    const eligible = snap.docs.filter((doc) => {
      try { assertReactivationAllowed(user, doc.data()); return true; } catch { return false; }
    }).slice(0, 50);
    const leads = [];
    // Bounded batches: check actual inbound dates, not the date of our last reply.
    for (let index = 0; index < eligible.length; index += 5) {
      const batch = await Promise.all(eligible.slice(index, index + 5).map(async (doc) => {
        const messages = await readReactivationMessages(doc.ref);
        const inbound = messages.filter((m) => m.sender === 'lead' && millis(m.timestamp) >= +from && millis(m.timestamp) <= +to).at(-1);
        const lead = doc.data();
        return inbound ? { id: doc.id, name: lead.nombre || lead.telefono || doc.id, phone: lead.telefono || '', lastMessageAt: millis(inbound.timestamp) } : null;
      }));
      leads.push(...batch.filter(Boolean));
    }
    res.json({ leads });
  });
  const prepare = async (leadId, user) => {
    const { ref, lead } = await contact(leadId, user);
    const messages = await readReactivationMessages(ref);
    if (messages.some((m) => m.sender === 'lead' && ['audio', 'audio_ptt', 'ptt'].includes(m.mediaType) && !m.transcription)) {
      const inbox = await ref.collection('sampleAgentInbox').orderBy('receivedAt', 'desc').limit(80).get();
      const transcripts = new Map(inbox.docs.map((doc) => [doc.data().mediaUrl, doc.data().transcription]));
      for (const message of messages) if (message.sender === 'lead' && !message.transcription && transcripts.get(message.mediaUrl)) message.transcription = transcripts.get(message.mediaUrl);
    }
    const preview = await buildReactivationPreview({ lead, messages, transcribe, extract });
    const draft = ref.collection('sampleAgentReactivations').doc(randomUUID());
    await draft.set({ ...preview, source: reactivationSource(lead, messages), state: 'prepared', createdBy: user.uid, createdAt: new Date(), expiresAt: new Date(Date.now() + 30 * 60_000) });
    return { draftId: draft.id, ...preview, history: undefined };
  };
  const deliver = async (leadId, draftId, user, generateWhenReady = false) => {
    const { ref, lead } = await contact(leadId, user);
    if (!/^[a-f0-9-]{36}$/.test(draftId || '')) throw error('Prepara primero la vista previa.', 400);
    const draftRef = ref.collection('sampleAgentReactivations').doc(draftId);
    const messages = await readReactivationMessages(ref);
    const pending = await ref.collection('sampleAgentInbox').where('state', '==', 'pending').get();
    const source = reactivationSource(lead, messages);
    const owner = randomUUID();
    const preview = await db.runTransaction(async (tx) => {
      const current = (await tx.get(ref)).data() || {};
      const draft = (await tx.get(draftRef)).data();
      if (!draft || draft.createdBy !== user.uid) throw error('Vista previa no encontrada.', 404);
      if (draft.state === 'sent') return null;
      assertReactivationAllowed(user, current);
      if (draft.state !== 'prepared') throw error('Este envío ya se intentó. Revisa el chat antes de volver a contactar.');
      if (millis(draft.expiresAt) < Date.now() || draft.source !== source || reactivationSource(current, messages) !== source) throw error('El chat cambió o la vista previa venció. Prepárala de nuevo.');
      tx.update(draftRef, { state: 'sending', attemptedAt: new Date() });
      for (const doc of pending.docs) if (millis(doc.data().receivedAt) <= millis(draft.createdAt)) tx.update(doc.ref, { state: 'done', recoveredBy: draftRef.id });
      tx.update(ref, { sampleAgent: { ...current.sampleAgent, enabled: true, version: owner, status: draft.status, brief: draft.brief, images: draft.images, history: draft.history, confirmationHash: draft.status === 'awaiting_confirmation' ? sampleBriefHash(draft.brief) : '', deliveryPaused: false, error: '', leaseOwner: owner, leaseUntil: new Date(Date.now() + 5 * 60_000) } });
      return draft;
    });
    if (!preview) return { success: true, duplicate: true, sampleAgent: lead.sampleAgent };
    let generation = null;
    let reply = preview.reply;
    let welcomeAudio = false;
    try {
      await cancelSequences(ref.id);
      const current = (await ref.get()).data() || {};
      if (!agentCanReply(current) || current.sampleAgent?.version !== owner
        || (current.sampleAgent?.inboxRevision || 0) !== (lead.sampleAgent?.inboxRevision || 0)
        || millis(current.lastMessageAt) !== millis(lead.lastMessageAt)) throw error('El contacto cambió durante la preparación del envío. Revisa el chat y prepara otra vista previa.');
      if (generateWhenReady && preview.status === 'awaiting_confirmation' && !preview.failedAudios) {
        if (!generate) throw error('No se configuró la generación de muestras.', 503);
        generation = await generate({ leadRef: ref, leadId: ref.id, lead: current, input: sampleInputFromConversation(preview.brief, preview.images, current.telefono || ref.id.replace(/\D/g, '')) });
        await draftRef.update({ generation });
        reply = 'Ya revisé lo que me compartiste y tengo la información necesaria. Voy a preparar tu muestra y te enviaré el enlace por aquí cuando esté lista.';
      }
      if (preview.sendMode === 'welcome') {
        welcomeAudio = Boolean(await sendFlowAudio?.({ leadId: ref.id, moment: 'welcome', isCurrent: async () => {
          const latest = (await ref.get()).data() || {};
          return latest.sampleAgent?.version === owner && agentCanReply(latest);
        } }));
        if (!welcomeAudio) throw error('No se pudo enviar el audio de bienvenida. Revisa que esté activo y que WhatsApp esté conectado.', 503);
      } else if (generation?.ready) {
        await send(ref.id, '¡Tu muestra ya está lista! Te comparto el enlace en el siguiente mensaje. 👇');
        await send(ref.id, generation.sampleUrl);
      } else await send(ref.id, reply);
      await db.runTransaction(async (tx) => {
        const current = (await tx.get(ref)).data() || {};
        tx.update(draftRef, { state: 'sent', sentAt: new Date() });
        if (current.sampleAgent?.version === owner) tx.update(ref, { 'sampleAgent.history': [...preview.history, { role: 'assistant', text: reply }].slice(-24), ...(generation ? { 'sampleAgent.status': generation.ready ? 'completed' : 'generating', 'sampleAgent.enabled': !generation.ready, 'sampleAgent.confirmationHash': '', 'sampleAgent.negocioId': generation.negocioId } : {}), ...(welcomeAudio ? { 'sampleAgent.audioMomentsSent': [...new Set([...(current.sampleAgent.audioMomentsSent || []), 'welcome'])] } : {}), 'sampleAgent.updatedAt': new Date(), 'sampleAgent.leaseOwner': '', 'sampleAgent.leaseUntil': new Date(0), 'sampleAgent.reactivationId': draftRef.id });
      });
      return { success: true, welcomeAudio, generated: Boolean(generation), sampleAgent: (await ref.get()).data()?.sampleAgent };
    } catch (err) {
      await db.runTransaction(async (tx) => {
        const current = (await tx.get(ref)).data() || {};
        tx.update(draftRef, { state: 'failed', error: String(err.message).slice(0, 300) });
        if (current.sampleAgent?.version === owner) tx.update(ref, { 'sampleAgent.enabled': Boolean(generation && !generation.ready), 'sampleAgent.status': generation ? (generation.ready ? 'completed' : 'generating') : 'error', 'sampleAgent.error': generation ? 'La muestra fue solicitada, pero falló el aviso de inicio. Revisa el chat.' : 'No se pudo confirmar el envío de reactivación. Revisa el chat antes de reintentar.', 'sampleAgent.leaseOwner': '', 'sampleAgent.leaseUntil': new Date(0) });
      });
      throw err;
    }
  };
  route('/api/crm/sample-agent/reactivation/preview', async (req, res, user) => res.json(await prepare(req.body.leadId, user)));
  route('/api/crm/sample-agent/reactivation/send', async (req, res, user) => res.json(await deliver(req.body.leadId, req.body.draftId, user)));
  const activating = new Map();
  return {
    startWelcome(leadId, user) {
      const key = `${user.uid}:${leadId}`;
      if (activating.has(key)) return activating.get(key);
      const work = (async () => {
        const { ref, lead } = await contact(leadId, user);
        if (lead.sampleAgent?.audioMomentsSent?.includes('welcome')) return { success: true, duplicate: true, welcomeAudio: true, sampleAgent: lead.sampleAgent };
        const welcome = (await loadAudioOptions()).find((audio) => audio.moment === 'welcome');
        if (!welcome || !sendFlowAudio) throw error('Graba y activa el audio en Audios del asistente → Bienvenida.', 400);
        const messages = await readReactivationMessages(ref);
        const images = lead.sampleAgent?.images || [];
        const history = lead.sampleAgent?.history?.length ? lead.sampleAgent.history : messages.map((m) => ({ role: m.sender === 'lead' ? 'customer' : 'assistant', text: String(m.transcription?.text || m.transcription || m.content || '').slice(0, 2000), mediaType: m.mediaType || 'text' })).slice(-24);
        const draft = ref.collection('sampleAgentReactivations').doc(randomUUID());
        await draft.set({ brief: normalizeConversationBrief(lead.sampleAgent?.brief || {}, images), images, history, reply: welcome.script, status: 'collecting', sendMode: 'welcome', source: reactivationSource(lead, messages), state: 'prepared', createdBy: user.uid, createdAt: new Date(), expiresAt: new Date(Date.now() + 5 * 60_000) });
        return deliver(leadId, draft.id, user);
      })().finally(() => activating.delete(key));
      activating.set(key, work);
      return work;
    },
    activate(leadId, user, { generateWhenReady = true } = {}) {
      const key = `${user.uid}:${leadId}`;
      if (activating.has(key)) return activating.get(key);
      const work = (async () => {
        const preview = await prepare(leadId, user);
        return deliver(leadId, preview.draftId, user, generateWhenReady);
      })().finally(() => activating.delete(key));
      activating.set(key, work);
      return work;
    },
  };
}
