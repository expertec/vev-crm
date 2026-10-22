import { createHash, randomUUID } from 'node:crypto';
import { agentCanReply, briefSummary, extractConversationBrief, missingBriefQuestion, normalizeConversationBrief, sampleBriefHash } from './sampleConversationAgent.js';
import { canGenerateAiSample } from './aiSampleAccess.js';
import { publicImageUrl } from './aiSampleGenerator.js';

const millis = (value) => value?.toMillis?.() || (value ? new Date(value).getTime() : 0) || 0;
const hash = (value) => createHash('sha256').update(JSON.stringify(value)).digest('hex');
const error = (message, status = 409) => Object.assign(new Error(message), { status });
export function assertReactivationAllowed(user, lead) {
  if (!canGenerateAiSample(user, lead)) throw error('No tienes acceso a este contacto.', 403);
  if (!agentCanReply({ ...lead, sampleAgent: { ...lead.sampleAgent, enabled: true } })) throw error('El contacto está bloqueado, canceló o está en atención humana.');
  if (lead.sampleLinkSentAt || lead.webLinkSentAt || lead.siteSentAt || lead.sampleAgent?.status === 'completed') throw error('Este contacto ya recibió su muestra.');
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
  if (!messages.some((m) => m.sender === 'lead')) throw error('No hay mensajes anteriores del cliente para retomar.', 400);
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
  const decision = await extract({ brief: lead.sampleAgent?.brief || {}, images, history, audioOptions: [], message: { reactivation: true, text: '', mediaType: 'reactivation' } });
  if (decision.action === 'pause') throw error('El historial indica que no debemos reactivar este contacto.');
  const brief = normalizeConversationBrief(decision.brief, images);
  for (let index = 0; index < images.length; index++) if (!brief.images.some((image) => image.index === index)) brief.images.push({ index, role: 'unclassified', description: '' });
  let missing = missingBriefQuestion(brief);
  if (brief.photosChoice === 'pending' && images.length && missing.startsWith('Envíame hasta tres fotos')) missing = `Ya tengo ${images.length} imagen${images.length === 1 ? '' : 'es'} que compartiste. ¿Quieres añadir alguna otra o son todas?`;
  const intro = `Hola, retomamos tu muestra${brief.companyName ? ` de ${brief.companyName}` : ' de tu página web'}.`;
  const reply = missing ? `${intro} Ya revisé lo que me compartiste. ${missing}` : `${intro}\n\n${briefSummary(brief)}`;
  return { brief, images, history: history.slice(-24), reply, status: missing ? 'collecting' : 'awaiting_confirmation', failedAudios, videoCount: Math.min(2, messages.filter((m) => m.sender === 'lead' && m.mediaType === 'video').length) };
}

export function registerSampleReactivationRoutes(app, { db, readUser, transcribe, send, cancelSequences = async () => {}, extract }) {
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
  route('/api/crm/sample-agent/reactivation/preview', async (req, res, user) => {
    const { ref, lead } = await contact(req.body.leadId, user);
    const messages = await readReactivationMessages(ref);
    if (messages.some((m) => m.sender === 'lead' && ['audio', 'audio_ptt', 'ptt'].includes(m.mediaType) && !m.transcription)) {
      const inbox = await ref.collection('sampleAgentInbox').orderBy('receivedAt', 'desc').limit(80).get();
      const transcripts = new Map(inbox.docs.map((doc) => [doc.data().mediaUrl, doc.data().transcription]));
      for (const message of messages) if (message.sender === 'lead' && !message.transcription && transcripts.get(message.mediaUrl)) message.transcription = transcripts.get(message.mediaUrl);
    }
    const preview = await buildReactivationPreview({ lead, messages, transcribe, extract });
    const draft = ref.collection('sampleAgentReactivations').doc(randomUUID());
    await draft.set({ ...preview, source: reactivationSource(lead, messages), state: 'prepared', createdBy: user.uid, createdAt: new Date(), expiresAt: new Date(Date.now() + 30 * 60_000) });
    res.json({ draftId: draft.id, ...preview, history: undefined });
  });
  route('/api/crm/sample-agent/reactivation/send', async (req, res, user) => {
    const { ref, lead } = await contact(req.body.leadId, user);
    if (!/^[a-f0-9-]{36}$/.test(req.body.draftId || '')) throw error('Prepara primero la vista previa.', 400);
    const draftRef = ref.collection('sampleAgentReactivations').doc(req.body.draftId);
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
    if (!preview) return res.json({ success: true, duplicate: true, sampleAgent: lead.sampleAgent });
    try {
      await cancelSequences(ref.id);
      const current = (await ref.get()).data() || {};
      if (!agentCanReply(current) || current.sampleAgent?.version !== owner
        || (current.sampleAgent?.inboxRevision || 0) !== (lead.sampleAgent?.inboxRevision || 0)
        || millis(current.lastMessageAt) !== millis(lead.lastMessageAt)) throw error('El contacto cambió durante la preparación del envío. Revisa el chat y prepara otra vista previa.');
      await send(ref.id, preview.reply);
      await db.runTransaction(async (tx) => {
        const current = (await tx.get(ref)).data() || {};
        tx.update(draftRef, { state: 'sent', sentAt: new Date() });
        if (current.sampleAgent?.version === owner) tx.update(ref, { 'sampleAgent.history': [...preview.history, { role: 'assistant', text: preview.reply }].slice(-24), 'sampleAgent.updatedAt': new Date(), 'sampleAgent.leaseOwner': '', 'sampleAgent.leaseUntil': new Date(0), 'sampleAgent.reactivationId': draftRef.id });
      });
      res.json({ success: true, sampleAgent: (await ref.get()).data()?.sampleAgent });
    } catch (err) {
      await db.runTransaction(async (tx) => {
        const current = (await tx.get(ref)).data() || {};
        tx.update(draftRef, { state: 'failed', error: String(err.message).slice(0, 300) });
        if (current.sampleAgent?.version === owner) tx.update(ref, { 'sampleAgent.enabled': false, 'sampleAgent.status': 'error', 'sampleAgent.error': 'No se pudo confirmar el envío de reactivación. Revisa el chat antes de reintentar.', 'sampleAgent.leaseOwner': '', 'sampleAgent.leaseUntil': new Date(0) });
      });
      throw err;
    }
  });
}
