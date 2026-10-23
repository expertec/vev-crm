import { canGenerateAiSample } from './aiSampleAccess.js';
import { randomUUID } from 'node:crypto';
import { agentCanReply, missingBriefQuestion, normalizeConversationBrief, sampleInputFromConversation } from './sampleConversationAgent.js';
import { hasDeliveredSample } from './sampleAgentReactivation.js';

const statuses = ['collecting', 'awaiting_confirmation', 'generating', 'paused', 'error'];
const millis = (value) => value?.toMillis?.() || (value ? new Date(value).getTime() : 0) || 0;

export function manualSampleInput(lead) {
  const images = lead.sampleAgent?.images || [];
  const brief = normalizeConversationBrief(lead.sampleAgent?.brief || {}, images);
  if (!brief.companyName || !brief.activity) throw Object.assign(new Error('Falta el nombre o la actividad del negocio. Completa esos datos en el chat antes de generar.'), { status: 400 });
  brief.images = brief.images.map((image) => image.role === 'logo' && ['crop', 'original_required'].includes(image.logoStatus) ? { ...image, role: 'reference' } : image);
  if (brief.logoChoice !== 'provided' || !brief.images.some((image) => image.role === 'logo')) {
    brief.logoChoice = 'none';
    brief.images = brief.images.map((image) => image.role === 'logo' ? { ...image, role: 'reference' } : image);
  }
  if (!brief.colorPreference && brief.palette.length) brief.colorPreference = `Colores detectados en el material de la marca: ${brief.palette.join(', ')}`;
  if (!brief.colorPreference && brief.logoChoice === 'none') brief.colorPreference = 'Proponer una combinación de colores acorde al negocio para la muestra';
  return sampleInputFromConversation(brief, images, lead.telefono || '');
}

export function summarizeSampleProgress(id, lead, now = Date.now()) {
  const agent = lead.sampleAgent;
  if (!agent || !statuses.includes(agent.status) || hasDeliveredSample(lead)
    || lead.archived || lead.archivedAt || lead.isArchived || lead.stopSequences
    || ['compro', 'cliente', 'no interesado'].includes(String(lead.estado || '').toLowerCase())) return null;
  const brief = normalizeConversationBrief(agent.brief || {}, agent.images || []);
  const logo = brief.images.filter((image) => image.role === 'logo');
  const usableLogo = brief.logoChoice === 'provided' && logo.some((image) => !['crop', 'original_required'].includes(image.logoStatus));
  const steps = [
    ['Nombre', Boolean(brief.companyName)], ['Actividad', Boolean(brief.activity)],
    ['Objetivo', Boolean(brief.objective)], ['Productos o servicios', Boolean(brief.offerings.length)],
    ['Clientes', Boolean(brief.customers)], ['Diferenciadores', Boolean(brief.differentiators)],
    ['Zona de servicio', Boolean(brief.serviceArea)], ['Logo o nombre', usableLogo || brief.logoChoice === 'none'],
    ['Colores', usableLogo ? brief.palette.length >= 3 : brief.logoChoice === 'none' && Boolean(brief.colorPreference) && !/sin definir|pendiente/i.test(brief.colorPreference)],
    ['Fotos', brief.photosChoice !== 'pending' && !brief.images.some((image) => image.role === 'unclassified' || !image.description)
      && (brief.photosChoice === 'none' || brief.images.some((image) => ['product', 'work', 'reference'].includes(image.role)))],
  ].map(([label, done]) => ({ label, done }));
  const missing = missingBriefQuestion(brief);
  const complete = !missing;
  const progress = agent.status === 'generating' ? 95 : complete ? 90 : Math.min(85, Math.round(steps.filter((step) => step.done).length / steps.length * 85));
  const updatedAt = Math.max(millis(agent.updatedAt), millis(lead.lastMessageAt));
  const inactive = Boolean(updatedAt && now - updatedAt >= 6 * 3600_000);
  const essentials = steps.slice(0, 4).filter((step) => !step.done).map((step) => step.label.toLowerCase());
  const recommended = [steps[6], ...steps.slice(7)].filter((step) => !step.done).map((step) => step.label.toLowerCase());
  const readiness = essentials.length
    ? { level: 'red', label: 'Falta contexto esencial', reason: `Conviene retomar antes de generar. Falta: ${essentials.join(', ')}.` }
    : recommended.length
      ? { level: 'yellow', label: 'Conviene completar datos', reason: `Puedes generar una primera propuesta, pero mejoraría al definir: ${recommended.join(', ')}.` }
      : { level: 'green', label: 'Información suficiente', reason: 'Ya hay contexto del negocio y una decisión sobre colores e imágenes. Puedes generar; revisa los detalles pendientes si quieres afinarla.' };
  const contactAvailable = agentCanReply({ ...lead, sampleAgent: { ...agent, enabled: true } });
  return { id, name: lead.nombre || lead.telefono || id, business: brief.companyName, phone: lead.telefono || '',
    status: agent.status, progress, steps, updatedAt, inactive, readiness,
    needsAttention: inactive || ['paused', 'error'].includes(agent.status),
    pending: agent.status === 'generating' ? 'Generando la muestra' : missing || 'Confirmar el resumen para generar la muestra',
    pauseReason: agent.pauseReason === 'other_service_or_human_conversation' ? 'Conversación cedida al asesor por otro servicio o negociación' : agent.status === 'paused' ? 'Asistente pausado' : '',
    error: String(agent.error || '').slice(0, 300),
    lastReply: String((agent.history || []).filter((item) => item.role === 'assistant').at(-1)?.text || '').slice(0, 500),
    canForce: Boolean(brief.companyName && brief.activity && agent.status !== 'generating'
      && contactAvailable),
    canResume: Boolean(agent.status !== 'generating' && contactAvailable && millis(agent.leaseUntil) <= now),
  };
}

export function registerSampleProgressRoutes(app, { db, readUser, generate, resume, cancelSequences = async () => {} }) {
  app.get('/api/crm/sample-agent/progress', async (req, res) => {
    try {
      const user = await readUser(req);
      if (!user) return res.status(401).json({ error: 'Inicia sesión para consultar las muestras.' });
      if (!canGenerateAiSample(user, { assignedTo: user.uid })) return res.status(403).json({ error: 'No tienes acceso a las muestras.' });
      const cursor = String(req.query?.cursor || '');
      if (cursor.length > 200 || cursor.includes('/')) return res.status(400).json({ error: 'Página no válida.' });
      let query = db.collection('leads').where('sampleAgent.status', 'in', statuses).orderBy('__name__');
      if (cursor) query = query.startAfter(cursor);
      const snapshot = await query.limit(100).get();
      const rows = snapshot.docs.flatMap((doc) => {
        const lead = doc.data();
        if (!canGenerateAiSample(user, lead)) return [];
        const row = summarizeSampleProgress(doc.id, lead);
        return row ? [row] : [];
      });
      res.set?.('Cache-Control', 'no-store');
      return res.json({ rows, nextCursor: snapshot.docs.length === 100 ? snapshot.docs.at(-1).id : '' });
    } catch (error) {
      console.warn('[sample-progress]', error.message);
      return res.status(500).json({ error: 'No se pudieron cargar las muestras en proceso.' });
    }
  });
  if (resume) app.post('/api/crm/sample-agent/progress/resume', async (req, res) => {
    try {
      const user = await readUser(req);
      if (!user) return res.status(401).json({ error: 'Inicia sesión para retomar la conversación.' });
      const leadId = String(req.body?.leadId || '');
      if (!leadId || leadId.includes('/') || leadId.length > 200) return res.status(400).json({ error: 'Selecciona un contacto válido.' });
      const snapshot = await db.collection('leads').doc(leadId).get();
      if (!snapshot.exists) return res.status(404).json({ error: 'Contacto no encontrado.' });
      const lead = snapshot.data();
      if (!canGenerateAiSample(user, lead)) return res.status(403).json({ error: 'No tienes acceso a este contacto.' });
      if (!summarizeSampleProgress(leadId, lead)?.canResume) return res.status(409).json({ error: 'Este contacto no se puede retomar ahora. Revisa su estado en el chat.' });
      await resume(leadId, user);
      return res.json({ success: true, row: summarizeSampleProgress(leadId, (await snapshot.ref.get()).data()) });
    } catch (error) {
      return res.status(error.status || 500).json({ error: error.message || 'No se pudo retomar la conversación.' });
    }
  });
  if (generate) app.post('/api/crm/sample-agent/progress/generate', async (req, res) => {
    let ref, owner, generation;
    try {
      const user = await readUser(req);
      if (!user) return res.status(401).json({ error: 'Inicia sesión para generar la muestra.' });
      const leadId = String(req.body?.leadId || '');
      if (!leadId || leadId.includes('/') || leadId.length > 200) return res.status(400).json({ error: 'Selecciona un contacto válido.' });
      ref = db.collection('leads').doc(leadId);
      const version = randomUUID();
      const lead = await db.runTransaction(async (tx) => {
        const snapshot = await tx.get(ref);
        if (!snapshot.exists) throw Object.assign(new Error('Contacto no encontrado.'), { status: 404 });
        const current = snapshot.data();
        if (!canGenerateAiSample(user, current)) throw Object.assign(new Error('No tienes acceso a este contacto.'), { status: 403 });
        if (!summarizeSampleProgress(leadId, current) || !agentCanReply({ ...current, sampleAgent: { ...current.sampleAgent, enabled: true } })) throw Object.assign(new Error('Este contacto no admite generación. Revisa si ya recibió la muestra, canceló o está en atención humana.'), { status: 409 });
        if (current.sampleAgent.status === 'generating' || millis(current.sampleAgent.leaseUntil) > Date.now()) throw Object.assign(new Error('La muestra o el asistente ya están procesando este contacto. Espera a que termine.'), { status: 409 });
        manualSampleInput(current);
        tx.update(ref, { 'sampleAgent.version': version, 'sampleAgent.enabled': false, 'sampleAgent.status': 'generating', 'sampleAgent.deliveryPaused': false, 'sampleAgent.pauseReason': '', 'sampleAgent.error': '', 'sampleAgent.confirmationHash': '', 'sampleAgent.updatedAt': new Date(), 'sampleAgent.manualRequestedBy': user.uid, 'sampleAgent.manualRequestedAt': new Date() });
        return current;
      });
      owner = version;
      await cancelSequences(leadId);
      const current = (await ref.get()).data() || {};
      if (current.sampleAgent?.version !== owner || current.sampleAgent?.deliveryPaused) throw Object.assign(new Error('El contacto cambió durante la solicitud. Revisa su estado.'), { status: 409 });
      generation = await generate({ leadRef: ref, leadId, lead: current, input: manualSampleInput(lead) });
      await db.runTransaction(async (tx) => {
        const current = (await tx.get(ref)).data() || {};
        if (current.sampleAgent?.version === owner && current.sampleAgent.status === 'generating') tx.update(ref, { 'sampleAgent.negocioId': generation.negocioId, 'sampleAgent.status': generation.ready ? 'completed' : 'generating', 'sampleAgent.updatedAt': new Date() });
      });
      return res.json({ success: true, row: summarizeSampleProgress(leadId, (await ref.get()).data()) });
    } catch (error) {
      if (owner && !generation) await db.runTransaction(async (tx) => {
        const current = (await tx.get(ref)).data() || {};
        if (current.sampleAgent?.version === owner) tx.update(ref, { 'sampleAgent.status': 'error', 'sampleAgent.error': String(error.message).slice(0, 300), 'sampleAgent.enabled': false, 'sampleAgent.deliveryPaused': true, 'sampleAgent.updatedAt': new Date() });
      }).catch(() => {});
      return res.status(error.status || 500).json({ error: error.message || 'No se pudo solicitar la muestra.' });
    }
  });
}
