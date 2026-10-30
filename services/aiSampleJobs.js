import { buildAiSamplePublicUrl } from './sampleUrls.js';
import { randomUUID } from 'node:crypto';
import { AI_SAMPLE_VERSION, aiSampleInputHash, generateAiSample, normalizeAiSampleInput, publicImageUrl } from './aiSampleGenerator.js';
import { syncAiSampleFailure } from './aiSampleFailure.js';

const LEASE_MS = 15 * 60_000;
const runningJobs = new Set();

export async function scheduleAiSampleFollowup({ leadRef, schedule, logger = console }) {
  const lead = (await leadRef.get()).data() || {};
  const opened = lead.sampleProposalCycle?.id ? lead.sampleProposalCycle.openedAt : lead.linkOpenedAt;
  if (opened || lead.sampleAgent?.enabled || lead.stopSequences || lead.humanControl
    || lead.salesBrainHumanControl || lead.archived || lead.archivedAt
    || ['compro', 'cliente', 'no interesado'].includes(String(lead.estado || '').trim().toLowerCase())) return;
  try {
    await schedule(leadRef.id, 'Web_SampleNotOpened', new Date(), { source: 'ai-sample-ready', debug: true });
  } catch (error) {
    // A follow-up failure must not resend the sample or pay for another generation.
    logger.warn('[ai-sample] seguimiento pendiente:', error?.message || error);
  }
}

export function createSampleImageStore(bucket, negocioId) {
  const fileFor = (key) => bucket.file(`web-assets/ai-samples/${negocioId}/${key}.webp`);
  const urlFor = async (file) => (await file.getSignedUrl({ action: 'read', expires: '2100-01-01' }))[0];
  return {
    async get(key) {
      const file = fileFor(key);
      return (await file.exists())[0] ? urlFor(file) : '';
    },
    async put(key, buffer) {
      const file = fileFor(key);
      await file.save(buffer, { contentType: 'image/webp', resumable: false, metadata: { cacheControl: 'public,max-age=31536000' } });
      return urlFor(file);
    },
  };
}

export async function queueAiSampleJob(db, negocioRef, data) {
  // Read saved inbound chat videos for both conversational and manual generation.
  let videoURLs = [];
  if (data.leadId) {
    const messages = await db.collection('leads').doc(data.leadId).collection('messages')
      .where('mediaType', '==', 'video').where('sender', '==', 'lead').limit(2).get();
    videoURLs = messages.docs.map((doc) => publicImageUrl(doc.data().mediaUrl)).filter(Boolean);
  }
  const input = normalizeAiSampleInput({ ...data, videoURLs: [...videoURLs, ...(data.videoURLs || [])] });
  const inputHash = aiSampleInputHash(input);
  return db.runTransaction(async (tx) => {
    const current = (await tx.get(negocioRef)).data() || {};
    const job = current.aiSampleJob || {};
    if (['queued', 'running', 'sending'].includes(job.state)) return { queued: true, duplicate: true };
    if ((job.state === 'sent' || current.aiSampleLastSentHash === inputHash) && current.schema?.aiPage?.inputHash === inputHash) {
      tx.update(negocioRef, { status: 'Web enviada', 'aiSampleJob.state': 'sent' });
      return { queued: false, duplicate: true, ready: true };
    }
    tx.set(negocioRef, {
      ...data,
      aiSampleJob: { id: randomUUID(), state: 'queued', input, inputHash, reviewBeforeSend: data.reviewBeforeSend === true, requestedAt: new Date(), error: '' },
      status: 'Muestra IA pendiente', updatedAt: new Date(),
    }, { merge: true });
    return { queued: true, duplicate: false };
  });
}

export async function runAiSampleJob({ db, negocioRef, bucket, sendReady, generate = generateAiSample, logger = console }) {
  if (runningJobs.has(negocioRef.id)) return;
  runningJobs.add(negocioRef.id);
  let claimed;
  try {
    claimed = await db.runTransaction(async (tx) => {
      const data = (await tx.get(negocioRef)).data() || {};
      if (data.aiSampleJob?.state !== 'queued') return null;
      tx.update(negocioRef, { 'aiSampleJob.state': 'running', 'aiSampleJob.startedAt': new Date(), 'aiSampleJob.leaseUntil': new Date(Date.now() + LEASE_MS), status: 'Generando muestra IA' });
      return data;
    });
    if (!claimed) return;
    const { input, inputHash, id: jobId } = claimed.aiSampleJob;
    const imageStore = createSampleImageStore(bucket, negocioRef.id);
    const reused = claimed.schema?.aiPage?.inputHash === inputHash;
    const schema = reused
      ? claimed.schema
      : await generate(input, {
        imageStore,
        usageContext: { leadId: claimed.leadId || '', negocioId: negocioRef.id },
        savedDesign: claimed.aiSampleDraft?.inputHash === inputHash ? claimed.aiSampleDraft.design : null,
        saveDesign: (design) => db.runTransaction(async (tx) => {
          const latest = (await tx.get(negocioRef)).data() || {};
          if (latest.aiSampleJob?.id === jobId && ['running', 'paused'].includes(latest.aiSampleJob?.state)) tx.update(negocioRef, { aiSampleDraft: { inputHash, design } });
        }),
      });
    const delivery = await db.runTransaction(async (tx) => {
      const latest = (await tx.get(negocioRef)).data() || {};
      const job = latest.aiSampleJob || {};
      if (job.id !== jobId || !['running', 'paused'].includes(job.state)) return false;
      const state = job.state === 'paused' ? 'paused' : job.reviewBeforeSend ? 'ready' : 'running';
      tx.update(negocioRef, {
        schema, designVersion: AI_SAMPLE_VERSION,
        status: state === 'paused' ? 'Muestra pausada' : state === 'ready' ? 'Muestra pendiente de revisión' : 'Muestra IA lista',
        processedAt: new Date(), lastGeneratedAt: new Date(), updatedAt: new Date(),
        'aiSampleJob.state': state, 'aiSampleJob.generatedAt': new Date(), aiSampleDraft: null,
      });
      return state === 'running';
    });
    if (!delivery) {
      if (claimed.leadId) await db.runTransaction(async (tx) => {
        const leadRef = db.collection('leads').doc(claimed.leadId);
        const lead = (await tx.get(leadRef)).data() || {};
        const job = ((await tx.get(negocioRef)).data() || {}).aiSampleJob || {};
        if (job.id === jobId && job.state === 'ready' && lead.sampleAgent?.negocioId === negocioRef.id && lead.sampleAgent?.status === 'generating') {
          tx.update(leadRef, { 'sampleAgent.status': 'review', 'sampleAgent.previewUrl': buildAiSamplePublicUrl(claimed.slug), 'sampleAgent.enabled': false, 'sampleAgent.updatedAt': new Date() });
        }
      });
      return;
    }
    const sending = await db.runTransaction(async (tx) => {
      const latest = (await tx.get(negocioRef)).data() || {};
      if (latest.aiSampleJob?.id !== jobId || latest.aiSampleJob?.state !== 'running') return null;
      tx.update(negocioRef, { 'aiSampleJob.state': 'sending' });
      return latest;
    });
    if (!sending) return;
    await sendReady({ id: negocioRef.id, ...sending });
    await db.runTransaction(async (tx) => {
      const current = (await tx.get(negocioRef)).data() || {};
      if (current.aiSampleJob?.id === jobId && current.aiSampleJob?.state === 'sending') tx.update(negocioRef, { 'aiSampleJob.state': 'sent', 'aiSampleJob.finishedAt': new Date(), 'aiSampleJob.error': '' });
    });
  } catch (error) {
    logger.warn('[ai-sample] generación/entrega fallida:', error?.message || error);
    const failed = claimed && await db.runTransaction(async (tx) => {
      const current = (await tx.get(negocioRef)).data() || {};
      if (current.aiSampleJob?.id !== claimed.aiSampleJob.id) return false;
      if (current.aiSampleJob?.state === 'paused') {
        tx.update(negocioRef, { 'aiSampleJob.leaseUntil': new Date(0), 'aiSampleJob.error': String(error?.message || error).slice(0, 500) });
        return false;
      }
      if (!['running', 'sending'].includes(current.aiSampleJob?.state)) return false;
      tx.update(negocioRef, {
        'aiSampleJob.state': 'failed', 'aiSampleJob.error': String(error?.message || error).slice(0, 500),
        'aiSampleJob.finishedAt': new Date(), status: 'Error muestra IA',
      });
      return true;
    }).catch(() => false);
    if (failed && claimed?.leadId) await syncAiSampleFailure(db, db.collection('leads').doc(claimed.leadId), negocioRef.id)
      .catch((syncError) => logger.warn('[ai-sample] estado del contacto pendiente:', syncError.message));
  } finally {
    runningJobs.delete(negocioRef.id);
  }
}

export async function resumeAiSampleJobs(options) {
  const queued = await options.db.collection('Negocios').where('aiSampleJob.state', '==', 'queued').limit(2).get();
  for (const doc of queued.docs) await runAiSampleJob({ ...options, negocioRef: doc.ref });
  const running = await options.db.collection('Negocios').where('aiSampleJob.state', 'in', ['running', 'sending']).limit(10).get();
  for (const doc of running.docs) {
    const until = doc.data().aiSampleJob?.leaseUntil;
    const expires = until?.toMillis?.() || new Date(until || 0).getTime();
    if (expires && expires < Date.now()) await options.db.runTransaction(async (tx) => {
      const current = (await tx.get(doc.ref)).data() || {};
      const lease = current.aiSampleJob?.leaseUntil;
      if (['running', 'sending'].includes(current.aiSampleJob?.state) && (lease?.toMillis?.() || new Date(lease || 0).getTime()) < Date.now()) {
        tx.update(doc.ref, { 'aiSampleJob.state': 'failed', 'aiSampleJob.error': 'La generación se interrumpió. Puedes reintentar desde el CRM.', status: 'Error muestra IA' });
      }
    });
    const leadId = doc.data().leadId;
    if (leadId && expires && expires < Date.now()) await syncAiSampleFailure(options.db, options.db.collection('leads').doc(leadId), doc.id)
      .catch((error) => (options.logger || console).warn('[ai-sample] estado del contacto pendiente:', error.message));
  }
}

// Pausing keeps completed work; cancellation invalidates any in-flight generation.
export async function controlAiSampleJob({ db, negocioRef, leadRef, action }) {
  if (!['pause', 'cancel', 'resume', 'send', 'retry'].includes(action)) throw Object.assign(Error('Acción de muestra no válida.'), { status: 400 });
  return db.runTransaction(async (tx) => {
    const lead = (await tx.get(leadRef)).data() || {};
    const business = (await tx.get(negocioRef)).data() || {};
    if (business.leadId !== leadRef.id) throw Object.assign(Error('La muestra no pertenece a este contacto.'), { status: 403 });
    const job = business.aiSampleJob || {};
    if (job.state === 'sending') throw Object.assign(Error('El envío ya comenzó. Espera a que termine antes de modificar la muestra.'), { status: 409 });
    if (action === 'send' && (job.state !== 'ready' && job.state !== 'paused' || business.schema?.aiPage?.inputHash !== job.inputHash)) throw Object.assign(Error('La muestra todavía no está lista para enviar.'), { status: 409 });
    if (action === 'retry' && (job.state !== 'failed' || !job.input || !job.inputHash)) throw Object.assign(Error('No hay una solicitud fallida para reintentar.'), { status: 409 });
    if (action === 'resume' && job.state !== 'paused') throw Object.assign(Error('La muestra no está pausada.'), { status: 409 });
    if (['pause', 'cancel'].includes(action) && !['queued', 'running', 'paused', 'ready', 'failed'].includes(job.state)) throw Object.assign(Error('No hay una muestra pendiente para pausar o cancelar.'), { status: 409 });
    if (['resume', 'send', 'retry'].includes(action) && (lead.stopSequences || lead.humanControl || lead.salesBrainHumanControl || lead.archived || lead.archivedAt || ['cliente', 'compro', 'no interesado'].includes(String(lead.estado || '').toLowerCase()))) throw Object.assign(Error('Este contacto no admite entrega automática.'), { status: 409 });
    const paused = action === 'pause', cancelled = action === 'cancel';
    const lease = job.leaseUntil?.toMillis?.() || new Date(job.leaseUntil || 0).getTime();
    const stillGenerating = action === 'resume' && business.schema?.aiPage?.inputHash !== job.inputHash && lease > Date.now();
    const state = paused ? 'paused' : cancelled ? 'cancelled' : stillGenerating ? 'running' : 'queued';
    tx.update(negocioRef, {
      'aiSampleJob.state': state, 'aiSampleJob.error': '', 'aiSampleJob.updatedAt': new Date(),
      ...(cancelled ? { 'aiSampleJob.id': randomUUID() } : {}),
      ...(action === 'send' ? { 'aiSampleJob.reviewBeforeSend': false } : {}),
      status: paused ? 'Muestra pausada' : cancelled ? 'Muestra cancelada' : 'Muestra IA pendiente', updatedAt: new Date(),
    });
    tx.update(leadRef, {
      'sampleAgent.version': randomUUID(), 'sampleAgent.negocioId': negocioRef.id,
      'sampleAgent.enabled': false, 'sampleAgent.status': paused ? 'paused' : cancelled ? 'cancelled' : 'generating',
      'sampleAgent.deliveryPaused': paused || cancelled, 'sampleAgent.pauseReason': cancelled ? 'manual_cancel' : paused ? 'manual_review' : '',
      'sampleAgent.error': '', 'sampleAgent.leaseUntil': new Date(0), 'sampleAgent.leaseOwner': '', 'sampleAgent.updatedAt': new Date(),
    });
    return { success: true, state };
  });
}
