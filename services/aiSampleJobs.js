import { randomUUID } from 'node:crypto';
import { AI_SAMPLE_VERSION, aiSampleInputHash, generateAiSample, normalizeAiSampleInput, publicImageUrl } from './aiSampleGenerator.js';

const LEASE_MS = 15 * 60_000;
const runningJobs = new Set();

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
    if (['queued', 'running'].includes(job.state)) return { queued: true, duplicate: true };
    if ((job.state === 'sent' || current.aiSampleLastSentHash === inputHash) && current.schema?.aiPage?.inputHash === inputHash) {
      tx.update(negocioRef, { status: 'Web enviada', 'aiSampleJob.state': 'sent' });
      return { queued: false, duplicate: true, ready: true };
    }
    tx.set(negocioRef, {
      ...data,
      aiSampleJob: { id: randomUUID(), state: 'queued', input, inputHash, requestedAt: new Date(), error: '' },
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
        savedDesign: claimed.aiSampleDraft?.inputHash === inputHash ? claimed.aiSampleDraft.design : null,
        saveDesign: (design) => negocioRef.update({ aiSampleDraft: { inputHash, design } }),
      });
    await db.runTransaction(async (tx) => {
      const latest = (await tx.get(negocioRef)).data() || {};
      if (latest.aiSampleJob?.id !== jobId || latest.aiSampleJob?.state !== 'running') throw new Error('La solicitud de muestra cambió durante la generación.');
      tx.update(negocioRef, {
        schema, designVersion: AI_SAMPLE_VERSION,
        status: 'Muestra IA lista', processedAt: new Date(), lastGeneratedAt: new Date(), updatedAt: new Date(),
        'aiSampleJob.generatedAt': new Date(),
        aiSampleDraft: null,
      });
    });
    const latest = (await negocioRef.get()).data() || {};
    await sendReady({ id: negocioRef.id, ...latest });
    await negocioRef.update({ 'aiSampleJob.state': 'sent', 'aiSampleJob.finishedAt': new Date(), 'aiSampleJob.error': '' });
  } catch (error) {
    logger.warn('[ai-sample] generación/entrega fallida:', error?.message || error);
    if (claimed) await negocioRef.update({
      'aiSampleJob.state': 'failed', 'aiSampleJob.error': String(error?.message || error).slice(0, 500),
      'aiSampleJob.finishedAt': new Date(),
      // Keep an existing usable schema and let the CRM retry delivery without new API calls.
      status: 'Error muestra IA',
    }).catch(() => {});
  } finally {
    runningJobs.delete(negocioRef.id);
  }
}

export async function resumeAiSampleJobs(options) {
  const queued = await options.db.collection('Negocios').where('aiSampleJob.state', '==', 'queued').limit(2).get();
  for (const doc of queued.docs) await runAiSampleJob({ ...options, negocioRef: doc.ref });
  const running = await options.db.collection('Negocios').where('aiSampleJob.state', '==', 'running').limit(10).get();
  for (const doc of running.docs) {
    const until = doc.data().aiSampleJob?.leaseUntil;
    const expires = until?.toMillis?.() || new Date(until || 0).getTime();
    if (expires && expires < Date.now()) await options.db.runTransaction(async (tx) => {
      const current = (await tx.get(doc.ref)).data() || {};
      const lease = current.aiSampleJob?.leaseUntil;
      if (current.aiSampleJob?.state === 'running' && (lease?.toMillis?.() || new Date(lease || 0).getTime()) < Date.now()) {
        tx.update(doc.ref, { 'aiSampleJob.state': 'failed', 'aiSampleJob.error': 'La generación se interrumpió. Puedes reintentar desde el CRM.', status: 'Error muestra IA' });
      }
    });
  }
}
