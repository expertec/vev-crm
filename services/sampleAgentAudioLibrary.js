import multer from 'multer';
import { randomUUID } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import ffmpegInstaller from '@ffmpeg-installer/ffmpeg';
import ffprobeInstaller from '@ffprobe-installer/ffprobe';

export const SAMPLE_AUDIO_MOMENTS = ['welcome', 'business', 'objective', 'services', 'branding', 'photos', 'confirmation', 'generating'];
const collectionName = 'sampleAgentAudios';
const exec = promisify(execFile);
const clean = (value, limit) => String(value || '').trim().slice(0, limit);
export function canManageSampleAudios(user) {
  return user?.active !== false && ['admin', 'superadmin'].includes(String(user?.role || '').toLowerCase().replace(/_/g, ''));
}

export async function prepareSampleRecordedAudio(buffer) {
  const folder = await fs.mkdtemp(path.join(os.tmpdir(), 'nw-sample-audio-'));
  const input = path.join(folder, 'recording');
  const output = path.join(folder, 'recording.ogg');
  try {
    await fs.writeFile(input, buffer);
    const probe = await exec(ffprobeInstaller.path, ['-v', 'error', '-protocol_whitelist', 'file,pipe', '-show_entries', 'format=duration:stream=codec_type', '-of', 'json', input], { timeout: 15_000, maxBuffer: 64 * 1024 });
    const metadata = JSON.parse(probe.stdout);
    const seconds = Number(metadata.format?.duration);
    if (!metadata.streams?.some((stream) => stream.codec_type === 'audio')) throw new Error('El archivo no contiene audio.');
    if (metadata.streams.some((stream) => stream.codec_type === 'video')) throw new Error('Graba o sube un archivo de audio, sin vídeo.');
    // Some MediaRecorder WebM files have no duration. Encode with a hard cap, then measure.
    if (Number.isFinite(seconds) && seconds > 90) throw new Error('La grabación debe durar como máximo 90 segundos.');
    await exec(ffmpegInstaller.path, ['-y', '-v', 'error', '-protocol_whitelist', 'file,pipe', '-i', input, '-t', '91', '-vn', '-ac', '1', '-ar', '48000', '-c:a', 'libopus', '-b:a', '32k', output], { timeout: 30_000, maxBuffer: 64 * 1024 });
    const encoded = await exec(ffprobeInstaller.path, ['-v', 'error', '-show_entries', 'format=duration', '-of', 'json', output], { timeout: 15_000, maxBuffer: 4096 });
    const duration = Number(JSON.parse(encoded.stdout).format?.duration);
    if (!Number.isFinite(duration) || duration <= 0 || duration > 90.1) throw new Error('La grabación debe durar entre 1 y 90 segundos.');
    return { buffer: await fs.readFile(output), duration };
  } finally { await fs.rm(folder, { recursive: true, force: true }); }
}

export function registerSampleAgentAudioRoutes(app, { db, bucket, readUser, prepare = prepareSampleRecordedAudio, logger = console }) {
  const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 8 * 1024 * 1024, files: 1, fields: 5 } }).single('audio');
  const authorize = async (req, res, next) => {
    try {
      const user = await readUser(req);
      if (!user) return res.status(401).json({ error: 'Inicia sesión para administrar los audios.' });
      if (!canManageSampleAudios(user)) return res.status(403).json({ error: 'Solo un administrador puede cambiar los audios del flujo.' });
      req.sampleAudioUser = user;
      next();
    } catch { res.status(500).json({ error: 'No se pudo validar el acceso.' }); }
  };
  const validMoment = (req, res, next) => SAMPLE_AUDIO_MOMENTS.includes(req.params.moment) ? next() : res.status(400).json({ error: 'Momento del flujo no válido.' });
  const handleError = (res, error) => {
    logger.warn('[sample-agent-audio]', error.message);
    res.status(400).json({ error: error.message || 'No se pudo guardar el audio.' });
  };
  app.get('/api/crm/sample-agent/audios', authorize, async (_req, res) => {
    try {
      const snap = await db.collection(collectionName).get();
      res.json({ audios: snap.docs.filter((doc) => SAMPLE_AUDIO_MOMENTS.includes(doc.id)).map((doc) => ({ ...doc.data(), moment: doc.id })) });
    } catch (error) { handleError(res, error); }
  });
  app.post('/api/crm/sample-agent/audios/:moment', authorize, validMoment, (req, res) => {
    upload(req, res, async (uploadError) => {
      if (uploadError) return res.status(400).json({ error: uploadError.code === 'LIMIT_FILE_SIZE' ? 'El audio debe pesar menos de 8 MB.' : 'No se pudo recibir el audio.' });
      let storedFile;
      try {
        if (!req.file?.buffer?.length) throw new Error('Graba o selecciona un audio primero.');
        const title = clean(req.body.title, 100);
        const script = clean(req.body.script, 1600);
        if (!title || !script) throw new Error('Escribe el nombre y el texto que dice el audio.');
        const prepared = await prepare(req.file.buffer);
        const storagePath = `sample-agent-audio/${req.params.moment}/${randomUUID()}.ogg`;
        storedFile = bucket.file(storagePath);
        await storedFile.save(prepared.buffer, { contentType: 'audio/ogg', resumable: false });
        const [url] = await storedFile.getSignedUrl({ action: 'read', expires: '2100-01-01' });
        const audio = { title, script, storagePath, url, enabled: req.body.enabled === 'true', duration: prepared.duration, updatedBy: req.sampleAudioUser.uid, updatedAt: new Date() };
        await db.collection(collectionName).doc(req.params.moment).set(audio);
        res.json({ success: true, audio: { ...audio, moment: req.params.moment } });
      } catch (error) {
        if (storedFile) await storedFile.delete({ ignoreNotFound: true }).catch(() => {});
        handleError(res, error);
      }
    });
  });
  app.patch('/api/crm/sample-agent/audios/:moment', authorize, validMoment, async (req, res) => {
    try {
      if (typeof req.body.enabled !== 'boolean') throw new Error('Indica si deseas activar o desactivar el audio.');
      const ref = db.collection(collectionName).doc(req.params.moment);
      if (!(await ref.get()).exists) return res.status(404).json({ error: 'Todavía no hay un audio grabado para este momento.' });
      await ref.update({ enabled: req.body.enabled, updatedBy: req.sampleAudioUser.uid, updatedAt: new Date() });
      res.json({ success: true });
    } catch (error) { handleError(res, error); }
  });
  app.delete('/api/crm/sample-agent/audios/:moment', authorize, validMoment, async (req, res) => {
    try {
      const ref = db.collection(collectionName).doc(req.params.moment);
      const data = (await ref.get()).data();
      await ref.delete();
      if (data?.storagePath?.startsWith(`sample-agent-audio/${req.params.moment}/`)) await bucket.file(data.storagePath).delete({ ignoreNotFound: true });
      res.json({ success: true });
    } catch (error) { handleError(res, error); }
  });
}

export function sampleAudioMoment(turn, agent) {
  if (turn.action === 'generate') return 'generating';
  if (turn.action === 'confirm') return 'confirmation';
  if (turn.action !== 'ask') return '';
  if (!agent.history?.length) return 'welcome';
  const brief = turn.brief;
  if (!brief.companyName || !brief.activity) return 'business';
  if (!brief.objective) return 'objective';
  if (!brief.offerings?.length) return 'services';
  if (!brief.customers || !brief.differentiators || !brief.serviceArea) return '';
  if (brief.logoChoice === 'pending' || brief.logoChoice === 'none' && !brief.colorPreference) return 'branding';
  if (brief.photosChoice === 'pending' || brief.images?.some((image) => image.role === 'unclassified')) return 'photos';
  return '';
}

export function createSampleFlowAudioSender({ db, bucket, sendAudio }) {
  return async ({ leadId, moment, isCurrent }) => {
    if (!SAMPLE_AUDIO_MOMENTS.includes(moment)) return false;
    const audio = (await db.collection(collectionName).doc(moment).get()).data();
    if (!audio?.enabled || !new RegExp(`^sample-agent-audio/${moment}/[a-f0-9-]+\\.ogg$`).test(audio.storagePath || '')) return false;
    const [url] = await bucket.file(audio.storagePath).getSignedUrl({ action: 'read', expires: '2100-01-01' });
    if (isCurrent && !await isCurrent()) return false;
    await sendAudio(leadId, url, { ptt: true, forwarded: false, mimetype: 'audio/ogg; codecs=opus' });
    return true;
  };
}

export async function loadSampleFlowAudioOptions(db) {
  const snap = await db.collection(collectionName).get();
  return snap.docs.filter((doc) => SAMPLE_AUDIO_MOMENTS.includes(doc.id) && doc.data().enabled)
    .map((doc) => ({ moment: doc.id, script: clean(doc.data().script, 1600) })).filter((audio) => audio.script);
}
