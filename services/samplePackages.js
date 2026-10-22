import multer from 'multer';
import sharp from 'sharp';
import { randomUUID } from 'node:crypto';
import { canManageSampleAudios, prepareSampleRecordedAudio } from './sampleAgentAudioLibrary.js';

const COLLECTION = 'sampleAgentPackages';
const validId = (id) => /^[a-f0-9-]{36}$/.test(id || '');
const clean = (value, max) => String(value || '').trim().slice(0, max);
export function normalizeSamplePackage(data) {
  const included = (Array.isArray(data.included) ? data.included : String(data.included || '').split('\n')).map((value) => clean(value, 200)).filter(Boolean).slice(0, 15);
  return { name: clean(data.name, 100), price: clean(data.price, 100), included, description: clean(data.description, 800), audioScript: clean(data.audioScript, 1600), enabled: data.enabled === true || data.enabled === 'true' };
}
export const packageSummary = (data) => `${data.name}\n${data.price}${data.included?.length ? `\n\nIncluye:\n${data.included.map((item) => `• ${item}`).join('\n')}` : ''}${data.description ? `\n\n${data.description}` : ''}`;

export function registerSamplePackageRoutes(app, { db, bucket, readUser, invalidate = () => {}, prepareAudio = prepareSampleRecordedAudio }) {
  const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 8 * 1024 * 1024, files: 2, fields: 12 } }).fields([{ name: 'image', maxCount: 1 }, { name: 'audio', maxCount: 1 }]);
  const authorize = async (req, res, next) => {
    try {
      const user = await readUser(req);
      if (!user) return res.status(401).json({ error: 'Inicia sesión para administrar paquetes.' });
      if (!canManageSampleAudios(user)) return res.status(403).json({ error: 'Solo un administrador puede cambiar los paquetes.' });
      req.packageUser = user; next();
    } catch { res.status(500).json({ error: 'No se pudo validar el acceso.' }); }
  };
  app.get('/api/crm/sample-agent/packages', authorize, async (_req, res) => {
    try {
      const [snap, config] = await Promise.all([db.collection(COLLECTION).get(), db.collection('config').doc('appConfig').get()]);
      res.json({ packages: snap.docs.map((doc) => ({ ...doc.data(), id: doc.id })), defaultPackageId: config.data()?.sampleAgentWebPackageId || '' });
    } catch { res.status(500).json({ error: 'No se pudieron cargar los paquetes.' }); }
  });
  app.post('/api/crm/sample-agent/packages/:id', authorize, (req, res) => {
    if (req.params.id !== 'new' && !validId(req.params.id)) return res.status(400).json({ error: 'Paquete no válido.' });
    upload(req, res, async (uploadError) => {
      if (uploadError) return res.status(400).json({ error: 'Usa una imagen y un audio de hasta 8 MB cada uno.' });
      const created = [];
      try {
        const ref = req.params.id === 'new' ? db.collection(COLLECTION).doc(randomUUID()) : db.collection(COLLECTION).doc(req.params.id);
        const previous = (await ref.get()).data() || {};
        const data = normalizeSamplePackage(req.body);
        if (!data.name || !data.price || !data.included.length) throw Error('Indica nombre, precio con periodicidad y qué incluye.');
        let image = req.body.removeImage === 'true' ? null : previous.image || null;
        let audio = req.body.removeAudio === 'true' ? null : previous.audio || null;
        const store = async (buffer, extension, contentType) => {
          const storagePath = `sample-packages/${ref.id}/${randomUUID()}.${extension}`;
          const file = bucket.file(storagePath);
          await file.save(buffer, { contentType, resumable: false }); created.push(file);
          return { storagePath, url: (await file.getSignedUrl({ action: 'read', expires: '2100-01-01' }))[0] };
        };
        if (req.files?.image?.[0]) {
          const buffer = await sharp(req.files.image[0].buffer, { limitInputPixels: 32_000_000 }).rotate().resize({ width: 1600, height: 2000, fit: 'inside', withoutEnlargement: true }).webp({ quality: 85 }).toBuffer();
          image = await store(buffer, 'webp', 'image/webp');
        }
        if (req.files?.audio?.[0]) {
          if (!data.audioScript) throw Error('Escribe lo que dice el audio para que el agente sepa cuándo usarlo.');
          const prepared = await prepareAudio(req.files.audio[0].buffer);
          audio = { ...await store(prepared.buffer, 'ogg', 'audio/ogg'), duration: prepared.duration };
        }
        if (audio && data.audioScript !== previous.audioScript && !req.files?.audio?.[0]) throw Error('Para cambiar lo que dice el audio, graba o sube su reemplazo.');
        const changedOffer = previous.name && (data.price !== previous.price || JSON.stringify(data.included) !== JSON.stringify(previous.included));
        if (changedOffer && ((audio && !req.files?.audio?.[0]) || (image && !req.files?.image?.[0]))) throw Error('Si cambias el precio o las inclusiones, reemplaza o quita los materiales anteriores para no enviar información desactualizada.');
        const configRef = db.collection('config').doc('appConfig');
        const config = (await configRef.get()).data() || {};
        const batch = db.batch();
        batch.set(ref, { ...data, image, audio, updatedBy: req.packageUser.uid, updatedAt: new Date() });
        if (req.body.isDefault === 'true' || !config.sampleAgentWebPackageId) batch.set(configRef, { sampleAgentWebPackageId: ref.id }, { merge: true });
        await batch.commit(); invalidate();
        res.json({ success: true, package: { ...data, image, audio, id: ref.id } });
      } catch (error) {
        await Promise.allSettled(created.map((file) => file.delete({ ignoreNotFound: true })));
        res.status(400).json({ error: error.message || 'No se pudo guardar el paquete.' });
      }
    });
  });
  app.delete('/api/crm/sample-agent/packages/:id', authorize, async (req, res) => {
    if (!validId(req.params.id)) return res.status(400).json({ error: 'Paquete no válido.' });
    try {
      await db.collection(COLLECTION).doc(req.params.id).delete(); invalidate();
      res.json({ success: true });
    } catch { res.status(500).json({ error: 'No se pudo eliminar el paquete.' }); }
  });
}

export function createSamplePackageSender({ db, bucket, sendAudio, sendImage, sendText, logger = console }) {
  return async ({ leadId, packageId, isCurrent, state = {}, save = async () => {} }) => {
    if (!validId(packageId)) return null;
    const data = (await db.collection(COLLECTION).doc(packageId).get()).data();
    if (!data?.enabled) return null;
    const sent = { ...state };
    const sendAsset = async (type, send) => {
      const asset = data[type];
      if (!asset || sent[type]) return;
      if (!new RegExp(`^sample-packages/${packageId}/[a-f0-9-]+\\.${type === 'audio' ? 'ogg' : 'webp'}$`).test(asset.storagePath || '')) return;
      if (!await isCurrent()) return;
      try {
        const [url] = await bucket.file(asset.storagePath).getSignedUrl({ action: 'read', expires: '2100-01-01' });
        await send(url); sent[type] = 'sent';
      } catch (error) { sent[type] = 'failed'; logger.warn('[sample-package]', type, error.message); }
      await save(sent);
    };
    await sendAsset('image', (url) => sendImage(leadId, url, ''));
    await sendAsset('audio', (url) => sendAudio(leadId, url, { ptt: true, forwarded: false, mimetype: 'audio/ogg; codecs=opus' }));
    if (!await isCurrent()) return null;
    if (sent.audio !== 'sent' && !sent.text) { await sendText(leadId, packageSummary(data)); sent.text = 'sent'; await save(sent); }
    return { text: sent.audio === 'sent' ? data.audioScript : packageSummary(data) };
  };
}
