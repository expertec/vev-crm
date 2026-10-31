import multer from 'multer';
import sharp from 'sharp';
import { randomUUID } from 'node:crypto';
import { prepareSampleRecordedAudio, canManageSampleAudios } from './sampleAgentAudioLibrary.js';
import { ms } from './closingConfig.js';
export function registerClosingRoutes(app, { db, bucket, store, agent, readUser }) {
  const auth = async (req, res, next) => { try { const user = await readUser(req); if (!canManageSampleAudios(user)) return res.status(user ? 403 : 401).json({ error: 'Solo administradores pueden gestionar el cierre.' }); req.closingUser = user; next(); } catch { res.status(500).json({ error: 'No se pudo verificar el acceso.' }); } };
  const wrap = fn => async (req, res) => { try { res.json(await fn(req)); } catch (e) { res.status(400).json({ error: e.message }); } };
  app.get('/api/crm/closing/config', auth, wrap(async () => {
    const config = await store.get(), previews = {};
    const paths = [...new Set(config.offers.flatMap(o => [o.imagePath, ...Object.values(o.audios || {}).map(a => a.path)]).filter(Boolean))];
    await Promise.all(paths.map(async path => { if (/^closing-assets\/[a-f0-9-]+\.(png|ogg)$/.test(path)) previews[path] = (await bucket.file(path).getSignedUrl({ action: 'read', expires: new Date(Date.now() + 86400000) }))[0]; }));
    return { config, previews };
  }));
  app.post('/api/crm/closing/config', auth, wrap(async req => ({ config: await store.save({ ...req.body, ownerUid: req.closingUser.uid }) })));
  const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 8 * 1024 * 1024, files: 1, fields: 2 } }).single('file');
  app.post('/api/crm/closing/assets/:kind', auth, (req, res) => upload(req, res, async error => {
    if (error) return res.status(400).json({ error: 'El archivo debe pesar menos de 8 MB.' });
    try {
      if (!req.file?.buffer || !['image', 'audio'].includes(req.params.kind)) throw Error('Selecciona una imagen o audio.');
      const audio = req.params.kind === 'audio';
      const buffer = audio ? (await prepareSampleRecordedAudio(req.file.buffer)).buffer : await sharp(req.file.buffer, { limitInputPixels: 16000000 }).resize({ width: 1600, withoutEnlargement: true }).png().toBuffer();
      const path = `closing-assets/${randomUUID()}.${audio ? 'ogg' : 'png'}`;
      const file = bucket.file(path); await file.save(buffer, { resumable: false, contentType: audio ? 'audio/ogg' : 'image/png' });
      const [url] = await file.getSignedUrl({ action: 'read', expires: new Date(Date.now() + 7 * 86400000) });
      res.json({ path, url });
    } catch (e) { res.status(400).json({ error: e.message }); }
  }));
  app.get('/api/crm/closing/contacts', auth, wrap(async () => {
    const snap = await db.collection('leads').orderBy('closing.updatedAt', 'desc').limit(50).get();
    return { contacts: snap.docs.map(d => { const l = d.data(); return { id: d.id, name: l.nombre || l.telefono || d.id, state: l.closing?.state, offer: l.closing?.offer?.name, error: l.closing?.error || '', updatedAt: ms(l.closing?.updatedAt) }; }) };
  }));
  app.post('/api/crm/closing/contacts/:leadId', auth, wrap(async req => {
    if (!/^[^/]{1,200}$/.test(req.params.leadId)) throw Error('Contacto inválido.');
    const ref = db.collection('leads').doc(req.params.leadId);
    if (!(await ref.get()).exists) throw Error('Contacto no encontrado.');
    if (req.body.action === 'resume') await agent.resume(ref);
    else if (['pause', 'stop'].includes(req.body.action)) await agent.pause(ref, req.body.action === 'stop' ? 'stopped' : 'paused');
    else throw Error('Acción inválida.');
    return { success: true };
  }));
  app.get('/api/crm/closing/fulfillment', auth, wrap(async () => {
    const snap = await db.collection('ClosingFulfillmentJobs').orderBy('updatedAt', 'desc').limit(30).get();
    return { jobs: snap.docs.map(d => ({ id: d.id, ...d.data(), updatedAt: ms(d.data().updatedAt), nextAt: ms(d.data().nextAt) })) };
  }));
  app.post('/api/crm/closing/fulfillment/:id/retry', auth, wrap(async req => {
    if (!/^[a-f0-9]{64}$/.test(req.params.id)) throw Error('Referencia inválida.');
    const ref = db.collection('ClosingFulfillmentJobs').doc(req.params.id), job = (await ref.get()).data();
    if (!job || job.state === 'done') throw Error('No hay una entrega pendiente.');
    if (ms(job.leaseUntil) > Date.now()) throw Error('La entrega sigue en proceso.');
    await ref.update({ state: 'pending', sending: '', leaseUntil: new Date(0), nextAt: new Date(), updatedAt: new Date() }); return { success: true };
  }));
}
