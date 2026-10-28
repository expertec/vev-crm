import { createHash, randomUUID } from 'node:crypto';
import multer from 'multer';
import sharp from 'sharp';

const fail = (message, status = 400) => Object.assign(Error(message), { status });
const pathFor = (leadId, uploadId) => `images/manual-logo-${createHash('sha256').update(leadId).digest('hex').slice(0, 24)}-${uploadId}.webp`;

export async function readManualLogoUpload(bucket, leadId, uploadId) {
  if (!/^[a-f0-9-]{36}$/.test(uploadId || '')) throw fail('Selecciona el logo que subiste desde este panel.');
  const file = bucket.file(pathFor(leadId, uploadId));
  let metadata;
  try { [metadata] = await file.getMetadata(); } catch { throw fail('No se encontró el logo de este contacto.'); }
  if (metadata.metadata?.leadId !== leadId || metadata.contentType !== 'image/webp' || !Number(metadata.size) || Number(metadata.size) > 8 * 1024 * 1024) throw fail('El logo no pertenece a este contacto.');
  const [url] = await file.getSignedUrl({ action: 'read', expires: '2100-01-01' });
  return { url, caption: 'Logo subido por el asesor' };
}

export function createManualLogoUploadHandler({ bucket, authorize }) {
  const receive = multer({ storage: multer.memoryStorage(), limits: { fileSize: 8 * 1024 * 1024, files: 1, fields: 0, parts: 2 } }).single('image');
  return async (req, res) => {
    res.set?.('Cache-Control', 'no-store');
    try {
      const { leadId, user } = await authorize(req);
      if (!bucket) throw fail('La carga de logos no está configurada.', 503);
      await new Promise((resolve, reject) => receive(req, res, err => err ? reject(err) : resolve()));
      if (!req.file || !['image/png', 'image/jpeg', 'image/webp'].includes(req.file.mimetype)) throw fail('Usa una imagen JPG, PNG o WebP de hasta 8 MB.');
      let buffer;
      try {
        const image = sharp(req.file.buffer, { limitInputPixels: 32_000_000, animated: false }).rotate();
        if (!['jpeg', 'png', 'webp'].includes((await image.metadata()).format)) throw Error('Formato');
        buffer = await image.resize({ width: 1920, height: 1920, fit: 'inside', withoutEnlargement: true }).webp({ quality: 92 }).toBuffer();
      } catch { throw fail('El archivo no es una imagen válida o es demasiado grande.'); }
      const uploadId = randomUUID(), file = bucket.file(pathFor(leadId, uploadId));
      await file.save(buffer, { resumable: false, metadata: { contentType: 'image/webp', metadata: { leadId, uploadedBy: user.uid } } });
      const [url] = await file.getSignedUrl({ action: 'read', expires: '2100-01-01' });
      res.json({ uploadId, url });
    } catch (error) {
      const limited = error instanceof multer.MulterError;
      res.status(limited ? 413 : error.status || 500).json({ error: limited ? 'Sube una imagen de hasta 8 MB.' : error.status ? error.message : 'No se pudo subir el logo. Intenta nuevamente.' });
    }
  };
}
