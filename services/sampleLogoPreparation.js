import sharp from 'sharp';
import { createHash } from 'node:crypto';

export function storedSampleLogoPath(value, bucketName) {
  try {
    const url = new URL(value);
    if (url.protocol !== 'https:' || url.username || url.password || url.port) return '';
    const prefix = url.hostname === 'storage.googleapis.com' ? `/${bucketName}/` : url.hostname === 'firebasestorage.googleapis.com' ? `/v0/b/${bucketName}/o/` : '';
    if (!prefix || !url.pathname.startsWith(prefix)) return '';
    const path = decodeURIComponent(url.pathname.slice(prefix.length));
    return /^images\/[^/\\\u0000]+\.(jpg|jpeg|png|webp)$/i.test(path) ? path : '';
  } catch { return ''; }
}

export async function cropSampleLogo(buffer, crop) {
  if (!Array.isArray(crop) || crop.length !== 4 || !crop.every(Number.isFinite)) throw Error('Recorte no válido.');
  const [x, y, width, height] = crop;
  if (x < 0 || y < 0 || width <= 0 || height <= 0 || x + width > 1.001 || y + height > 1.001 || width * height > 0.92) throw Error('El logo no está bien delimitado.');
  const upright = await sharp(buffer, { limitInputPixels: 32_000_000 }).rotate().toBuffer();
  const meta = await sharp(upright).metadata();
  const left = Math.round(x * meta.width), top = Math.round(y * meta.height);
  const w = Math.min(Math.round(width * meta.width), meta.width - left), h = Math.min(Math.round(height * meta.height), meta.height - top);
  if (Math.min(w, h) < 160) throw Error('El logo es demasiado pequeño.');
  return sharp(upright).extract({ left, top, width: w, height: h }).png().toBuffer();
}

export function createSampleLogoPreparer({ bucket }) {
  return async ({ url, crop }) => {
    const source = storedSampleLogoPath(url, bucket.name);
    if (!source) throw Error('La imagen no pertenece al almacenamiento de WhatsApp.');
    const stream = bucket.file(source).createReadStream(), chunks = [];
    const timer = setTimeout(() => stream.destroy(Error('Tiempo de lectura agotado.')), 20_000);
    let size = 0;
    try {
      for await (const chunk of stream) { size += chunk.length; if (size > 12 * 1024 * 1024) throw Error('Imagen demasiado grande.'); chunks.push(chunk); }
    } finally { clearTimeout(timer); stream.destroy(); }
    const png = await cropSampleLogo(Buffer.concat(chunks), crop);
    const key = createHash('sha256').update(source).update(JSON.stringify(crop)).digest('hex');
    const file = bucket.file(`sample-logos/${key}.png`);
    await file.save(png, { contentType: 'image/png', resumable: false });
    return (await file.getSignedUrl({ action: 'read', expires: '2100-01-01' }))[0];
  };
}
