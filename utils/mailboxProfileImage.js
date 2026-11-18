import sharp from 'sharp';

export async function prepareMailboxPhoto(buffer) {
  try {
    const input = sharp(buffer, { limitInputPixels: 16000000, animated: false });
    const metadata = await input.metadata();
    if (!['jpeg', 'png', 'webp', 'gif'].includes(metadata.format)) throw new Error('unsupported');
    return await input.rotate().resize(256, 256, { fit: 'cover' }).webp({ quality: 85 }).toBuffer();
  } catch {
    const error = new Error('Selecciona una imagen PNG, JPG, WebP o GIF válida.');
    error.statusCode = 400;
    error.code = 'MAILBOX_PHOTO_INVALID';
    throw error;
  }
}
