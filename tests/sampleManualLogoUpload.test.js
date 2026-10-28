import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import sharp from 'sharp';
import { createManualLogoUploadHandler, readManualLogoUpload } from '../services/sampleManualLogoUpload.js';

test('logo upload authenticates before reading files and saves an optimized draft confined to one lead', async () => {
  const files = new Map();
  const bucket = { file: (path) => ({
    save: async (buffer, options) => files.set(path, { buffer, ...options.metadata, size: buffer.length }),
    getSignedUrl: async () => [`https://storage.googleapis.com/own/${path}`],
    getMetadata: async () => { if (!files.has(path)) throw Error('missing'); return [files.get(path)]; },
  }) };
  const handler = createManualLogoUploadHandler({ bucket, authorize: async (req) => {
    if (!req.allowed) throw Object.assign(Error('Acceso denegado'), { status: 403 });
    return { leadId: 'lead1', user: { uid: 'advisor' } };
  } });
  const call = async (buffer, allowed = true) => {
    const boundary = 'manual-logo-upload';
    const bytes = Buffer.concat([Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="image"; filename="logo.png"\r\nContent-Type: image/png\r\n\r\n`), buffer, Buffer.from(`\r\n--${boundary}--\r\n`)]);
    const req = Readable.from([bytes]); Object.assign(req, { allowed, method: 'POST', headers: { 'content-type': `multipart/form-data; boundary=${boundary}`, 'content-length': String(bytes.length) } });
    let status = 200, body;
    await handler(req, { set() {}, status(s) { status = s; return this; }, json(value) { body = value; } });
    return { status, body };
  };
  const png = await sharp({ create: { width: 800, height: 400, channels: 4, background: '#0099cc' } }).png().toBuffer();
  assert.equal((await call(png, false)).status, 403); assert.equal(files.size, 0);
  assert.equal((await call(Buffer.from('<svg onload="bad()"/>'))).status, 400);
  assert.equal((await call(Buffer.alloc(8 * 1024 * 1024 + 1))).status, 413);
  const result = await call(png);
  assert.equal(result.status, 200); assert.equal(files.size, 1);
  const file = [...files.values()][0]; assert.equal(file.metadata.leadId, 'lead1');
  assert.equal((await sharp(file.buffer).metadata()).format, 'webp');
  assert.equal((await readManualLogoUpload(bucket, 'lead1', result.body.uploadId)).url, result.body.url);
  await assert.rejects(readManualLogoUpload(bucket, 'lead2', result.body.uploadId), /No se encontró/);
  await assert.rejects(readManualLogoUpload(bucket, 'lead1', '../../unsafe'), /panel/);
});
