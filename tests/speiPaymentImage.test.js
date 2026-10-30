import test from 'node:test';
import assert from 'node:assert/strict';
import sharp from 'sharp';
import { renderSpeiPaymentImage, createSpeiPaymentRenderer } from '../services/speiPaymentImage.js';

const record = { planName: 'Plan Emprendedor', amountCents: 49500, totalCents: 99000 };
const instructions = { clabe: '124180652173169491', holder: 'Negocios Web Mx', reference: '644788', bank: 'CITI MEXICO' };

test('rellena la plantilla original y cambia los campos al cambiar los datos reales', async () => {
  const first = await renderSpeiPaymentImage(record, instructions);
  const metadata = await sharp(first).metadata();
  assert.equal(metadata.width, 1254); assert.equal(metadata.height, 1254); assert.equal(metadata.format, 'png');
  const changed = await renderSpeiPaymentImage({ ...record, planName: 'Plan Avanzado', amountCents: 60000 }, { ...instructions, reference: '998877' });
  assert.notDeepEqual(first, changed);
  const escaped = await renderSpeiPaymentImage({ ...record, planName: 'Plan <Web> & Negocio' }, { ...instructions, holder: 'Diseño & Servicios "Norte"' });
  assert.equal((await sharp(escaped).metadata()).format, 'png');
  await assert.rejects(renderSpeiPaymentImage(record, { ...instructions, clabe: '123' }), /CLABE/);
});

test('cuando Stripe indica otro banco sustituye la marca Citi de la plantilla', async () => {
  const citi = await renderSpeiPaymentImage(record, instructions);
  const other = await renderSpeiPaymentImage(record, { ...instructions, bank: 'OTRO BANCO' });
  const logoArea = buffer => sharp(buffer).extract({ left: 840, top: 625, width: 335, height: 75 }).raw().toBuffer();
  assert.notDeepEqual(await logoArea(citi), await logoArea(other));
});

test('guarda la ficha con acceso firmado y reutiliza la imagen sin generar otra', async () => {
  let exists = false, saved = 0;
  const file = { exists: async () => [exists], save: async (buffer, options) => { saved++; exists = true; assert.equal(options.metadata.contentType, 'image/png'); assert.equal((await sharp(buffer).metadata()).width, 1254); }, getSignedUrl: async () => ['https://storage.googleapis.com/private/spei.png'] };
  const render = createSpeiPaymentRenderer({ bucket: { file: path => { assert.match(path, /^payment-vouchers\/spei\//); return file; } } });
  const input = { requestId: 'a'.repeat(64), record, instructions };
  assert.equal(await render(input), 'https://storage.googleapis.com/private/spei.png');
  await render(input); assert.equal(saved, 1);
});
