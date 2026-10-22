import test from 'node:test';
import assert from 'node:assert/strict';
import sharp from 'sharp';
import { Readable } from 'node:stream';
import { storedSampleLogoPath, cropSampleLogo, createSampleLogoPreparer } from '../services/sampleLogoPreparation.js';
import { extractConversationBrief, normalizeConversationBrief, missingBriefQuestion, sampleInputFromConversation } from '../services/sampleConversationAgent.js';

test('recorta únicamente el logo y rechaza zonas demasiado pequeñas o fuera de la imagen', async () => {
  const source = await sharp({ create: { width: 400, height: 800, channels: 3, background: '#000' } }).composite([{ input: await sharp({ create: { width: 200, height: 240, channels: 3, background: '#f00' } }).png().toBuffer(), left: 100, top: 200 }]).png().toBuffer();
  const result = await cropSampleLogo(source, [0.25, 0.25, 0.5, 0.3]);
  const meta = await sharp(result).metadata();
  assert.equal(meta.width, 200); assert.equal(meta.height, 240);
  const pixel = await sharp(result).raw().toBuffer(); assert.deepEqual([...pixel.slice(0, 3)], [255, 0, 0]);
  await assert.rejects(cropSampleLogo(source, [0, 0, 0.1, 0.1]), /pequeño/);
  await assert.rejects(cropSampleLogo(source, [0.8, 0, 0.5, 0.4]), /delimitado/);
  assert.equal(storedSampleLogoPath('https://storage.googleapis.com/own/images/logo.jpg', 'own'), 'images/logo.jpg');
  assert.equal(storedSampleLogoPath('https://storage.googleapis.com/other/images/logo.jpg', 'own'), '');
  assert.equal(storedSampleLogoPath('https://example.com/logo.jpg', 'own'), '');
  const files = new Map();
  const prepare = createSampleLogoPreparer({ bucket: { name: 'own', file: (path) => ({ createReadStream: () => Readable.from([source]), save: async (buffer) => files.set(path, buffer), getSignedUrl: async () => [`https://storage.googleapis.com/own/${path}`] }) } });
  assert.match(await prepare({ url: 'https://storage.googleapis.com/own/images/logo.jpg', crop: [0.25, 0.25, 0.5, 0.3] }), /sample-logos/);
  assert.equal(files.size, 1);
});

test('reemplaza la captura por el recorte; si falla pide el original y bloquea la confirmación', async () => {
  const source = 'https://storage.googleapis.com/own/images/phone.jpg';
  const cropped = 'https://storage.googleapis.com/own/sample-logos/clean.png';
  const brief = { companyName: 'Aguilera', activity: 'Instala pisos', objective: 'Contactos', offerings: ['Pisos'], customers: 'Hogares', differentiators: 'Instalación', serviceArea: 'Mante', logoChoice: 'provided', palette: ['#000000', '#ffffff', '#ff0000'], photosChoice: 'none', images: [{ index: 0, role: 'logo', description: 'Águila y nombre', logoStatus: 'crop', logoCrop: [0.1, 0.3, 0.8, 0.4] }] };
  const request = async () => ({ output_text: JSON.stringify({ brief, action: 'confirm', reply: '', audioMoment: 'none' }) });
  const images = [{ url: source }];
  const result = await extractConversationBrief({ images, message: { text: 'Mi logo' }, prepareLogo: async ({ url }) => { assert.equal(url, source); return cropped; } }, request);
  assert.equal(result.brief.images[0].logoStatus, 'clean');
  assert.equal(missingBriefQuestion(result.brief), '');
  assert.equal(sampleInputFromConversation(result.brief, images, '521234').logoURL, cropped);
  assert.equal(images[0].sourceUrl, source);
  const failed = await extractConversationBrief({ images: [{ url: source }], message: { text: 'Mi logo' }, prepareLogo: async () => { throw Error('low quality'); } }, request);
  assert.equal(failed.brief.images[0].logoStatus, 'original_required');
  assert.match(missingBriefQuestion(normalizeConversationBrief(failed.brief, [{ url: source }])), /imagen original/);
});
