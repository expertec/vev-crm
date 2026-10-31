import test from 'node:test';
import assert from 'node:assert/strict';
import { compactSampleContext, sampleVisionIndexes, mergeSampleImages, canDeferExtraSamplePhotos, deferExtraSamplePhotos, recoverSampleVisionCache } from '../services/sampleAgentContext.js';
import { extractConversationBrief, normalizeConversationBrief, sampleInputFromConversation } from '../services/sampleConversationAgent.js';
import { requestSampleOpenAI } from '../services/aiSampleGenerator.js';
import { recordOpenAiUsage } from '../services/openAiUsage.js';

test('deduplicates chat history without dropping human negotiation markers', () => {
  const result = compactSampleContext([{ role: 'assistant', text: '¿Cómo se llama?', mediaType: 'text' }, { role: 'customer', text: 'FC inoxidables', mediaType: 'text' }], [{ role: 'business', text: '¿Cómo se llama?', mediaType: 'text' }, { role: 'business', text: 'También ofrecemos publicidad', outsideAgent: true }]);
  assert.equal(result.history.length, 1);
  assert.equal(result.history[0].text, 'FC inoxidables');
  assert.equal(result.conversationContext.at(-1).outsideAgent, true);
  assert.equal(compactSampleContext(Array.from({ length: 30 }, (_, i) => ({ text: String(i) }))).history.length, 12);
});

test('reviews each image once, keeps original indexes, rechecks changed files and explicit corrections', async () => {
  const images = [{ url: 'https://storage.googleapis.com/test/first.png' }];
  const calls = [];
  const request = async (_resource, body) => {
    calls.push(body);
    return { status: 'completed', output_text: JSON.stringify({ brief: { ...normalizeConversationBrief({}), images: images.map((_, index) => ({ index, role: 'work', description: `Trabajo en acero ${index}`, logoStatus: 'unknown', logoCrop: [] })) }, action: 'ask', reply: '¿En qué ciudad?', audioMoment: 'none', packageId: '' }) };
  };
  let decision = await extractConversationBrief({ images, message: { text: 'Mis trabajos' } }, request);
  assert.equal(calls[0].input[0].content.filter((item) => item.type === 'input_image').length, 1);
  decision = await extractConversationBrief({ brief: decision.brief, images, message: { text: 'Todo México' } }, request);
  assert.equal(calls[1].input[0].content.filter((item) => item.type === 'input_image').length, 0);
  assert.ok(JSON.parse(calls[1].input[0].content[0].text).images[0].description.includes('acero'));
  images.push({ url: 'https://storage.googleapis.com/test/second.png' });
  await extractConversationBrief({ brief: decision.brief, images, message: { text: '' } }, request);
  const attachments = calls[2].input[0].content;
  assert.equal(attachments.filter((item) => item.type === 'input_image').length, 1);
  assert.ok(attachments.some((item) => item.text === 'Imagen de índice 1:'));
  assert.deepEqual(sampleVisionIndexes(images, { text: 'Revisa bien el logo' }), [0, 1]);
  images[0].url = 'https://storage.googleapis.com/test/crop.png';
  assert.deepEqual(sampleVisionIndexes(images), [0]);
  const fresh = [{ url: 'https://storage.googleapis.com/test/fresh.png' }];
  await assert.rejects(extractConversationBrief({ images: fresh }, async () => { throw Error('failed'); }));
  assert.equal(fresh[0].visionReviewedUrl, undefined);
});

test('admits a logo arriving after thirty product photos while keeping bounded context, cached roles and primary photos', () => {
  const images = Array.from({ length: 12 }, (_, index) => ({ url: `https://storage.googleapis.com/test/shoe${index}.jpg`, visionReviewedUrl: `https://storage.googleapis.com/test/shoe${index}.jpg` }));
  const brief = { images: images.map((_, index) => ({ index, role: index === 10 ? 'logo' : 'product', description: `Imagen ${index}` })) };
  const incoming = Array.from({ length: 30 }, (_, i) => ({ url: `https://storage.googleapis.com/test/new${i}.jpg` }));
  const logo = { url: 'https://storage.googleapis.com/test/lona.jpg' };
  const result = mergeSampleImages(brief, images, [...incoming, logo]);
  assert.equal(result.images.length, 12); assert.equal(result.images.at(-1).url, logo.url);
  assert.ok(result.images.some((image) => image.url === images[10].url));
  for (const role of result.brief.images) assert.equal(role.description, brief.images.find((old) => images[old.index].url === result.images[role.index].url).description);
  for (const i of [0, 1, 2]) assert.ok(result.images.some((image) => image.url === images[i].url));
  assert.ok(sampleVisionIndexes(result.images).includes(11));
  const cached = result.images.map((image) => ({ ...image, visionReviewedUrl: image.url }));
  assert.deepEqual(sampleVisionIndexes(cached, { text: 'Ese es el logotipo que te mandé en la foto del toldo' }), [9, 10, 11]);
  assert.deepEqual(sampleVisionIndexes(cached, { text: 'Gracias' }), []);
});

test('records provider counters without customer content and marks unavailable usage honestly', async () => {
  const events = [];
  await recordOpenAiUsage({ resource: 'responses', model: 'test', purpose: 'sample_brief', context: { leadId: 'lead', prompt: 'PRIVATE' }, usage: { input_tokens: 100, output_tokens: 20, input_tokens_details: { cached_tokens: 40 } } }, async (event) => events.push(event));
  assert.equal(events[0].totalTokens, 120);
  assert.equal(events[0].cachedInputTokens, 40);
  assert.equal(events[0].leadId, 'lead');
  assert.ok(!JSON.stringify(events[0]).includes('PRIVATE'));
  await recordOpenAiUsage({ resource: 'images/generations' }, async (event) => events.push(event));
  assert.equal(events[1].usageAvailable, false);
});

test('API reports actual purpose, image count, usage, request id and quota failures', async () => {
  const events = [];
  const options = { apiKey: 'test', recordUsage: async (event) => events.push(event), fetchImpl: async () => ({ ok: true, status: 200, json: async () => ({ id: 'response-1', usage: { input_tokens: 50, output_tokens: 10 } }) }) };
  await requestSampleOpenAI('responses', { model: 'test', text: { format: { name: 'sample_brief' } }, input: [{ content: [{ type: 'input_image', image_url: 'PRIVATE' }] }] }, options);
  assert.equal(events[0].purpose, 'sample_brief'); assert.equal(events[0].imageCount, 1); assert.equal(events[0].requestId, 'response-1');
  assert.ok(!JSON.stringify(events[0]).includes('PRIVATE'));
  await assert.rejects(requestSampleOpenAI('responses', {}, { ...options, fetchImpl: async () => ({ ok: false, status: 429, json: async () => ({ error: { code: 'insufficient_quota', message: 'No credits' } }) }) }), (error) => error.code === 'insufficient_quota' && error.status === 503);
});

const reviewedPhotos = () => Array.from({ length: 3 }, (_, index) => ({ url: `https://storage.googleapis.com/test/photo${index}.jpg`, visionReviewedUrl: `https://storage.googleapis.com/test/photo${index}.jpg` }));
const photoBrief = () => ({ logoChoice: 'none', photosChoice: 'provided', images: reviewedPhotos().map((_, index) => ({ index, role: 'product', description: 'Producto disponible' })) });

test('saves extra photos without analysis or sending them to generation; explicit replacement can review them later', () => {
  const images = reviewedPhotos();
  const incoming = [{ url: 'https://storage.googleapis.com/test/extra.jpg' }];
  assert.equal(canDeferExtraSamplePhotos(photoBrief(), images, incoming), true);
  const merged = mergeSampleImages(photoBrief(), images, incoming);
  const brief = deferExtraSamplePhotos(merged.brief, merged.images, incoming);
  assert.deepEqual(sampleVisionIndexes(merged.images), []);
  const input = sampleInputFromConversation(brief, merged.images, '');
  assert.deepEqual(input.photoURLs, images.map(item => item.url));
  assert.deepEqual(input.referenceImageURLs, []);
  assert.equal(input.imageContext.length, 3);
  assert.deepEqual(sampleVisionIndexes(merged.images, { text: 'Usa la última foto' }, brief), [3]);
  assert.deepEqual(sampleVisionIndexes(merged.images, { text: 'La última foto es el logo' }, brief), [3]);
  assert.deepEqual(sampleVisionIndexes(merged.images, { text: 'Revisa la primera foto' }, brief), [0]);
});

test('does not skip information slides, captions, a pending logo, or initial product photos', () => {
  const images = reviewedPhotos();
  const incoming = [{ url: 'https://storage.googleapis.com/test/extra.jpg' }];
  for (const description of ['Diapositiva con servicios', 'Presentación del despacho', 'Catálogo con precios']) {
    const brief = photoBrief(); brief.images[0].description = description;
    assert.equal(canDeferExtraSamplePhotos(brief, images, incoming), false);
  }
  assert.equal(canDeferExtraSamplePhotos(photoBrief(), images, incoming, 'Estos son mis servicios'), false);
  assert.equal(canDeferExtraSamplePhotos(photoBrief(), images, [{ ...incoming[0], caption: 'Logo nuevo' }]), false);
  assert.equal(canDeferExtraSamplePhotos({ ...photoBrief(), logoChoice: 'provided' }, images, incoming), false);
  assert.equal(canDeferExtraSamplePhotos(photoBrief(), images.slice(0, 2), incoming), false);
});

test('incremental visual responses preserve previously reviewed descriptions and do not reattach their files', async () => {
  const images = reviewedPhotos();
  const brief = normalizeConversationBrief(photoBrief(), images);
  const result = await extractConversationBrief({ brief, images, message: { text: 'Estamos en Mérida' } }, async (_resource, body) => {
    assert.equal(body.input[0].content.filter(item => item.type === 'input_image').length, 0);
    return { status: 'completed', output_text: JSON.stringify({ brief: { ...brief, serviceArea: 'Mérida', images: [] }, action: 'ask', reply: '¿Cómo se llama el negocio?' }) };
  });
  assert.deepEqual(result.brief.images, brief.images);
  assert.equal(result.brief.serviceArea, 'Mérida');
});

test('reuses reviewed files from an interrupted batch using URLs rather than stale indexes or decisions', () => {
  const cached = { url: 'https://storage.googleapis.com/test/crop.jpg', sourceUrl: 'https://storage.googleapis.com/test/original.jpg', visionReviewedUrl: 'https://storage.googleapis.com/test/crop.jpg', visionDescription: 'Logo limpio' };
  const pending = [{ url: 'https://storage.googleapis.com/test/new.jpg' }, { url: cached.sourceUrl }];
  const result = recoverSampleVisionCache({ companyName: 'Actual', images: [] }, pending, [{ action: 'generate', brief: { companyName: 'Obsoleto', images: [{ index: 0, role: 'logo', description: 'Logo limpio', logoStatus: 'clean' }] }, images: [cached] }]);
  assert.equal(result.brief.companyName, 'Actual');
  assert.equal(result.brief.images[0].index, 1);
  assert.equal(result.images[1].url, cached.url);
  assert.deepEqual(sampleVisionIndexes(result.images), [0]);
  assert.equal(result.action, undefined);
});
