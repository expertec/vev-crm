import test from 'node:test';
import assert from 'node:assert/strict';
import { compactSampleContext, sampleVisionIndexes } from '../services/sampleAgentContext.js';
import { extractConversationBrief, normalizeConversationBrief } from '../services/sampleConversationAgent.js';
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
