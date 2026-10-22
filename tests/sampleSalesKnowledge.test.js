import test from 'node:test';
import assert from 'node:assert/strict';
import { SAMPLE_CAMPAIGN_OFFER, createSampleSalesKnowledgeLoader } from '../services/sampleSalesKnowledge.js';
import { extractConversationBrief, interpretSampleApproval, normalizeConversationBrief } from '../services/sampleConversationAgent.js';

test('el agente recibe la oferta de campaña separada de los datos del cliente', async () => {
  await extractConversationBrief({ message: { text: '¿Qué costo tiene y qué incluye?' }, salesKnowledge: { price: '$990 MXN al año', included: ['Diseño adaptable', 'Botón de WhatsApp'] } }, async (_resource, body) => {
    assert.match(body.instructions, /\$990 MXN al año/);
    assert.match(body.instructions, /Diseño adaptable/);
    assert.match(body.instructions, /Responde primero precio/);
    assert.doesNotMatch(JSON.parse(body.input[0].content[0].text).brief?.objective || '', /990/);
    return { output_text: JSON.stringify({ brief: normalizeConversationBrief({}), action: 'respond', reply: 'La página cuesta $990 MXN al año.', audioMoment: 'none' }) };
  });
});

test('las dudas de precio después del resumen también reciben la oferta verificada', async () => {
  const result = await interpretSampleApproval({ brief: {}, text: '¿Cuánto cuesta?', salesKnowledge: SAMPLE_CAMPAIGN_OFFER }, async (_resource, body) => {
    assert.match(body.instructions, /\$990 MXN al año/);
    return { output_text: JSON.stringify({ intent: 'question', reply: 'Son $990 MXN al año.' }) };
  });
  assert.equal(result.intent, 'question');
});

test('usa el paquete principal activo, conserva caché y actualiza tras guardar', async () => {
  let reads = 0;
  const load = createSampleSalesKnowledgeLoader({ collection(name) {
    if (name === 'config') return { doc: () => ({ get: async () => { reads++; return { data: () => ({ sampleAgentWebPackageId: 'campaign', sampleAgentWebOffer: { price: '$1390' } }) }; } }) };
    assert.equal(name, 'sampleAgentPackages');
    return { where: () => ({ limit: () => ({ get: async () => ({ docs: [{ id: 'campaign', data: () => ({ name: 'Campaña', price: '$990 MXN al año', included: ['Dominio'], image: { url: 'private' } }) }] }) }) }) };
  } });
  assert.deepEqual((await load()).included, ['Dominio']);
  assert.equal((await load()).price, '$990 MXN al año');
  assert.equal((await load()).defaultPackageId, 'campaign');
  assert.equal((await load()).packages[0].image, undefined);
  assert.equal(reads, 1);
  load.invalidate(); await load(); assert.equal(reads, 2);
});
