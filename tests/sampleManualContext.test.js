import test from 'node:test';
import assert from 'node:assert/strict';
import { manualSampleContext } from '../services/sampleManualContext.js';
import { manualSampleInput } from '../services/sampleProgress.js';
import { normalizeConversationBrief, sampleInputFromConversation } from '../services/sampleConversationAgent.js';

const fields = { companyName: 'Coroinox', activity: 'Fabricación en acero inoxidable', objective: 'Cotizaciones', offerings: ['Cazos', 'Equipos industriales'], customers: '', differentiators: '', serviceArea: '', colorPreference: '', withoutLogo: true, logoImageIndex: 0 };
test('manual context fixes name and discards an unusable logo even when classified as work', () => {
  const images = [{ url: 'https://storage.googleapis.com/test/bad-logo.jpg' }, { url: 'https://storage.googleapis.com/test/work.jpg' }];
  const lead = { telefono: '521234567890', sampleAgent: { brief: normalizeConversationBrief({ companyName: 'Nombre incorrecto', logoChoice: 'pending', photosChoice: 'provided', images: [{ index: 0, role: 'work', description: 'Foto de metal' }, { index: 1, role: 'work', description: 'Cazo terminado' }] }, images), images } };
  const brief = manualSampleContext(lead, fields);
  assert.equal(brief.companyName, 'Coroinox'); assert.equal(brief.logoChoice, 'none'); assert.match(brief.colorPreference, /Proponer/);
  lead.sampleAgent.brief = brief;
  for (const input of [manualSampleInput(lead), sampleInputFromConversation(brief, images, lead.telefono)]) {
    assert.equal(input.logoURL, '');
    assert.deepEqual(input.photoURLs, [images[1].url]);
    assert.deepEqual(input.referenceImageURLs, []);
    assert.ok(!JSON.stringify(input).includes('bad-logo.jpg'));
    assert.equal(input.companyInfo, 'Coroinox');
  }
  assert.equal(lead.sampleAgent.images.length, 2);
});
test('validates fields without inventing facts or changing an existing usable logo', () => {
  const lead = { sampleAgent: { brief: { logoChoice: 'provided' }, images: [] } };
  const next = manualSampleContext(lead, { ...fields, withoutLogo: false, logoImageIndex: '' });
  assert.equal(next.logoChoice, 'provided'); assert.equal(next.customers, ''); assert.equal(next.serviceArea, '');
  assert.throws(() => manualSampleContext(lead, { ...fields, logoImageIndex: 100 }), /imagen válida/);
  assert.throws(() => manualSampleContext(lead, { ...fields, offerings: [123] }), /servicios/);
  assert.throws(() => manualSampleContext(lead, { ...fields, companyName: 'x'.repeat(501) }), /datos/);
});
