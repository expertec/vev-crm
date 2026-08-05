import test from 'node:test';
import assert from 'node:assert/strict';
import { extractEmails, ProspectingService, scoreProspect } from '../services/prospectingService.js';

test('extractEmails returns unique public emails and ignores asset-like matches', () => {
  const html = `
    <a href="mailto:Ventas@Cliente.com">Ventas</a>
    <span>ventas@cliente.com</span>
    <span>info@cliente.mx</span>
    <img src="/assets/banner@2x.png">
  `;

  assert.deepEqual(extractEmails(html), ['ventas@cliente.com', 'info@cliente.mx']);
});

test('scoreProspect prioritizes businesses without website and visible Google activity', () => {
  const noWebsite = scoreProspect({
    website: '',
    emails: [],
    rating: 4.5,
    userRatingCount: 80,
  });
  const withWebsiteAndEmail = scoreProspect({
    website: 'https://cliente.com',
    emails: ['ventas@cliente.com'],
    rating: 4.8,
    userRatingCount: 80,
  });

  assert.equal(noWebsite.label, 'Alta');
  assert.ok(noWebsite.score > withWebsiteAndEmail.score);
  assert.ok(noWebsite.reasons.includes('Sin sitio web detectado'));
});

test('recommendZones and classifyProspects work without OpenAI key', async () => {
  const service = new ProspectingService({
    apiKey: 'google-test-key',
    openAiApiKey: '',
  });

  const zones = await service.recommendZones({
    area: 'Monterrey',
    businessType: 'dentistas',
  });
  assert.equal(zones.source, 'rules');
  assert.ok(zones.items.length >= 4);
  assert.match(zones.items[0].searchArea, /Monterrey/);

  const classified = await service.classifyProspects({
    area: 'Monterrey',
    businessType: 'dentistas',
    items: [
      {
        placeId: 'place-1',
        name: 'Dental Norte',
        phone: '+52 81 1234 5678',
        phoneDigits: '528112345678',
        website: '',
        emails: [],
        socialLinks: {},
        rating: 4.4,
        userRatingCount: 60,
        opportunity: { score: 88, label: 'Alta', reasons: [] },
      },
    ],
  });

  assert.equal(classified.source, 'rules');
  assert.equal(classified.items[0].fit.label, 'Prioritario');
  assert.ok(classified.items[0].fit.pitchAngle.includes('pagina web'));
});
