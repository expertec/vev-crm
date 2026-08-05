import test from 'node:test';
import assert from 'node:assert/strict';
import { extractEmails, scoreProspect } from '../services/prospectingService.js';

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
