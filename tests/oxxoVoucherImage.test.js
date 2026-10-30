import test from 'node:test';
import assert from 'node:assert/strict';
import { createOxxoVoucherRenderer } from '../services/oxxoVoucherImage.js';

const input = { requestId: 'a'.repeat(64), paymentIntentId: 'pi_test123', instructions: { url: 'https://payments.stripe.com/oxxo/voucher/test', number: '9101552020250956' } };
function harness({ exists = false, mismatch = false } = {}) {
  const calls = [], file = { exists: async () => [exists], save: async (data, options) => calls.push(['save', data, options]), getSignedUrl: async options => { calls.push(['signed', options]); return ['https://storage.googleapis.com/private/ficha.png']; } };
  const page = {
    setViewport: async options => calls.push(['viewport', options]), setRequestInterception: async () => {}, on: () => {},
    goto: async (url, options) => { calls.push(['goto', url, options]); return { ok: () => true }; },
    waitForFunction: async (_fn, _options, number) => { calls.push(['reference', number]); if (mismatch) throw Error('La referencia no coincide'); },
    evaluate: async () => 1600, screenshot: async options => { calls.push(['capture', options]); return Buffer.from('PNG fixture'); },
  };
  const render = createOxxoVoucherRenderer({ bucket: { file: path => { calls.push(['path', path]); return file; } }, launch: async () => { calls.push(['launch']); return { newPage: async () => page, close: async () => calls.push(['close']) }; } });
  return { calls, render };
}

test('captura la ficha Stripe comprobando la referencia y guarda PNG antes de compartirla', async () => {
  const h = harness(); const url = await h.render(input);
  assert.equal(url, 'https://storage.googleapis.com/private/ficha.png');
  assert.deepEqual(h.calls.find(call => call[0] === 'reference'), ['reference', input.instructions.number]);
  assert.deepEqual(h.calls.find(call => call[0] === 'capture')[1], { type: 'png', fullPage: true });
  assert.equal(h.calls.find(call => call[0] === 'viewport')[1].deviceScaleFactor, 2);
  assert.equal(h.calls.find(call => call[0] === 'save')[2].metadata.contentType, 'image/png');
  assert.ok(h.calls.find(call => call[0] === 'close'));
});

test('reutiliza la imagen guardada sin abrir otro navegador', async () => {
  const h = harness({ exists: true }); await h.render(input);
  assert.equal(h.calls.some(call => call[0] === 'launch'), false);
});

test('bloquea URLs ajenas a la ficha Stripe y no captura una referencia distinta', async () => {
  for (const url of ['http://payments.stripe.com/oxxo/voucher/test', 'https://payments.stripe.com.attacker.com/oxxo/test', 'https://localhost/oxxo/test', 'https://payments.stripe.com/other']) {
    const h = harness(); await assert.rejects(h.render({ ...input, instructions: { ...input.instructions, url } }), /no es válida/);
    assert.equal(h.calls.length, 0);
  }
  const h = harness({ mismatch: true }); await assert.rejects(h.render(input), /no coincide/);
  assert.equal(h.calls.some(call => call[0] === 'save'), false);
  assert.ok(h.calls.find(call => call[0] === 'close'));
});
