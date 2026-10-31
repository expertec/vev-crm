import puppeteer from 'puppeteer';

const isStripeResource = value => {
  try {
    const url = new URL(value);
    return url.protocol === 'https:' && ['stripe.com', 'stripecdn.com', 'stripeassets.com'].some(domain => url.hostname === domain || url.hostname.endsWith(`.${domain}`));
  } catch { return false; }
};

export function createOxxoVoucherRenderer({ bucket, launch = options => puppeteer.launch(options), logger = console }) {
  let capturing = false;
  return async ({ requestId, paymentIntentId, instructions }) => {
    let url;
    try { url = new URL(instructions.url); } catch { throw Error('La ficha OXXO no tiene un enlace válido.'); }
    if (!/^[a-f0-9]{64}$/.test(requestId) || !/^pi_[a-zA-Z0-9]+$/.test(paymentIntentId)
      || url.protocol !== 'https:' || url.hostname !== 'payments.stripe.com' || url.username || url.password || url.port
      || !/^\d{10,24}$/.test(String(instructions.number || '').replace(/\s/g, ''))) throw Error('La ficha OXXO no es válida.');
    const number = String(instructions.number).replace(/\s/g, '');
    const file = bucket.file(`payment-vouchers/oxxo/${requestId}/${paymentIntentId}.png`);
    if (!(await file.exists())[0]) {
      if (capturing) throw Object.assign(Error('Se está preparando otra ficha OXXO. Intenta de nuevo en unos segundos.'), { status: 503 });
      capturing = true;
      let browser, stage = 'browser';
      try {
        browser = await launch({ headless: true, timeout: 15000, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--no-zygote', '--renderer-process-limit=1'] });
        const page = await browser.newPage();
        await page.setViewport({ width: 800, height: 1000, deviceScaleFactor: 2 });
        await page.setRequestInterception(true);
        page.on('request', request => {
          if (isStripeResource(request.url()) || /^(?:data:|about:blank$)/.test(request.url())) void request.continue();
          else void request.abort();
        });
        stage = 'navigation';
        const response = await page.goto(url.href, { waitUntil: 'domcontentloaded', timeout: 25000 });
        if (!response?.ok()) throw Error('Stripe no permitió abrir la ficha OXXO.');
        // Match the actual reference before capturing; never send an error page or another voucher.
        stage = 'reference';
        await page.waitForFunction(number => document.body.innerText.replace(/\s/g, '').includes(number), { timeout: 10000 }, number);
        await page.evaluate(async () => {
          await Promise.race([Promise.all([document.fonts.ready, ...[...document.images].map(image => image.decode().catch(() => {}))]), new Promise(resolve => setTimeout(resolve, 3000))]);
        });
        const height = await page.evaluate(() => document.documentElement.scrollHeight);
        if (height > 6000) throw Error('La ficha OXXO tiene un tamaño inesperado.');
        stage = 'screenshot';
        const image = Buffer.from(await page.screenshot({ type: 'png', fullPage: true, timeout: 10000 }));
        stage = 'storage';
        await file.save(image, { resumable: false, metadata: { contentType: 'image/png', cacheControl: 'private,max-age=3600' } });
      } catch (cause) {
        logger.warn('[oxxo-voucher]', { requestId, paymentIntentId, stage, error: String(cause.message || cause).replace(/https?:\/\/[^\s]+/g, '[url]').slice(0, 500) });
        const messages = {
          browser: 'No se pudo iniciar el navegador para generar la imagen OXXO. Reinstala las dependencias del servidor y vuelve a intentar.',
          navigation: 'No se pudo abrir la ficha original de Stripe. Puedes abrir su enlace y volver a intentar.',
          reference: 'La ficha no mostró la referencia OXXO esperada. No se envió ninguna imagen.',
          screenshot: 'No se pudo capturar la imagen OXXO. Vuelve a intentar.',
          storage: 'No se pudo guardar la imagen OXXO. Vuelve a intentar.',
        };
        throw Object.assign(Error(messages[stage]), { status: 503, cause });
      } finally {
        if (browser) await browser.close().catch(() => {});
        capturing = false;
      }
    }
    return (await file.getSignedUrl({ action: 'read', expires: new Date(Date.now() + 7 * 86400_000) }))[0];
  };
}
