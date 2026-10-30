import puppeteer from 'puppeteer';

const isStripeResource = value => {
  try {
    const url = new URL(value);
    return url.protocol === 'https:' && ['stripe.com', 'stripecdn.com', 'stripeassets.com'].some(domain => url.hostname === domain || url.hostname.endsWith(`.${domain}`));
  } catch { return false; }
};

export function createOxxoVoucherRenderer({ bucket, launch = options => puppeteer.launch(options) }) {
  return async ({ requestId, paymentIntentId, instructions }) => {
    let url;
    try { url = new URL(instructions.url); } catch { throw Error('La ficha OXXO no tiene un enlace válido.'); }
    if (!/^[a-f0-9]{64}$/.test(requestId) || !/^pi_[a-zA-Z0-9]+$/.test(paymentIntentId)
      || url.protocol !== 'https:' || url.hostname !== 'payments.stripe.com' || url.username || url.password || url.port
      || !url.pathname.startsWith('/oxxo/') || !/^\d{10,24}$/.test(instructions.number)) throw Error('La ficha OXXO no es válida.');
    const file = bucket.file(`payment-vouchers/oxxo/${requestId}/${paymentIntentId}.png`);
    if (!(await file.exists())[0]) {
      const browser = await launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] });
      try {
        const page = await browser.newPage();
        await page.setViewport({ width: 800, height: 1000, deviceScaleFactor: 2 });
        await page.setRequestInterception(true);
        page.on('request', request => {
          if (isStripeResource(request.url()) || /^(?:data:|about:blank$)/.test(request.url())) void request.continue();
          else void request.abort();
        });
        const response = await page.goto(url.href, { waitUntil: 'networkidle2', timeout: 45000 });
        if (!response?.ok()) throw Error('Stripe no permitió abrir la ficha OXXO.');
        // Match the actual reference before capturing; never send an error page or another voucher.
        await page.waitForFunction(number => document.body.innerText.replace(/\s/g, '').includes(number), { timeout: 15000 }, instructions.number);
        await page.evaluate(async () => {
          await document.fonts.ready;
          await Promise.all([...document.images].map(image => image.decode().catch(() => {})));
        });
        const height = await page.evaluate(() => document.documentElement.scrollHeight);
        if (height > 6000) throw Error('La ficha OXXO tiene un tamaño inesperado.');
        const image = Buffer.from(await page.screenshot({ type: 'png', fullPage: true }));
        await file.save(image, { resumable: false, metadata: { contentType: 'image/png', cacheControl: 'private,max-age=3600' } });
      } finally { await browser.close().catch(() => {}); }
    }
    return (await file.getSignedUrl({ action: 'read', expires: new Date(Date.now() + 7 * 86400_000) }))[0];
  };
}
