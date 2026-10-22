import puppeteer from 'puppeteer';
import { samplePageDocument } from './aiSampleGenerator.js';

export async function validateAiSampleLayout(schema, { launch = (options) => puppeteer.launch(options) } = {}) {
  const pageData = schema.aiPage;
  const allowedUrls = new Set([schema.brand?.logo, ...pageData.assets.map((asset) => asset.url)].filter(Boolean));
  const browser = await launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage'] });
  try {
    const page = await browser.newPage();
    await page.setJavaScriptEnabled(false);
    await page.setRequestInterception(true);
    page.on('request', (request) => {
      const trustedHost = /^(?:storage\.googleapis\.com|firebasestorage\.googleapis\.com|images\.unsplash\.com)$/.test(new URL(request.url()).hostname);
      if (request.resourceType() === 'image' && trustedHost && allowedUrls.has(request.url())) void request.continue();
      else void request.abort();
    });
    await page.setViewport({ width: 390, height: 844, deviceScaleFactor: 1 });
    await page.setContent(samplePageDocument(pageData, schema.brand?.logo), { waitUntil: 'networkidle0', timeout: 45_000 });
    const checks = [];
    for (const width of [390, 1280]) {
      await page.setViewport({ width, height: 900, deviceScaleFactor: 1 });
      const result = await page.evaluate(() => ({
        width: window.innerWidth,
        overflow: document.documentElement.scrollWidth > window.innerWidth + 2,
        brokenImages: [...document.images].filter((image) => !image.complete || !image.naturalWidth).length,
        hasHeading: Boolean(document.querySelector('main h1')),
      }));
      if (result.overflow || result.brokenImages || !result.hasHeading) throw new Error(`La muestra necesita ajustes de diseño (${width}px, desbordamiento: ${result.overflow}, imágenes fallidas: ${result.brokenImages}).`);
      checks.push(result);
    }
    await page.setViewport({ width: 390, height: 844, deviceScaleFactor: 1 });
    const screenshot = Buffer.from(await page.screenshot({ type: 'webp', quality: 80, fullPage: false }));
    return { checks, screenshot };
  } finally { await browser.close(); }
}
