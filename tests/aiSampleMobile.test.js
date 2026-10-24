import test from 'node:test';
import assert from 'node:assert/strict';
import postcss from 'postcss';
import { parseDocument } from 'htmlparser2';
import { appendSampleVideos, sampleVideoPlaybackHtml } from '../services/aiSampleVideos.js';
import { samplePageDocument } from '../services/aiSampleGenerator.js';
import { SAMPLE_SERVICE_RESPONSIVE_CSS, SAMPLE_BRAND_HEADER_CSS } from '../services/aiSampleVisuals.js';

const url = 'https://storage.googleapis.com/test/video.mp4?key=a&signature=b';
const page = { title: 'Negocio', description: '', html: '<main><section id="servicios"><div class="nw-service-grid nw-featured-services"><article class="nw-service-card"><img src="https://storage.googleapis.com/test/photo.jpg"><div class="nw-featured-copy"><span class="nw-service-icon">Icono</span><div><h3>Mesas de acero inoxidable</h3><p>Consulta fabricación.</p></div></div></article></div></section><section id="proceso"></section></main>', css: '.nw-service-grid{grid-template-columns:1fr 1fr}.nw-featured-copy{display:flex}', assets: [] };

test('all saved video cards gain inline controls and a direct device-player link, without scripts', () => {
  const saved = appendSampleVideos(page, [url]);
  const html = sampleVideoPlaybackHtml(saved.html, [url]);
  const root = parseDocument(html), videos = [], links = [];
  const walk = (node) => { if (node.name === 'video') videos.push(node); if (node.name === 'a') links.push(node); (node.children || []).forEach(walk); }; walk(root);
  assert.equal(videos.length, 1);
  assert.equal(videos[0].attribs.src, url);
  assert.ok(Object.hasOwn(videos[0].attribs, 'playsinline'));
  assert.ok(Object.hasOwn(videos[0].attribs, 'webkit-playsinline'));
  assert.equal(links[0].attribs.href, url);
  assert.equal(links[0].attribs.target, '_blank');
  assert.match(links[0].attribs.rel, /noopener/);
  assert.equal(sampleVideoPlaybackHtml(html, [url]), html);
  assert.doesNotMatch(html, /<script|autoplay|onplay/);
  assert.equal(sampleVideoPlaybackHtml(saved.html, []), saved.html);
});

test('existing public pages and editor previews receive the mobile correction after their stored CSS', () => {
  const saved = { ...appendSampleVideos(page, [url]), videos: [url] };
  const document = samplePageDocument(saved);
  assert.ok(document.indexOf(SAMPLE_SERVICE_RESPONSIVE_CSS) > document.indexOf(page.css));
  assert.ok(document.includes('grid-template-columns:minmax(0,1fr)!important'));
  assert.ok(document.includes('min-width:0;flex:1'));
  assert.match(document, /script-src &#39;none&#39;/);
  assert.doesNotMatch(document, /allow-same-origin/);
  postcss.parse(SAMPLE_SERVICE_RESPONSIVE_CSS + SAMPLE_BRAND_HEADER_CSS);
});
