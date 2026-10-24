import { parseDocument } from 'htmlparser2';
import render from 'dom-serializer';

// Videos are rendered by the application, never by model-authored HTML.
export function appendSampleVideos(page, videos = []) {
  if (!videos.length) return page;
  const escape = (value) => String(value).replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
  const section = `<section id="videos" class="nw-videos" aria-labelledby="nw-videos-title"><div class="nw-videos-inner"><span class="nw-videos-eyebrow">EN VIDEO</span><h2 id="nw-videos-title">Conoce más de nuestro negocio.</h2><p class="nw-videos-intro">Dale play y descubre lo que compartimos contigo.</p><div class="nw-video-grid">${videos.map((url, index) => `<figure class="nw-video-card"><div class="nw-video-screen"><video controls playsinline preload="metadata" aria-label="Video del negocio ${index + 1}" src="${escape(url)}">Tu navegador no puede reproducir este video.</video></div><figcaption><span class="nw-video-play" aria-hidden="true">▶</span><div><strong>Video ${String(index + 1).padStart(2, '0')}</strong><span>Compartido por el negocio</span></div><span class="nw-video-tag">VIDEO</span></figcaption></figure>`).join('')}</div></div></section>`;
  // Keep all media together, ahead of the process/FAQ and final contact sections.
  const before = /<section\b[^>]*\bid="(?:proceso|preguntas|contacto)"[^>]*>/i;
  const html = before.test(page.html) ? page.html.replace(before, (tag) => section + tag) : page.html.replace(/<\/main>/i, section + '</main>');
  return { ...page, html, css: `${page.css}\n${SAMPLE_VIDEO_CSS}` };
}

export const SAMPLE_VIDEO_CSS = `
.nw-videos{padding:64px 24px!important;background:#0b1524;color:#fff}
.nw-videos-inner{max-width:1120px;margin:0 auto}
.nw-videos-eyebrow{font-size:11px;font-weight:800;letter-spacing:.16em;color:var(--nw-hero-accent,#fff)}
.nw-videos h2{font-size:clamp(1.8rem,4vw,2.8rem);line-height:1.1;margin:12px 0}
.nw-videos-intro{color:#cbd5e1;font-size:15px;line-height:1.6;margin:0 0 28px}
.nw-video-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,300px),1fr));gap:24px}
.nw-video-card{margin:0;overflow:hidden;border-radius:20px;border:1px solid #ffffff24;background:#162437;box-shadow:0 14px 32px #0003}
.nw-video-screen{position:relative;background:#050b13;aspect-ratio:16/10;display:block}
.nw-video-screen video{position:absolute;inset:0;display:block;width:100%;height:100%;max-height:520px;object-fit:contain;color-scheme:dark}
.nw-video-card figcaption{display:flex;align-items:center;gap:12px;padding:18px}
.nw-video-play{display:grid;place-items:center;width:40px;height:40px;flex-shrink:0;border-radius:50%;background:var(--nw-accent,#fff);color:var(--nw-on-accent,#111);font-size:15px;box-shadow:0 4px 14px #0003}
.nw-video-card figcaption strong{display:block;font-size:15px;line-height:1.4;color:#fff}
.nw-video-card figcaption div>span{display:block;font-size:12px;margin-top:3px;color:#cbd5e1}
.nw-video-tag{margin-left:auto;font-size:9px;font-weight:700;letter-spacing:.12em;color:#cbd5e1;border:1px solid #ffffff30;border-radius:20px;padding:5px 8px}
.nw-video-open{display:inline-flex;align-items:center;justify-content:center;min-height:44px;white-space:nowrap;text-decoration:none;flex-shrink:0}
@media(max-width:600px){.nw-videos{padding:40px 20px!important}.nw-video-grid{gap:18px}.nw-video-card figcaption{padding:14px}}
`;

export function sampleVideoPlaybackHtml(html, videos = []) {
  if (!videos.length || !/<video\b/i.test(html)) return html;
  const allowed = new Set(videos), root = parseDocument(html);
  const walk = (node, fn) => { fn(node); (node.children || []).forEach((child) => walk(child, fn)); };
  walk(root, (node) => {
    if (node.name !== 'figure' || !String(node.attribs?.class || '').split(/\s+/).includes('nw-video-card')) return;
    let video, tag, linked = false;
    walk(node, (child) => {
      if (child.name === 'video' && allowed.has(child.attribs?.src)) video = child;
      if (String(child.attribs?.class || '').split(/\s+/).includes('nw-video-tag')) tag = child;
      if (child.name === 'a' && allowed.has(child.attribs?.href)) linked = true;
    });
    if (!video) return;
    video.attribs.controls = ''; video.attribs.playsinline = ''; video.attribs['webkit-playsinline'] = '';
    video.attribs.preload = 'metadata';
    if (tag && !linked) {
      tag.name = 'a'; tag.attribs = { ...tag.attribs, class: `${tag.attribs.class} nw-video-open`, href: video.attribs.src, target: '_blank', rel: 'noopener noreferrer', 'aria-label': 'Abrir video en el reproductor del dispositivo' };
      const text = parseDocument('Ver video ↗').children[0]; text.parent = tag; tag.children = [text];
    }
  });
  return render(root, { encodeEntities: 'utf8' });
}
