const escape = (value) => String(value || '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
export const EXTRA_GALLERY_LIMIT = 20;
export const EXTRA_GALLERY_CSS = `
#nw-extra-gallery{padding:48px max(20px,calc((100% - 1120px)/2));background:#f5f7fa;color:#17202a;isolation:isolate}
#nw-extra-gallery *{box-sizing:border-box;min-width:0}
#nw-extra-gallery h2{font:700 clamp(24px,4vw,36px)/1.2 system-ui;margin:0 0 8px}
#nw-extra-gallery>p{font:15px/1.5 system-ui;color:#526174;margin:0 0 20px}
#nw-extra-gallery .nw-extra-stage{background:#101820;border-radius:18px;overflow:hidden}
#nw-extra-gallery .nw-extra-slide{display:none;margin:0;scroll-margin-top:12px}
#nw-extra-gallery .nw-extra-slide:first-child{display:block}
#nw-extra-gallery .nw-extra-stage:has(.nw-extra-slide:target) .nw-extra-slide{display:none}
#nw-extra-gallery .nw-extra-stage:has(.nw-extra-slide:target) .nw-extra-slide:target{display:block}
#nw-extra-gallery .nw-extra-photo{display:block;width:100%;height:clamp(240px,52vw,480px);object-fit:contain;margin:0}
#nw-extra-gallery .nw-extra-caption{display:flex;align-items:center;justify-content:space-between;gap:14px;padding:14px 16px;color:#fff;font:14px/1.5 system-ui}
#nw-extra-gallery .nw-extra-caption span{flex:1;overflow-wrap:anywhere}
#nw-extra-gallery .nw-extra-caption a{display:inline-flex;align-items:center;justify-content:center;min-width:44px;min-height:44px;padding:8px 12px;background:#ffffff18;border:1px solid #ffffff40;border-radius:10px;color:#fff;text-decoration:none;font:600 14px system-ui;flex-shrink:0}
#nw-extra-gallery .nw-extra-thumbs{display:flex;gap:10px;overflow-x:auto;padding:14px 2px 8px;scroll-snap-type:x proximity}
#nw-extra-gallery .nw-extra-thumbs a{flex:0 0 82px;scroll-snap-align:start;border-radius:10px;overflow:hidden;border:2px solid transparent;display:block}
#nw-extra-gallery .nw-extra-thumbs img{width:78px;height:64px;object-fit:cover;display:block}
#nw-extra-gallery a:focus-visible{outline:3px solid #1685ef;outline-offset:2px}
@media(max-width:600px){#nw-extra-gallery{padding:32px 16px}#nw-extra-gallery .nw-extra-caption{flex-wrap:wrap;gap:8px}#nw-extra-gallery .nw-extra-caption span{flex-basis:100%}}
@media(prefers-reduced-motion:reduce){#nw-extra-gallery{scroll-behavior:auto}}
`;

export function appendExtraGallery(html, gallery) {
  if (!gallery?.items?.length) return html;
  const items = gallery.items.slice(0, EXTRA_GALLERY_LIMIT);
  const id = (i) => `nw-extra-photo-${i + 1}`;
  const figures = items.map((photo, i) => `<figure id="${id(i)}" class="nw-extra-slide"><img class="nw-extra-photo" src="${escape(photo.url)}" alt="${escape(photo.caption || `Foto ${i + 1} de la galería`)}" loading="lazy" decoding="async" width="1200" height="800"><figcaption class="nw-extra-caption"><span>${photo.caption ? `${escape(photo.caption)} · ` : ''}${i + 1} / ${items.length}</span><a href="#${id((i + items.length - 1) % items.length)}" aria-label="Foto anterior">←</a><a href="#${id((i + 1) % items.length)}" aria-label="Foto siguiente">→</a><a href="${escape(photo.url)}" target="_blank" rel="noopener noreferrer">Ampliar ↗</a></figcaption></figure>`).join('');
  const thumbs = items.map((photo, i) => `<a href="#${id(i)}" aria-label="Ver foto ${i + 1}${photo.caption ? `: ${escape(photo.caption)}` : ''}"><img src="${escape(photo.thumbnailUrl || photo.url)}" alt="" width="78" height="64" loading="lazy" decoding="async"></a>`).join('');
  const section = `<section id="nw-extra-gallery" aria-label="Galería de fotos"><h2>${escape(gallery.title || 'Más de nuestro trabajo')}</h2><p>Explora las fotos y selecciona una miniatura para verla con más detalle.</p><div class="nw-extra-stage">${figures}</div><nav class="nw-extra-thumbs" aria-label="Seleccionar foto">${thumbs}</nav></section>`;
  const closing = html.lastIndexOf('</main>');
  return closing >= 0 ? html.slice(0, closing) + section + html.slice(closing) : html + section;
}
