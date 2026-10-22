// Trusted inline icons. Model-authored SVG stays forbidden by the page sanitizer.
const paths = {
  whatsapp: '<path d="M21 11.5a8.4 8.4 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.4 8.4 0 0 1-3.8-.9L3 21l1.9-5.7a8.4 8.4 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.4 8.4 0 0 1 3.8-.9h.5a8.5 8.5 0 0 1 8 8v.5Z"/><path d="m9 8 1 2-1 1c1 2 2 3 4 4l1-1 2 1c0 2-2 2-3 1-3-1-5-3-6-6 0-1 0-2 2-2Z"/>',
  phone: '<path d="m8 3 2 5-2 2a14 14 0 0 0 6 6l2-2 5 2v3a2 2 0 0 1-2 2C10 21 3 14 3 5a2 2 0 0 1 2-2h3Z"/>',
  mail: '<rect x="3" y="5" width="18" height="14" rx="2"/><path d="m3 6 9 7 9-7"/>',
  pin: '<path d="M20 10c0 6-8 12-8 12S4 16 4 10a8 8 0 1 1 16 0Z"/><circle cx="12" cy="10" r="3"/>',
  arrow: '<path d="M4 12h16m-6-6 6 6-6 6"/>',
  check: '<path d="m5 12 4 4L19 6"/>',
  home: '<path d="m3 10 9-7 9 7v11h-6v-8H9v8H3V10Z"/>',
  shield: '<path d="m12 3 8 3v6c0 5-8 10-8 10S4 17 4 12V6l8-3Z"/><path d="m8 12 3 3 5-6"/>',
  clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
  sparkles: '<path d="m12 3 2.5 6.5L21 12l-6.5 2.5L12 21l-2.5-6.5L3 12l6.5-2.5L12 3Zm7 0v4m-2-2h4"/>',
  sofa: '<path d="M5 12V7a3 3 0 0 1 3-3h8a3 3 0 0 1 3 3v5M5 20v-3m14 3v-3"/><path d="M5 10a2 2 0 0 1 2 2v2h10v-2a2 2 0 0 1 4 0v6H3v-6a2 2 0 0 1 2-2Z"/>',
  tools: '<path d="m14 6 4 4M6 14l4 4M3 21l7-7m4-4 7-7M15 3l6 6M3 3l18 18M3 3h5l1 4-2 2-4-1V3Z"/>',
  water: '<path d="M12 3c-2 4-7 8-7 12a7 7 0 0 0 14 0c0-4-5-8-7-12Z"/><path d="M8 15a4 4 0 0 0 4 4"/>',
  camera: '<path d="m8 5 2-2h4l2 2h5v15H3V5h5Z"/><circle cx="12" cy="12" r="4"/>',
  calendar: '<rect x="3" y="5" width="18" height="16" rx="2"/><path d="M7 3v4m10-4v4M3 11h18m-14 4h2m6 0h2"/>',
  star: '<path d="m12 3 2.8 5.7 6.2.9-4.5 4.4 1.1 6.2-5.6-3-5.6 3 1.1-6.2L3 9.6l6.2-.9L12 3Z"/>',
};

export function sampleIconPlaceholder(key) {
  const name = String(key).replace(/^ICON_/, '').toLowerCase();
  return Object.hasOwn(paths, name) ? `<span class="nw-icon nw-icon-${name}" aria-hidden="true"></span>` : '';
}

export function renderSampleIcons(html) {
  return html.replace(/<span class="nw-icon nw-icon-([a-z]+)" aria-hidden="true"><\/span>/g, (match, name) => Object.hasOwn(paths, name)
    ? `<span class="nw-icon nw-icon-${name}" aria-hidden="true"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" focusable="false">${paths[name]}</svg></span>`
    : match);
}
