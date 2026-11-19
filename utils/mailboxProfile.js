export const MAX_PROFILE_PHOTO_BYTES = 512 * 1024;

export function cleanDisplayName(value) {
  return String(value ?? '').replace(/[\r\n\x00-\x1f\x7f]/g, ' ').trim().slice(0, 120);
}

export function escapeProfileHtml(value) {
  return String(value ?? '').replace(/[&<>"']/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
}

export function withMailboxPhoto({ html, text, profile }) {
  if (!profile?.photoUrl) return html;
  const body = html || `<div style="white-space:pre-wrap">${escapeProfileHtml(text)}</div>`;
  const footer = `<div data-mailbox-profile style="margin-top:20px;padding-top:12px;border-top:1px solid #e2e8f0;font-family:Arial,sans-serif"><table role="presentation" cellpadding="0" cellspacing="0"><tr><td style="padding-right:12px"><img src="${escapeProfileHtml(profile.photoUrl)}" alt="${escapeProfileHtml(profile.displayName || 'Foto de perfil')}" width="56" height="56" style="display:block;width:56px;height:56px;border-radius:50%;object-fit:cover"></td><td style="font-size:14px;color:#334155">${escapeProfileHtml(profile.displayName)}<br>${escapeProfileHtml(profile.email)}</td></tr></table></div>`;
  return /<\/body\s*>/i.test(body) ? body.replace(/<\/body\s*>/i, () => `${footer}</body>`) : body + footer;
}
