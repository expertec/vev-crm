export const MAX_PROFILE_PHOTO_BYTES = 512 * 1024;

export function cleanDisplayName(value) {
  return String(value ?? '').replace(/[\r\n\x00-\x1f\x7f]/g, ' ').trim().slice(0, 120);
}
