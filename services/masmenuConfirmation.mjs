import { createHash, timingSafeEqual } from 'node:crypto';

const hash = value => createHash('sha256').update(value).digest('hex');
const slugPattern = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
const uuidPattern = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i;

function publicOrigin(value) {
  try {
    const url = new URL(value);
    if (url.protocol !== 'https:' || url.username || url.password || !url.hostname.includes('.')
      || /(^|\.)(localhost|local|internal|test|invalid)$/.test(url.hostname)
      || /^[\d.]+$/.test(url.hostname) || url.hostname.startsWith('[')) return null;
    return url.origin;
  } catch { return null; }
}

function normalizeInput(body, appOrigin) {
  if (!body || typeof body !== 'object') return null;
  const { menuId, slug, restaurantName, whatsapp, menuUrl } = body;
  if (typeof menuId !== 'string' || !uuidPattern.test(menuId)
    || typeof slug !== 'string' || slug.length > 96 || !slugPattern.test(slug)
    || typeof restaurantName !== 'string' || !restaurantName.trim()
    || typeof whatsapp !== 'string' || !/^\+52\d{10}$/.test(whatsapp)
    || typeof menuUrl !== 'string' || menuUrl !== `${appOrigin}/menu/${slug}`) return null;
  return {
    menuId, slug, whatsapp, menuUrl,
    restaurantName: restaurantName.replace(/\s+/g, ' ').trim().slice(0, 150),
  };
}

// Dependencies are injected so tests never boot Firebase, Baileys or real sessions.
export function createMasmenuConfirmationHandler({ db, sendText, isConnected, env = process.env, now = Date.now }) {
  return async function confirmMenu(req, res) {
    res.set('Cache-Control', 'private, no-store');
    const apiKey = String(env.MASMENU_CONFIRMATION_API_KEY || '').trim();
    const appOrigin = publicOrigin(String(env.MASMENU_PUBLIC_APP_URL || '').trim());
    if (apiKey.length < 32 || !appOrigin) {
      return res.status(503).json({ ok: false, code: 'CONFIRMATION_NOT_CONFIGURED' });
    }
    const authorization = String(req.get('authorization') || '');
    const token = authorization.match(/^Bearer\s+(\S+)$/i)?.[1] || '';
    if (!token || !timingSafeEqual(Buffer.from(hash(token)), Buffer.from(hash(apiKey)))) {
      return res.status(401).json({ ok: false, code: 'UNAUTHORIZED' });
    }
    const input = normalizeInput(req.body, appOrigin);
    if (!input) return res.status(400).json({ ok: false, code: 'INVALID_CONFIRMATION' });

    // One record per menu/recipient/canonical URL, shared by every Render instance.
    const deliveryId = hash(JSON.stringify([input.menuId, input.whatsapp, input.menuUrl]));
    const deliveryRef = db.collection('masmenuConfirmations').doc(deliveryId);
    const limitRef = db.collection('masmenuConfirmationLimits').doc(`menu_${hash(input.menuId)}`);
    const phoneLimitRef = db.collection('masmenuConfirmationLimits').doc(`phone_${hash(input.whatsapp)}`);
    let reserved = false;
    try {
      const reservation = await db.runTransaction(async transaction => {
        const delivery = await transaction.get(deliveryRef);
        if (delivery.exists) {
          const record = delivery.data();
          if (record.status === 'sent') return { outcome: 'already_sent' };
          return { outcome: record.status === 'sending' && now() - record.attemptedAt < 120000 ? 'processing' : 'unknown' };
        }
        if (!isConnected()) return { outcome: 'disconnected' };
        const limit = await transaction.get(limitRef);
        const phoneLimit = await transaction.get(phoneLimitRef);
        const attempts = (limit.exists ? limit.data().attempts : []) || [];
        const recent = attempts.filter(at => now() - at < 86400000);
        const phoneAttempts = (phoneLimit.exists ? phoneLimit.data().attempts : []) || [];
        const recentPhone = phoneAttempts.filter(at => now() - at < 86400000);
        if (recent.length >= 3 || recentPhone.length >= 5
          || recent.some(at => now() - at < 60000)
          || recentPhone.some(at => now() - at < 60000)) return { outcome: 'rate_limited' };
        const attemptedAt = now();
        transaction.set(deliveryRef, {
          status: 'sending', attemptedAt,
          menuId: input.menuId, slug: input.slug, menuUrl: input.menuUrl,
          phoneHash: hash(input.whatsapp),
        });
        transaction.set(limitRef, { attempts: [...recent, attemptedAt] });
        transaction.set(phoneLimitRef, { attempts: [...recentPhone, attemptedAt] });
        return { outcome: 'reserved' };
      });

      if (reservation.outcome === 'already_sent') return res.json({ ok: true, status: 'already_sent' });
      if (reservation.outcome === 'processing') return res.status(202).json({ ok: false, code: 'CONFIRMATION_PROCESSING' });
      if (reservation.outcome === 'unknown') return res.status(409).json({ ok: false, code: 'CONFIRMATION_UNCERTAIN' });
      if (reservation.outcome === 'disconnected') return res.status(409).json({ ok: false, code: 'SESSION_NOT_CONNECTED' });
      if (reservation.outcome === 'rate_limited') return res.status(429).json({ ok: false, code: 'RATE_LIMITED' });
      reserved = true;
      const message = [
        `¡Tu menú de ${input.restaurantName} ya está listo!`, '',
        'Aquí tienes tu enlace:', input.menuUrl, '',
        'Guárdalo y compártelo con tus clientes.', '', '+Menú · Negocios Web',
      ].join('\n');
      const result = await sendText(input.whatsapp.replace('+', ''), message);
      if (result?.success !== true) throw new Error('SEND_NOT_CONFIRMED');
      await deliveryRef.set({ status: 'sent', sentAt: now() }, { merge: true });
      return res.json({ ok: true, status: 'sent' });
    } catch {
      if (reserved) {
        // A socket/DB error may occur after WhatsApp accepted the message.
        // Keep the reservation; retrying must not send a possible duplicate.
        await deliveryRef.set({ status: 'unknown' }, { merge: true }).catch(() => {});
        return res.status(409).json({ ok: false, code: 'CONFIRMATION_UNCERTAIN' });
      }
      return res.status(503).json({ ok: false, code: 'CONFIRMATION_UNAVAILABLE' });
    }
  };
}
