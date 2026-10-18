import { createHash, createHmac, randomBytes } from 'node:crypto';
import { Webhook } from 'standardwebhooks';

const digest = value => createHash('sha256').update(value).digest('hex');
const uuid = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i;
const fail = (res, status, message) => res.status(status).json({ error: { http_code: status, message } });

// Mounted BEFORE the global JSON parser. Supabase signs the exact raw bytes.
// No OTP, phone, message body or raw provider errors are logged/persisted here.
export function createMasmenuAuthOtpHandler({ db, getSocket, env = process.env, now = Date.now }) {
  return async function authOtp(req, res) {
    res.set('Cache-Control', 'private, no-store');
    const secret = String(env.MASMENU_AUTH_HOOK_SECRET || '').replace(/^v1,whsec_/, '');
    if (env.MASMENU_AUTH_OTP_ENABLED !== 'true' || !/^[A-Za-z0-9+/]{43,86}={0,2}$/.test(secret) || Buffer.from(secret, 'base64').length < 32 || Buffer.from(secret, 'base64').toString('base64') !== secret) return fail(res, 503, 'WhatsApp authentication is not configured.');
    if (!Buffer.isBuffer(req.body) || req.body.length > 20480) return fail(res, 400, 'Invalid hook request.');
    let payload;
    try {
      payload = new Webhook(secret).verify(req.body.toString('utf8'), {
        'webhook-id': req.get('webhook-id') || '',
        'webhook-timestamp': req.get('webhook-timestamp') || '',
        'webhook-signature': req.get('webhook-signature') || '',
      });
    } catch { return fail(res, 401, 'Invalid hook signature.'); }
    const rawPhone = payload?.sms?.phone || payload?.user?.phone || payload?.user?.phone_change;
    const phone = typeof rawPhone === 'string' ? rawPhone.replace(/^\+/, '') : '';
    const otp = payload?.sms?.otp;
    const requestId = req.get('webhook-id') || '';
    if (!uuid.test(payload?.user?.id || '') || !/^52\d{10}$/.test(phone) || typeof otp !== 'string' || !/^\d{6,10}$/.test(otp) || !requestId || requestId.length > 200) return fail(res, 400, 'Invalid authentication payload.');
    const mac = value => createHmac('sha256', secret).update(value).digest('hex');
    const fingerprint = mac(`${phone}:${otp}`);
    const deliveryRef = db.collection('masmenuAuthOtpDeliveries').doc(digest(requestId));
    const limitRef = db.collection('masmenuAuthOtpLimits').doc(mac(phone));
    let reserved = false;
    try {
      const outcome = await db.runTransaction(async transaction => {
        const delivery = await transaction.get(deliveryRef);
        if (delivery.exists) {
          const record = delivery.data();
          if (record.fingerprint !== fingerprint) return 'conflict';
          return record.status === 'sent' ? 'sent' : 'uncertain';
        }
        if (!getSocket()) return 'disconnected';
        const limit = await transaction.get(limitRef);
        const recent = ((limit.exists ? limit.data().attempts : []) || []).filter(at => now() - at < 86400000);
        if (recent.length >= 10 || recent.some(at => now() - at < 60000)) return 'limited';
        transaction.set(deliveryRef, { fingerprint, status: 'sending', attemptedAt: now(), expiresAt: new Date(now() + 86400000) });
        transaction.set(limitRef, { attempts: [...recent, now()], expiresAt: new Date(now() + 86400000) });
        return 'reserved';
      });
      if (outcome === 'sent') return res.json({});
      if (outcome === 'limited') return fail(res, 429, 'Wait before requesting another code.');
      if (outcome !== 'reserved') return fail(res, 503, 'WhatsApp authentication is unavailable.');
      reserved = true;
      const socket = getSocket();
      if (!socket) throw new Error('DISCONNECTED');
      const text = `Tu código de acceso a +Menú es: ${otp}\n\nEs temporal y de un solo uso. Escríbelo en +Menú y no lo compartas. Nunca te pediremos tu PIN por WhatsApp.`;
      let timer;
      try {
        // Reuse the primary CRM socket without persisting sensitive OTPs in leads,
        // conversation histories or campaign/automation paths.
        const sent = await Promise.race([
          socket.sendMessage(`${phone}@s.whatsapp.net`, { text, linkPreview: false }, { timeoutMs: 2500, messageId: `MASMENUAUTH_${randomBytes(16).toString('hex').toUpperCase()}` }),
          new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('TIMEOUT')), 2500); }),
        ]);
        if (!sent?.key?.id) throw new Error('SEND_NOT_CONFIRMED');
      } finally { clearTimeout(timer); }
      await deliveryRef.set({ status: 'sent', sentAt: now() }, { merge: true });
      return res.json({});
    } catch {
      if (reserved) await deliveryRef.set({ status: 'unknown' }, { merge: true }).catch(() => {});
      // A timeout may follow acceptance by WhatsApp: do not resend automatically.
      return fail(res, 503, 'WhatsApp authentication could not be confirmed.');
    }
  };
}
