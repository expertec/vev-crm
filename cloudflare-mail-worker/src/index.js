// Cloudflare Email Worker — captura el correo entrante de los alias con buzón.
// Flujo: parsea el MIME → lo manda al Core API (/mailbox/ingest) → reenvía copia.
//
// Requiere secret: INGEST_SECRET (= MAILBOX_INGEST_SECRET del Core API).
// La regla de Email Routing del alias (ej. ventas@dominio.com) debe apuntar a
// este Worker (acción "Send to a Worker").
import PostalMime from 'postal-mime';

export default {
  async email(message, env) {
    let parsed = {};
    try {
      parsed = await PostalMime.parse(message.raw);
    } catch (err) {
      parsed = {};
    }

    const payload = {
      to: message.to,
      from: message.from,
      subject: parsed.subject || '',
      text: parsed.text || '',
      html: parsed.html || '',
      messageId: parsed.messageId || '',
      inReplyTo: parsed.inReplyTo || '',
      date: parsed.date || new Date().toISOString(),
      sizeBytes: message.rawSize || 0,
    };

    // 1) Guardar en el buzón y averiguar a dónde reenviar la copia.
    let forwardTo = '';
    try {
      const res = await fetch(env.INGEST_URL, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-ingest-secret': env.INGEST_SECRET || '',
        },
        body: JSON.stringify(payload),
      });
      const data = await res.json().catch(() => ({}));
      forwardTo = (data && data.forwardTo) || '';
    } catch (err) {
      // No romper la entrega si el guardado falla temporalmente.
    }

    // 2) Copia por reenvío a un destino verificado (buzón + reenvío a la vez).
    if (forwardTo && message.canBeForwarded !== false) {
      try {
        await message.forward(forwardTo);
      } catch (err) {
        // El destino debe estar verificado en Cloudflare; si no, se ignora.
      }
    }
  },
};
