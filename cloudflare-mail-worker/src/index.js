// Cloudflare Email Worker — captura el correo entrante de los alias con buzón.
// Flujo: parsea el MIME → lo manda al Core API (/mailbox/ingest) → reenvía copia.
//
// Requiere secret: INGEST_SECRET (= MAILBOX_INGEST_SECRET del Core API).
// La regla de Email Routing del alias (ej. ventas@dominio.com) debe apuntar a
// este Worker (acción "Send to a Worker").
import PostalMime from 'postal-mime';

function numberFromEnv(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function bytesToBase64(bytes) {
  let binary = '';
  const chunkSize = 0x8000;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunkSize));
  }
  return btoa(binary);
}

function attachmentContentToBytes(content) {
  if (!content) return new Uint8Array();
  if (content instanceof Uint8Array) return content;
  if (content instanceof ArrayBuffer) return new Uint8Array(content);
  if (ArrayBuffer.isView(content)) {
    return new Uint8Array(content.buffer, content.byteOffset, content.byteLength);
  }
  if (typeof content === 'string') {
    return new TextEncoder().encode(content);
  }
  return new Uint8Array();
}

function normalizeAttachments(parsedAttachments = [], env = {}) {
  const maxFiles = numberFromEnv(env.MAILBOX_INBOUND_MAX_ATTACHMENTS, 5);
  const maxFileBytes = numberFromEnv(env.MAILBOX_INBOUND_MAX_ATTACHMENT_BYTES, 5 * 1024 * 1024);
  const maxTotalBytes = numberFromEnv(env.MAILBOX_INBOUND_MAX_TOTAL_ATTACHMENT_BYTES, 10 * 1024 * 1024);
  const out = [];
  let totalBytes = 0;

  for (const attachment of Array.isArray(parsedAttachments) ? parsedAttachments : []) {
    if (out.length >= maxFiles) break;
    const bytes = attachmentContentToBytes(attachment?.content);
    if (bytes.byteLength <= 0 || bytes.byteLength > maxFileBytes) continue;
    if (totalBytes + bytes.byteLength > maxTotalBytes) continue;

    totalBytes += bytes.byteLength;
    out.push({
      filename: attachment?.filename || `adjunto-${out.length + 1}`,
      contentType: attachment?.mimeType || attachment?.contentType || 'application/octet-stream',
      contentId: attachment?.contentId || '',
      disposition: attachment?.disposition || 'attachment',
      sizeBytes: bytes.byteLength,
      contentBase64: bytesToBase64(bytes),
    });
  }

  return out;
}

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
      attachments: normalizeAttachments(parsed.attachments, env),
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
