import crypto from 'node:crypto';

function cleanString(value = '', maxLength = 1000) {
  return String(value ?? '').trim().slice(0, maxLength);
}

function decodeBuffer(buffer, charset = '') {
  const normalized = String(charset || '').trim().toLowerCase();
  if (normalized.includes('iso-8859-1') || normalized.includes('latin1') || normalized.includes('windows-1252')) {
    return Buffer.from(buffer).toString('latin1');
  }
  return Buffer.from(buffer).toString('utf8');
}

function decodeQuotedPrintable(value = '', charset = '') {
  const withoutSoftBreaks = String(value || '').replace(/=\r?\n/g, '');
  const bytes = [];
  for (let index = 0; index < withoutSoftBreaks.length; index += 1) {
    const char = withoutSoftBreaks[index];
    if (char === '=' && /^[0-9a-fA-F]{2}$/.test(withoutSoftBreaks.slice(index + 1, index + 3))) {
      bytes.push(Number.parseInt(withoutSoftBreaks.slice(index + 1, index + 3), 16));
      index += 2;
    } else {
      bytes.push(char.charCodeAt(0));
    }
  }
  return decodeBuffer(Buffer.from(bytes), charset);
}

function decodeEncodedWords(value = '') {
  return String(value || '').replace(/=\?([^?]+)\?([bqBQ])\?([^?]*)\?=/g, (_match, charset, encoding, text) => {
    try {
      if (String(encoding).toUpperCase() === 'B') {
        return decodeBuffer(Buffer.from(String(text || ''), 'base64'), charset);
      }
      const qp = String(text || '').replace(/_/g, ' ');
      return decodeQuotedPrintable(qp, charset);
    } catch {
      return text || '';
    }
  });
}

function parseHeaderBlock(rawHeaders = '') {
  const lines = String(rawHeaders || '').split(/\r?\n/);
  const unfolded = [];
  for (const line of lines) {
    if (/^[\t ]/.test(line) && unfolded.length > 0) {
      unfolded[unfolded.length - 1] += ` ${line.trim()}`;
    } else {
      unfolded.push(line);
    }
  }

  const headers = new Map();
  for (const line of unfolded) {
    const colonIndex = line.indexOf(':');
    if (colonIndex <= 0) continue;
    const name = line.slice(0, colonIndex).trim().toLowerCase();
    const value = line.slice(colonIndex + 1).trim();
    if (!headers.has(name)) headers.set(name, []);
    headers.get(name).push(value);
  }
  return headers;
}

function header(headers, name = '') {
  const values = headers.get(String(name || '').toLowerCase());
  return values?.length ? decodeEncodedWords(values.join(', ')) : '';
}

function splitHeaderAndBody(raw = '') {
  const normalized = String(raw || '').replace(/\r\n/g, '\n');
  const match = normalized.match(/\n\n/);
  if (!match) return { rawHeaders: normalized, body: '' };
  const index = match.index;
  return {
    rawHeaders: normalized.slice(0, index),
    body: normalized.slice(index + 2),
  };
}

function parseContentType(value = '') {
  const parts = String(value || '').split(';');
  const mime = cleanString(parts.shift() || 'text/plain', 120).toLowerCase();
  const params = {};
  for (const part of parts) {
    const equalIndex = part.indexOf('=');
    if (equalIndex <= 0) continue;
    const key = part.slice(0, equalIndex).trim().toLowerCase();
    let val = part.slice(equalIndex + 1).trim();
    if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
      val = val.slice(1, -1);
    }
    params[key] = val;
  }
  return {
    mime,
    charset: params.charset || '',
    boundary: params.boundary || '',
  };
}

function decodeBody(body = '', headers = new Map()) {
  const transfer = header(headers, 'content-transfer-encoding').toLowerCase();
  const contentType = parseContentType(header(headers, 'content-type'));
  if (transfer === 'base64') {
    try {
      return decodeBuffer(Buffer.from(String(body || '').replace(/\s+/g, ''), 'base64'), contentType.charset);
    } catch {
      return String(body || '');
    }
  }
  if (transfer === 'quoted-printable') {
    return decodeQuotedPrintable(body, contentType.charset);
  }
  return String(body || '');
}

function splitMultipart(body = '', boundary = '') {
  const safeBoundary = String(boundary || '');
  if (!safeBoundary) return [];
  const delimiter = `--${safeBoundary}`;
  const closeDelimiter = `--${safeBoundary}--`;
  const lines = String(body || '').replace(/\r\n/g, '\n').split('\n');
  const parts = [];
  let current = null;

  for (const line of lines) {
    if (line === delimiter || line === closeDelimiter) {
      if (current) parts.push(current.join('\n'));
      current = line === closeDelimiter ? null : [];
      continue;
    }
    if (current) current.push(line);
  }
  if (current?.length) parts.push(current.join('\n'));
  return parts;
}

function parseBody(rawBody = '', headers = new Map()) {
  const contentType = parseContentType(header(headers, 'content-type'));
  if (contentType.mime.startsWith('multipart/') && contentType.boundary) {
    const parts = splitMultipart(rawBody, contentType.boundary);
    const out = { textBody: '', htmlBody: '' };
    for (const part of parts) {
      const parsed = parseMessagePart(part);
      if (!out.textBody && parsed.textBody) out.textBody = parsed.textBody;
      if (!out.htmlBody && parsed.htmlBody) out.htmlBody = parsed.htmlBody;
      if (out.textBody && out.htmlBody) break;
    }
    return out;
  }

  const decoded = decodeBody(rawBody, headers);
  if (contentType.mime === 'text/html') {
    return { textBody: '', htmlBody: decoded };
  }
  if (contentType.mime === 'text/plain' || contentType.mime === 'message/rfc822') {
    return { textBody: decoded, htmlBody: '' };
  }
  return { textBody: '', htmlBody: '' };
}

function parseMessagePart(raw = '') {
  const { rawHeaders, body } = splitHeaderAndBody(raw);
  const headers = parseHeaderBlock(rawHeaders);
  return parseBody(body, headers);
}

function extractEmailAddresses(value = '') {
  const source = decodeEncodedWords(String(value || ''));
  const found = [];
  const bracketRegex = /<([^<>@\s]+@[^<>\s]+)>/g;
  let match = null;
  while ((match = bracketRegex.exec(source)) !== null) {
    found.push(match[1].trim().toLowerCase());
  }
  const plainRegex = /(^|[,\s])([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})(?=$|[,\s;])/gi;
  while ((match = plainRegex.exec(source)) !== null) {
    found.push(match[2].trim().toLowerCase());
  }
  return Array.from(new Set(found));
}

function firstEmail(value = '') {
  return extractEmailAddresses(value)[0] || cleanString(value, 320).toLowerCase();
}

function normalizeMessageId(value = '') {
  return cleanString(decodeEncodedWords(value), 260).replace(/^<|>$/g, '');
}

function hashRawMessage(raw = '') {
  return crypto.createHash('sha1').update(String(raw || '')).digest('hex');
}

export function parseEmailMessage(raw = '') {
  const source = String(raw || '');
  const { rawHeaders, body } = splitHeaderAndBody(source);
  const headers = parseHeaderBlock(rawHeaders);
  const parsedBody = parseBody(body, headers);
  const rawMessageId = normalizeMessageId(header(headers, 'message-id'));
  const fallbackHash = hashRawMessage(source);
  const date = cleanString(header(headers, 'date'), 120);

  return {
    messageId: rawMessageId || `import-${fallbackHash}`,
    importHash: fallbackHash,
    from: firstEmail(header(headers, 'from')),
    to: extractEmailAddresses(header(headers, 'to')),
    cc: extractEmailAddresses(header(headers, 'cc')),
    subject: cleanString(header(headers, 'subject'), 500),
    date,
    textBody: parsedBody.textBody,
    htmlBody: parsedBody.htmlBody,
    sizeBytes: Buffer.byteLength(source),
  };
}

export function splitMbox(raw = '') {
  const normalized = String(raw || '').replace(/\r\n/g, '\n');
  const lines = normalized.split('\n');
  const messages = [];
  let current = [];
  let sawSeparator = false;

  for (const line of lines) {
    if (/^From (?:-|[^:]+?\s)/.test(line)) {
      if (current.length > 0) {
        messages.push(current.join('\n').trim());
        current = [];
      }
      sawSeparator = true;
      continue;
    }
    current.push(line);
  }

  if (current.length > 0) {
    const message = current.join('\n').trim();
    if (message) messages.push(message);
  }

  if (!sawSeparator && normalized.trim()) {
    return [normalized.trim()];
  }
  return messages.filter(Boolean);
}

export function parseMailboxImport(raw = '', { maxMessages = 1000 } = {}) {
  const safeLimit = Math.max(1, Math.min(5000, Number(maxMessages) || 1000));
  const chunks = splitMbox(raw).slice(0, safeLimit);
  return chunks
    .map((chunk) => parseEmailMessage(chunk))
    .filter((message) => (
      message.from
      || message.subject
      || message.textBody
      || message.htmlBody
    ));
}
