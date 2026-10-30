import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import sharp from 'sharp';

const templatePath = fileURLToPath(new URL('../assets/spei-payment-template.png', import.meta.url));
const escape = value => String(value ?? '').replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
const money = cents => `${(cents / 100).toLocaleString('es-MX', { style: 'currency', currency: 'MXN' })} MXN`;

function field(value, x, y, width, maxSize, maxLines = 1) {
  const text = String(value || '').trim().replace(/\s+/g, ' ');
  let size = maxSize, lines;
  do {
    const capacity = Math.floor(width / (size * 0.65));
    lines = [''];
    for (const word of text.split(' ')) {
      const last = lines.length - 1;
      if (lines[last] && lines[last].length + word.length + 1 > capacity) lines.push(word);
      else lines[last] += (lines[last] ? ' ' : '') + word;
    }
    if (lines.length <= maxLines && lines.every(line => line.length <= capacity)) break;
    size -= 1;
  } while (size > 12);
  const spacing = size * 1.15;
  return `<text x="${x}" y="${y + size * 0.35 - (lines.length - 1) * spacing / 2}" fill="#ffffff" font-family="DejaVu Sans,Arial,sans-serif" font-size="${size}" font-weight="600" dominant-baseline="middle">${lines.map((line, i) => `<tspan x="${x}" dy="${i ? spacing : 0}">${escape(line)}</tspan>`).join('')}</text>`;
}

export async function renderSpeiPaymentImage(record, instructions) {
  if (!/^\d{18}$/.test(instructions.clabe || '') || !instructions.holder) throw Error('Stripe no devolvió una CLABE o beneficiario válidos.');
  const amount = instructions.remainingCents ?? record.amountCents;
  if (![amount, record.amountCents, record.totalCents].every(Number.isSafeInteger) || amount < 0 || record.totalCents < record.amountCents) throw Error('El importe de la referencia SPEI no es válido.');
  const bank = String(instructions.bank || 'Transferencia bancaria');
  const citi = /^(?:citi(?:\s+m[eé]xico)?|citibanamex|banamex|banco\s+nacional\s+de\s+m[eé]xico)$/i.test(bank.trim());
  const overlay = `<svg xmlns="http://www.w3.org/2000/svg" width="1254" height="1254">
    ${field(record.planName, 260, 325, 875, 42, 2)}
    ${field(money(amount), 260, 521, 425, 44)}
    ${field(money(record.totalCents - record.amountCents), 920, 526, 230, 30)}
    ${field(instructions.clabe, 460, 787, 620, 40)}
    ${field(instructions.holder, 460, 894, 620, 36, 2)}
    ${field(instructions.reference || 'Sin referencia adicional', 460, 1001, 620, 38)}
    ${!citi ? `<rect x="230" y="620" width="950" height="85" rx="10" fill="#03140f"/>${field(`SPEI · ${bank}`, 240, 668, 920, 40, 2)}` : ''}
  </svg>`;
  return sharp(templatePath).composite([{ input: Buffer.from(overlay) }]).png().toBuffer();
}

export function createSpeiPaymentRenderer({ bucket }) {
  return async ({ requestId, record, instructions }) => {
    if (!/^[a-f0-9]{64}$/.test(requestId)) throw Error('Referencia de anticipo no válida.');
    const signature = createHash('sha256').update(JSON.stringify({ revision: 1, plan: record.planName, total: record.totalCents, amount: record.amountCents, instructions })).digest('hex');
    const file = bucket.file(`payment-vouchers/spei/${requestId}/${signature}.png`);
    if (!(await file.exists())[0]) await file.save(await renderSpeiPaymentImage(record, instructions), { resumable: false, metadata: { contentType: 'image/png', cacheControl: 'private,max-age=3600' } });
    return (await file.getSignedUrl({ action: 'read', expires: new Date(Date.now() + 7 * 86400_000) }))[0];
  };
}
