function toMillis(value) {
  if (!value) return 0;
  if (value instanceof Date) return value.getTime() || 0;
  if (typeof value?.toMillis === 'function') return value.toMillis() || 0;
  if (typeof value?.toDate === 'function') return value.toDate()?.getTime?.() || 0;
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function minutesAgo(value, now = new Date()) {
  const ms = toMillis(value);
  if (!ms) return '';
  const diff = Math.max(0, (toMillis(now) || Date.now()) - ms);
  const minutes = Math.max(1, Math.round(diff / 60_000));
  if (minutes < 60) return `hace ${minutes} min`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `hace ${hours} h`;
  return `hace ${Math.round(hours / 24)} dias`;
}

function formatShortDate(value) {
  const ms = toMillis(value);
  if (!ms) return '';
  return new Date(ms).toLocaleString('es-MX', {
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export function humanQueueReason({ reasonCode = '', lead = {}, analysis = {}, now = new Date() } = {}) {
  const code = String(reasonCode || '').trim();
  if (code === 'asked_payment_method') return `Pregunto como realizar el pago ${minutesAgo(lead.lastMessageAt, now)}`.trim();
  if (code === 'asked_price') return 'Pregunto precio despues de mostrar interes';
  if (code === 'ready_to_buy') return 'Mostro intencion alta de compra';
  if (code === 'commercial_question') return 'Hizo una pregunta comercial relevante';
  if (code === 'high_interest') return 'Mostro interes alto en la conversacion';
  if (code === 'followup_overdue') {
    const when = formatShortDate(lead?.followUp?.nextAt);
    return when ? `Seguimiento pendiente desde ${when}` : 'Seguimiento pendiente vencido';
  }
  if (Number(lead?.unreadCount || 0) > 0) {
    return `Mensaje esperando respuesta ${minutesAgo(lead.lastMessageAt, now)}`.trim();
  }
  if (analysis?.summary) return String(analysis.summary).replace(/\s+/g, ' ').trim().slice(0, 180);
  return 'Requiere revision comercial';
}
