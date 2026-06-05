// services/biReport.js
//
// Genera un informe de Business Intelligence del CRM a partir de la coleccion
// `leads` (+ `tasks`). Devuelve datos estructurados y un Markdown listo para
// pegar en Claude y pedir analisis de estrategia de producto y seguimiento.
//
// Diseno: una sola pasada sobre los leads, usando campos a nivel documento
// (sin leer subcolecciones de mensajes) para que sea barato.
//
const WON_STATUSES = new Set(['compro', 'cliente', 'ganado', 'closed_won', 'cerrado_ganado', 'pagado']);
const LOST_STATUSES = new Set(['no_interesa', 'nointeresa', 'perdido', 'descartado', 'closed_lost']);

function safeStr(value = '') {
  return String(value || '').trim();
}

function lower(value = '') {
  return safeStr(value).toLowerCase();
}

function normToken(value = '') {
  return String(value || '')
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function toMillis(value) {
  if (value === null || value === undefined || value === '') return 0;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (value instanceof Date) return value.getTime() || 0;
  if (typeof value?.toMillis === 'function') return value.toMillis() || 0;
  if (typeof value?.toDate === 'function') return value.toDate()?.getTime?.() || 0;
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function pct(n, d) {
  if (!d) return '—';
  return `${((100 * n) / d).toFixed(1)}%`;
}

function tagSet(lead = {}) {
  const tags = Array.isArray(lead?.etiquetas) ? lead.etiquetas : [];
  return new Set(tags.map((t) => normToken(t)));
}

function hasSlug(lead = {}) {
  return Boolean(
    safeStr(lead?.slug) || safeStr(lead?.webSlug) || safeStr(lead?.siteSlug)
    || safeStr(lead?.briefWeb?.slug) || safeStr(lead?.schema?.slug)
  );
}

function hasCompletedForm(lead = {}, tags) {
  if (lower(lead?.etapa) === 'form_submitted' || lower(lead?.etapaNombre) === 'form_submitted') return true;
  return tags.has('formok') || tags.has('formulariocompletado');
}

function incr(map, key, by = 1) {
  const k = key || '(sin dato)';
  map.set(k, (map.get(k) || 0) + by);
}

function mapToSortedRows(map) {
  return [...map.entries()].sort((a, b) => b[1] - a[1]);
}

function renderTable(headers, rows) {
  const head = `| ${headers.join(' | ')} |`;
  const sep = `| ${headers.map(() => '---').join(' | ')} |`;
  const body = rows.map((r) => `| ${r.join(' | ')} |`).join('\n');
  return [head, sep, body].join('\n');
}

function monthKey(ms) {
  if (!ms) return '(sin fecha)';
  const d = new Date(ms);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
}

async function getDb(dbOverride) {
  if (dbOverride) return dbOverride;
  const { db } = await import('../firebaseAdmin.js');
  return db;
}

export async function generateBiReport({ dbOverride = null, now = new Date() } = {}) {
  const db = await getDb(dbOverride);
  const nowMs = now.getTime();
  const DAY = 24 * 60 * 60 * 1000;

  const leadsSnap = await db.collection('leads').get();

  const totals = {
    all: 0,
    active: 0,
    archived: 0,
    merged: 0,
  };

  const bySource = new Map();
  const byStatus = new Map();
  const byStage = new Map();
  const byMonth = new Map();
  const wonByMonth = new Map();
  const touchBuckets = new Map([['0', 0], ['1-2', 0], ['3-5', 0], ['6+', 0]]);
  const recencyBuckets = new Map([['< 24h', 0], ['1-3 dias', 0], ['3-7 dias', 0], ['7-30 dias', 0], ['> 30 dias', 0], ['sin actividad', 0]]);
  const sourceStats = new Map(); // source -> { total, replied, formCompleted, won }

  let replied = 0;
  let neverContacted = 0;
  let contactedToday = 0;
  let silent7d = 0;
  let formLinkSent = 0;
  let formCompleted = 0;
  let sampleGenerated = 0;
  let hot = 0;
  let bot = 0;
  let won = 0;
  let lost = 0;
  let stopped = 0;
  let withUnread = 0;

  const startOfToday = new Date(now);
  startOfToday.setHours(0, 0, 0, 0);
  const startOfTodayMs = startOfToday.getTime();

  leadsSnap.forEach((doc) => {
    const lead = doc.data() || {};
    totals.all += 1;

    if (lead?.mergedInto) {
      totals.merged += 1;
      return; // los merged no cuentan para metricas
    }
    const archived = lead?.isArchived === true || lead?.archived === true || Boolean(lead?.archivedAt);
    if (archived) {
      totals.archived += 1;
      return;
    }
    totals.active += 1;

    const tags = tagSet(lead);
    const source = safeStr(lead?.source) || '(sin fuente)';
    const status = lower(lead?.estado) || '(sin estado)';
    const stage = normToken(lead?.etapa || lead?.etapaNombre) || 'leads_nuevos';

    const createdMs = toMillis(lead?.fecha_creacion);
    const inboundMs = toMillis(lead?.lastInboundAt);
    const outboundMs = Math.max(
      toMillis(lead?.lastOutboundAt),
      toMillis(lead?.lastManualFollowupAt),
      toMillis(lead?.aiFollowup?.lastSentAt)
    );
    const lastActivityMs = Math.max(inboundMs, toMillis(lead?.lastMessageAt), outboundMs, createdMs);

    incr(bySource, source);
    incr(byStatus, status);
    incr(byStage, stage);
    incr(byMonth, monthKey(createdMs));

    if (!sourceStats.has(source)) sourceStats.set(source, { total: 0, replied: 0, formCompleted: 0, won: 0 });
    const ss = sourceStats.get(source);
    ss.total += 1;

    const didReply = inboundMs > 0;
    if (didReply) { replied += 1; ss.replied += 1; }

    if (outboundMs <= 0) neverContacted += 1;
    if (outboundMs >= startOfTodayMs) contactedToday += 1;
    if (!didReply && createdMs > 0 && (nowMs - lastActivityMs) > 7 * DAY) silent7d += 1;
    if (Number(lead?.unreadCount || 0) > 0) withUnread += 1;

    const linkSent = tags.has('formlinksent') || tags.has('samplelinksent') || tags.has('webenviada') || tags.has('muestralista') || tags.has('muestraactiva');
    const completed = hasCompletedForm(lead, tags);
    const sample = hasSlug(lead) || tags.has('samplelinksent') || tags.has('muestraactiva');
    if (linkSent) formLinkSent += 1;
    if (completed) formCompleted += 1;
    if (sample) sampleGenerated += 1;

    if (tags.has('respuestacaliente') || lead?.aiReply?.hot === true) hot += 1;
    if (tags.has('respuestaautomatica') || lead?.autoResponder?.detected === true) bot += 1;
    if (lead?.stopSequences === true || tags.has('detenersecuencia') || tags.has('stopsequences')) stopped += 1;

    const isWon = WON_STATUSES.has(status) || tags.has('compro');
    const isLost = LOST_STATUSES.has(status) || tags.has('no_interesa') || tags.has('nointeresa');
    if (isWon) { won += 1; ss.won += 1; incr(wonByMonth, monthKey(createdMs)); }
    if (isLost) lost += 1;
    if (completed) ss.formCompleted += 1;

    const touch = Number(lead?.aiFollowup?.touchCount || 0);
    if (touch <= 0) incr(touchBuckets, '0');
    else if (touch <= 2) incr(touchBuckets, '1-2');
    else if (touch <= 5) incr(touchBuckets, '3-5');
    else incr(touchBuckets, '6+');

    const sinceActivity = nowMs - lastActivityMs;
    if (lastActivityMs <= 0) incr(recencyBuckets, 'sin actividad');
    else if (sinceActivity < DAY) incr(recencyBuckets, '< 24h');
    else if (sinceActivity < 3 * DAY) incr(recencyBuckets, '1-3 dias');
    else if (sinceActivity < 7 * DAY) incr(recencyBuckets, '3-7 dias');
    else if (sinceActivity < 30 * DAY) incr(recencyBuckets, '7-30 dias');
    else incr(recencyBuckets, '> 30 dias');
  });

  // Tareas (alertas del detector + manuales)
  const taskStats = { total: 0, open: 0, bySource: new Map() };
  try {
    const tasksSnap = await db.collection('tasks').limit(8000).get();
    tasksSnap.forEach((doc) => {
      const t = doc.data() || {};
      taskStats.total += 1;
      const open = lower(t?.status) !== 'completada';
      if (open) {
        taskStats.open += 1;
        incr(taskStats.bySource, safeStr(t?.source) || 'manual');
      }
    });
  } catch {
    /* tasks opcional */
  }

  const data = {
    generatedAt: new Date(nowMs).toISOString(),
    totals,
    active: totals.active,
    bySource: Object.fromEntries(mapToSortedRows(bySource)),
    byStatus: Object.fromEntries(mapToSortedRows(byStatus)),
    byStage: Object.fromEntries(mapToSortedRows(byStage)),
    sampleFunnel: { formLinkSent, formCompleted, sampleGenerated },
    engagement: { replied, neverContacted, contactedToday, silent7d, withUnread, hot, bot, stopped },
    outcome: { won, lost },
    touchBuckets: Object.fromEntries(touchBuckets),
    recencyBuckets: Object.fromEntries(recencyBuckets),
    byMonth: Object.fromEntries(mapToSortedRows(byMonth)),
    tasks: { total: taskStats.total, open: taskStats.open, bySource: Object.fromEntries(mapToSortedRows(taskStats.bySource)) },
  };

  // ----------------------------- Markdown -----------------------------
  const A = totals.active || 1;
  const lines = [];
  lines.push('# Informe BI — CRM NegociosWeb');
  lines.push(`Generado: ${new Date(nowMs).toLocaleString('es-MX')}`);
  lines.push('');
  lines.push('## 1. Resumen general');
  lines.push(renderTable(['Metrica', 'Valor'], [
    ['Leads totales (incluye archivados/merged)', String(totals.all)],
    ['Leads activos (base de analisis)', String(totals.active)],
    ['Archivados', String(totals.archived)],
    ['Merged/duplicados', String(totals.merged)],
    ['Ganados (compraron)', `${won} (${pct(won, A)})`],
    ['Perdidos / no interesa', `${lost} (${pct(lost, A)})`],
  ]));
  lines.push('');

  lines.push('## 2. Embudo de muestra (clave del negocio)');
  lines.push('Flujo: se envia link de formulario → cliente lo llena → se genera su muestra → compra.');
  lines.push(renderTable(['Etapa', 'Leads', '% de activos', '% del paso previo'], [
    ['Recibieron link de formulario/muestra', String(formLinkSent), pct(formLinkSent, A), '—'],
    ['Llenaron el formulario', String(formCompleted), pct(formCompleted, A), pct(formCompleted, formLinkSent)],
    ['Tienen muestra generada', String(sampleGenerated), pct(sampleGenerated, A), pct(sampleGenerated, formCompleted)],
    ['Compraron', String(won), pct(won, A), pct(won, sampleGenerated)],
  ]));
  lines.push('');

  lines.push('## 3. Seguimiento y engagement');
  lines.push(renderTable(['Metrica', 'Leads', '% de activos'], [
    ['Respondieron al menos una vez', String(replied), pct(replied, A)],
    ['Nunca se les ha escrito (saliente)', String(neverContacted), pct(neverContacted, A)],
    ['Contactados hoy', String(contactedToday), pct(contactedToday, A)],
    ['Silenciosos >7 dias (no respondieron)', String(silent7d), pct(silent7d, A)],
    ['Con mensajes sin leer', String(withUnread), pct(withUnread, A)],
    ['Respuestas calientes detectadas', String(hot), pct(hot, A)],
    ['Bots/IA contestando detectados', String(bot), pct(bot, A)],
    ['Marcados como detener/stop', String(stopped), pct(stopped, A)],
  ]));
  lines.push('');

  lines.push('## 4. Por fuente de lead');
  lines.push(renderTable(['Fuente', 'Leads', '% respondio', '% lleno form', '% compro'],
    mapToSortedRows(bySource).map(([src]) => {
      const s = sourceStats.get(src) || { total: 0, replied: 0, formCompleted: 0, won: 0 };
      return [src, String(s.total), pct(s.replied, s.total), pct(s.formCompleted, s.total), pct(s.won, s.total)];
    })
  ));
  lines.push('');

  lines.push('## 5. Por estado en el CRM');
  lines.push(renderTable(['Estado', 'Leads', '%'], mapToSortedRows(byStatus).map(([k, v]) => [k, String(v), pct(v, A)])));
  lines.push('');

  lines.push('## 6. Por etapa del embudo');
  lines.push(renderTable(['Etapa', 'Leads', '%'], mapToSortedRows(byStage).map(([k, v]) => [k, String(v), pct(v, A)])));
  lines.push('');

  lines.push('## 7. Recencia (ultima actividad)');
  lines.push(renderTable(['Antiguedad', 'Leads', '%'], [...recencyBuckets.entries()].map(([k, v]) => [k, String(v), pct(v, A)])));
  lines.push('');

  lines.push('## 8. Intensidad de seguimiento IA (toques)');
  lines.push(renderTable(['Toques de reactivacion', 'Leads'], [...touchBuckets.entries()].map(([k, v]) => [k, String(v)])));
  lines.push('');

  lines.push('## 9. Cohortes por mes de creacion');
  lines.push(renderTable(['Mes', 'Leads creados', 'Compraron', '% conversion'],
    mapToSortedRows(byMonth).map(([m, count]) => {
      const w = wonByMonth.get(m) || 0;
      return [m, String(count), String(w), pct(w, count)];
    })
  ));
  lines.push('');

  lines.push('## 10. Tareas (alertas del sistema)');
  lines.push(renderTable(['Metrica', 'Valor'], [
    ['Tareas abiertas', String(taskStats.open)],
    ...mapToSortedRows(taskStats.bySource).map(([k, v]) => [`  - origen: ${k}`, String(v)]),
  ]));
  lines.push('');

  lines.push('---');
  lines.push('## Contexto para el analisis');
  lines.push('Negocio: agencia que vende paginas web, campanas de Meta Ads y software a la medida en Mexico.');
  lines.push('Gancho principal: muestra de pagina GRATIS (el cliente llena un formulario corto y se le genera).');
  lines.push('Canal principal: WhatsApp. Estrategia: seguimiento constante (recordar a diario) sin que baneen el numero.');
  lines.push('');
  lines.push('## Preguntas que quiero que analices');
  lines.push('1. ¿Donde se cae mas el embudo de muestra y que harias para subir esa conversion?');
  lines.push('2. ¿Que fuente de leads conviene mas (responde/convierte) y donde invertir?');
  lines.push('3. ¿Que segmentos de leads (por estado/etapa/recencia) debo priorizar esta semana?');
  lines.push('4. ¿La estrategia de seguimiento esta funcionando? ¿Que mensajes/acciones probar?');
  lines.push('5. ¿Que mejoras de PRODUCTO (oferta, muestra, formulario, precios) sugieres con estos numeros?');
  lines.push('6. Dame un plan de accion concreto para las proximas 2 semanas.');

  return { data, markdown: lines.join('\n') };
}
