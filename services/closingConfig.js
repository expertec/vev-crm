import { createHash } from 'node:crypto';
export const closingDefaults = { enabled: false, defaultOfferId: '', timezone: 'America/Monterrey', startHour: 9, endHour: 20, firstDelayMinutes: 15, followupHours: [24, 48], methods: ['card', 'oxxo', 'spei'], issuerName: 'NEGOCIOS WEB MX', issuerRfc: '', offers: [] };
export const clean = (v, n = 1000) => String(v || '').trim().slice(0, n);
export const ms = v => v?.toMillis?.() || new Date(v || 0).getTime() || 0;
export const money = n => (n / 100).toLocaleString('es-MX', { style: 'currency', currency: 'MXN' });
export function sanitizeClosingConfig(raw) {
  const n = (v, min, max) => { const x = Number(v); if (!Number.isFinite(x) || x < min || x > max) throw Error('Revisa los importes y tiempos configurados.'); return x; };
  const cfg = { ...closingDefaults, enabled: raw.enabled === true, defaultOfferId: clean(raw.defaultOfferId, 80), ownerUid: clean(raw.ownerUid, 128), timezone: clean(raw.timezone || closingDefaults.timezone, 80), startHour: n(raw.startHour ?? 9, 0, 23), endHour: n(raw.endHour ?? 20, 1, 24), firstDelayMinutes: n(raw.firstDelayMinutes ?? 15, 1, 1440), followupHours: (raw.followupHours || [24, 48]).slice(0, 5).map(x => n(x, 1, 720)), methods: [...new Set((raw.methods || closingDefaults.methods).filter(x => closingDefaults.methods.includes(x)))], issuerName: clean(raw.issuerName || closingDefaults.issuerName, 150), issuerRfc: clean(raw.issuerRfc, 30), offers: [] };
  new Intl.DateTimeFormat('es-MX', { timeZone: cfg.timezone }).format();
  if (cfg.startHour >= cfg.endHour || !cfg.methods.length) throw Error('Define un horario de inicio menor al final y al menos un método de pago.');
  const ids = new Set();
  cfg.offers = (raw.offers || []).slice(0, 30).map(o => {
    const id = clean(o.id, 80); if (!/^[a-zA-Z0-9_-]+$/.test(id) || ids.has(id)) throw Error('Cada promoción necesita un identificador único de letras, números o guiones.'); ids.add(id);
    const offer = { id, enabled: o.enabled === true, name: clean(o.name, 120), totalCents: Math.round(n(o.totalCents, 1000, 100000000)), depositCents: Math.round(n(o.depositCents, 1000, 100000000)), plan: clean(o.plan, 20), years: n(o.years ?? 1, 1, 5), terms: clean(o.terms, 5000), domain: clean(o.domain, 300), renewal: clean(o.renewal, 300), delivery: clean(o.delivery, 300), balanceDue: clean(o.balanceDue, 300), startsAt: o.startsAt ? new Date(o.startsAt).toISOString() : '', endsAt: o.endsAt ? new Date(o.endsAt).toISOString() : '', imagePath: clean(o.imagePath, 250), audios: {} };
    if (!offer.name || !['basic', 'pro', 'premium', 'ventas'].includes(offer.plan) || offer.depositCents > offer.totalCents || !Number.isInteger(offer.years)) throw Error('Revisa nombre, plan, anticipo y vigencia del plan.');
    if (offer.enabled && (!offer.terms || !offer.domain || !offer.renewal || !offer.delivery || !offer.balanceDue)) throw Error('Completa inclusiones, dominio, renovación, entrega y condición del saldo antes de activar una promoción.');
    if (offer.endsAt && offer.startsAt && ms(offer.endsAt) <= ms(offer.startsAt)) throw Error('El fin de la promoción debe ser posterior al inicio.');
    if (offer.imagePath && !/^closing-assets\/[a-f0-9-]+\.png$/.test(offer.imagePath)) throw Error('Sube la imagen desde este administrador.');
    for (const moment of ['offer', 'changes', 'payment', 'followup']) {
      const a = o.audios?.[moment];
      if (a?.path) { if (!/^closing-assets\/[a-f0-9-]+\.ogg$/.test(a.path)) throw Error('Sube el audio desde este administrador.'); offer.audios[moment] = { path: a.path, script: clean(a.script, 1600) }; }
    }
    offer.revision = createHash('sha256').update(JSON.stringify(offer)).digest('hex').slice(0, 16);
    return offer;
  });
  if (cfg.enabled && !cfg.offers.some(o => o.id === cfg.defaultOfferId && offerAvailable(o))) throw Error('Selecciona una promoción activa y vigente antes de encender el agente.');
  return cfg;
}
export function offerAvailable(o, at = Date.now()) { return o?.enabled && (!o.startsAt || ms(o.startsAt) <= at) && (!o.endsAt || ms(o.endsAt) > at); }
export function nextClosingTime(at, cfg) {
  const formatter = new Intl.DateTimeFormat('en-US', { timeZone: cfg.timezone, hour: '2-digit', minute: '2-digit', hourCycle: 'h23' });
  let value = Number(at);
  for (let i = 0; i < 192; i++, value += 15 * 60000) {
    const parts = Object.fromEntries(formatter.formatToParts(value).map(p => [p.type, p.value]));
    const hour = Number(parts.hour) + Number(parts.minute) / 60;
    if (hour >= cfg.startHour && hour < cfg.endHour) return new Date(value);
  }
  throw Error('No se pudo programar dentro del horario.');
}
export const offerPlanId = o => `closing_${o.id}_${o.revision}`;
export function createClosingConfigStore(db) {
  let cached, until = 0;
  return { async get() { if (cached && until > Date.now()) return cached; const data = (await db.collection('config').doc('closingAgent').get()).data(); cached = data ? { ...closingDefaults, ...data } : { ...closingDefaults }; until = Date.now() + 30000; return cached; }, async save(raw) { const cfg = sanitizeClosingConfig(raw); await db.collection('config').doc('closingAgent').set({ ...cfg, updatedAt: new Date() }); cached = cfg; until = Date.now() + 30000; return cfg; } };
}
