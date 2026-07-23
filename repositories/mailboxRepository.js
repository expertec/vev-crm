// repositories/mailboxRepository.js
// Almacenamiento del "mini-mail": bandeja de entrada por correo corporativo y
// configuración del buzón (contraseña, reenvío-copia). Reutiliza la colección
// Negocios/{empresaId}/corporateEmails/{correoId} y le añade la subcolección `inbox`.
import { FieldValue, Timestamp } from 'firebase-admin/firestore';
import crypto from 'node:crypto';
import { db } from '../firebaseAdmin.js';
import {
  buildCorporateEmailRecordId,
  normalizeAlias,
  normalizeDomain,
} from '../utils/corporateEmailUtils.js';

function cleanId(value = '', maxLength = 200) {
  return String(value ?? '').trim().slice(0, maxLength);
}

function normalizeEmail(value = '') {
  return String(value || '').trim().toLowerCase();
}

function toTimestamp(value, fallback = Timestamp.now()) {
  if (!value) return fallback;
  if (typeof value?.toDate === 'function') return value;
  if (value instanceof Date && Number.isFinite(value.getTime())) {
    return Timestamp.fromDate(value);
  }
  const parsed = Date.parse(String(value));
  if (Number.isFinite(parsed)) {
    return Timestamp.fromDate(new Date(parsed));
  }
  return fallback;
}

function lookupDocId(address) {
  // ID de documento seguro a partir del correo (sin '/', sin caracteres raros).
  const email = normalizeEmail(address);
  return crypto.createHash('sha1').update(email).digest('hex');
}

export class FirestoreMailboxRepository {
  constructor({
    dbClient = db,
    companiesCollection = 'Negocios',
    corporateEmailsSub = 'corporateEmails',
    inboxSub = 'inbox',
    lookupCollection = 'mailboxLookup',
  } = {}) {
    this.db = dbClient;
    this.companiesCollection = companiesCollection;
    this.corporateEmailsSub = corporateEmailsSub;
    this.inboxSub = inboxSub;
    this.lookupCollection = lookupCollection;
  }

  lookupRef(address) {
    return this.db.collection(this.lookupCollection).doc(lookupDocId(address));
  }

  async getLookup(address) {
    const email = normalizeEmail(address);
    if (!email) return null;
    const snap = await this.lookupRef(email).get();
    if (!snap.exists) return null;
    const data = snap.data() || {};
    if (!data.empresaId || !data.correoId) return null;
    return { empresaId: cleanId(data.empresaId, 140), correoId: cleanId(data.correoId, 240) };
  }

  async putLookup({ address, empresaId, correoId }) {
    const email = normalizeEmail(address);
    if (!email || !empresaId || !correoId) return;
    await this.lookupRef(email).set(
      {
        address: email,
        empresaId: cleanId(empresaId, 140),
        correoId: cleanId(correoId, 240),
        updatedAt: Timestamp.now(),
      },
      { merge: true }
    );
  }

  /** Resuelve el correo corporativo por negocio + dirección, SIN queries (lectura directa). */
  async getCorporateEmailByNegocioAndAddress({ empresaId, address }) {
    const safeEmpresaId = cleanId(empresaId, 140);
    const email = normalizeEmail(address);
    const [localPart = '', domainPart = ''] = email.split('@');
    const correoId = buildCorporateEmailRecordId({
      alias: normalizeAlias(localPart),
      domain: normalizeDomain(domainPart),
    });
    if (!safeEmpresaId || !correoId) return null;
    const data = await this.getCorporateEmailById(safeEmpresaId, correoId);
    if (!data) return null;
    return { empresaId: safeEmpresaId, correoId, data };
  }

  companyRef(empresaId) {
    return this.db.collection(this.companiesCollection).doc(cleanId(empresaId, 140));
  }

  correoRef(empresaId, correoId) {
    return this.companyRef(empresaId)
      .collection(this.corporateEmailsSub)
      .doc(cleanId(correoId, 240));
  }

  inboxCol(empresaId, correoId) {
    return this.correoRef(empresaId, correoId).collection(this.inboxSub);
  }

  async findCorporateEmailByAddress(address) {
    // Sin queries ni índices: usa la tablita de lookup `mailboxLookup`.
    const lookup = await this.getLookup(address);
    if (!lookup) return null;
    const data = await this.getCorporateEmailById(lookup.empresaId, lookup.correoId);
    if (!data) return null;
    return { empresaId: lookup.empresaId, correoId: lookup.correoId, data };
  }

  async getCorporateEmailById(empresaId, correoId) {
    const snap = await this.correoRef(empresaId, correoId).get();
    if (!snap.exists) return null;
    return { id: snap.id, ...(snap.data() || {}) };
  }

  async setMailboxConfig({ empresaId, correoId, patch = {} }) {
    await this.correoRef(empresaId, correoId).set(
      { ...patch, updatedAt: Timestamp.now() },
      { merge: true }
    );
    return this.getCorporateEmailById(empresaId, correoId);
  }

  buildInboxMessageId(providerMessageId) {
    const clean = cleanId(providerMessageId, 200);
    if (clean) {
      return `in_${crypto.createHash('sha1').update(clean).digest('hex').slice(0, 24)}`;
    }
    return `in_${Date.now()}_${crypto.randomBytes(5).toString('hex')}`;
  }

  async saveInboundMessage({ empresaId, correoId, messageId, payload = {} }) {
    const ref = this.inboxCol(empresaId, correoId).doc(cleanId(messageId, 180));
    const existing = await ref.get();
    if (existing.exists) {
      // Idempotente: no duplicamos ni re-incrementamos el no-leído.
      return { id: existing.id, ...(existing.data() || {}), duplicate: true };
    }
    const now = Timestamp.now();
    const createdAt = toTimestamp(payload.createdAt || payload.date, now);
    const read = payload.read === true;
    const { createdAt: _createdAt, read: _read, ...safePayload } = payload;
    await ref.set({ ...safePayload, direction: 'inbound', read, createdAt }, { merge: true });
    if (!read) {
      await this.correoRef(empresaId, correoId).set(
        { unreadCount: FieldValue.increment(1), lastInboundAt: createdAt },
        { merge: true }
      );
    }
    const snap = await ref.get();
    return { id: snap.id, ...(snap.data() || {}) };
  }

  async listInbox({ empresaId, correoId, limit = 50 }) {
    const safeLimit = Math.max(1, Math.min(200, Number(limit) || 50));
    const snap = await this.inboxCol(empresaId, correoId)
      .orderBy('createdAt', 'desc')
      .limit(safeLimit)
      .get();
    return snap.docs.map((doc) => ({ id: doc.id, ...(doc.data() || {}) }));
  }

  async getInboxMessage({ empresaId, correoId, messageId }) {
    const snap = await this.inboxCol(empresaId, correoId).doc(cleanId(messageId, 180)).get();
    if (!snap.exists) return null;
    return { id: snap.id, ...(snap.data() || {}) };
  }

  async markInboxRead({ empresaId, correoId, messageId }) {
    const ref = this.inboxCol(empresaId, correoId).doc(cleanId(messageId, 180));
    const snap = await ref.get();
    if (!snap.exists) return null;
    const wasRead = (snap.data() || {}).read === true;
    await ref.set({ read: true, readAt: Timestamp.now() }, { merge: true });
    if (!wasRead) {
      await this.correoRef(empresaId, correoId).set(
        { unreadCount: FieldValue.increment(-1) },
        { merge: true }
      );
    }
    const updated = await ref.get();
    return { id: updated.id, ...(updated.data() || {}) };
  }
}
