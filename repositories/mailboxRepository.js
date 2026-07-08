// repositories/mailboxRepository.js
// Almacenamiento del "mini-mail": bandeja de entrada por correo corporativo y
// configuración del buzón (contraseña, reenvío-copia). Reutiliza la colección
// Negocios/{empresaId}/corporateEmails/{correoId} y le añade la subcolección `inbox`.
import { FieldValue, Timestamp } from 'firebase-admin/firestore';
import crypto from 'node:crypto';
import { db } from '../firebaseAdmin.js';

function cleanId(value = '', maxLength = 200) {
  return String(value ?? '').trim().slice(0, maxLength);
}

function normalizeEmail(value = '') {
  return String(value || '').trim().toLowerCase();
}

export class FirestoreMailboxRepository {
  constructor({
    dbClient = db,
    companiesCollection = 'Negocios',
    corporateEmailsSub = 'corporateEmails',
    inboxSub = 'inbox',
  } = {}) {
    this.db = dbClient;
    this.companiesCollection = companiesCollection;
    this.corporateEmailsSub = corporateEmailsSub;
    this.inboxSub = inboxSub;
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
    const email = normalizeEmail(address);
    if (!email) return null;
    const snap = await this.db
      .collectionGroup(this.corporateEmailsSub)
      .where('email', '==', email)
      .limit(1)
      .get();
    if (snap.empty) return null;
    const doc = snap.docs[0];
    const empresaId = doc.ref.parent.parent?.id || '';
    return { empresaId, correoId: doc.id, data: { id: doc.id, ...(doc.data() || {}) } };
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
    await ref.set({ ...payload, direction: 'inbound', read: false, createdAt: now }, { merge: true });
    await this.correoRef(empresaId, correoId).set(
      { unreadCount: FieldValue.increment(1), lastInboundAt: now },
      { merge: true }
    );
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
