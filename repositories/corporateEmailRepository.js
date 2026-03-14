import { FieldValue, Timestamp } from 'firebase-admin/firestore';
import { db } from '../firebaseAdmin.js';
import {
  buildCorporateEmailRecordId,
  normalizeAlias,
  normalizeDomain,
} from '../utils/corporateEmailUtils.js';

function cleanId(value = '', maxLength = 180) {
  return String(value ?? '').trim().slice(0, maxLength);
}

function repositoryError(message, code = 'REPOSITORY_ERROR') {
  const error = new Error(message);
  error.code = code;
  return error;
}

export class FirestoreCorporateEmailRepository {
  constructor({
    dbClient = db,
    companiesCollection = 'Negocios',
    subcollectionName = 'corporateEmails',
  } = {}) {
    this.db = dbClient;
    this.companiesCollection = companiesCollection;
    this.subcollectionName = subcollectionName;
  }

  getCompanyRef(empresaId) {
    const safeEmpresaId = cleanId(empresaId, 140);
    return this.db.collection(this.companiesCollection).doc(safeEmpresaId);
  }

  getCorporateEmailRef(empresaId, correoId) {
    const safeEmpresaId = cleanId(empresaId, 140);
    const safeCorreoId = cleanId(correoId, 240);
    return this.getCompanyRef(safeEmpresaId)
      .collection(this.subcollectionName)
      .doc(safeCorreoId);
  }

  async getCompanyById(empresaId) {
    const safeEmpresaId = cleanId(empresaId, 140);
    if (!safeEmpresaId) return null;
    const snap = await this.getCompanyRef(safeEmpresaId).get();
    if (!snap.exists) return null;
    return {
      id: snap.id,
      ...(snap.data() || {}),
    };
  }

  async listCorporateEmailsByCompany(empresaId) {
    const safeEmpresaId = cleanId(empresaId, 140);
    if (!safeEmpresaId) return [];
    const snap = await this.getCompanyRef(safeEmpresaId)
      .collection(this.subcollectionName)
      .orderBy('createdAt', 'desc')
      .get();

    return snap.docs.map((doc) => ({
      id: doc.id,
      ...(doc.data() || {}),
    }));
  }

  async getCorporateEmailById(empresaId, correoId) {
    const safeEmpresaId = cleanId(empresaId, 140);
    const safeCorreoId = cleanId(correoId, 240);
    if (!safeEmpresaId || !safeCorreoId) return null;
    const snap = await this.getCorporateEmailRef(safeEmpresaId, safeCorreoId).get();
    if (!snap.exists) return null;
    return {
      id: snap.id,
      ...(snap.data() || {}),
    };
  }

  async getCorporateEmailByAliasAndDomain({
    empresaId,
    alias,
    domain,
  }) {
    const safeEmpresaId = cleanId(empresaId, 140);
    const correoId = buildCorporateEmailRecordId({
      alias: normalizeAlias(alias),
      domain: normalizeDomain(domain),
    });
    if (!safeEmpresaId || !correoId) return null;
    return this.getCorporateEmailById(safeEmpresaId, correoId);
  }

  async createCorporateEmail({
    empresaId,
    correoId,
    payload = {},
  }) {
    const safeEmpresaId = cleanId(empresaId, 140);
    const safeCorreoId = cleanId(correoId, 240);
    if (!safeEmpresaId || !safeCorreoId) {
      throw repositoryError('empresaId y correoId son requeridos', 'INVALID_INPUT');
    }

    const companyRef = this.getCompanyRef(safeEmpresaId);
    const emailRef = this.getCorporateEmailRef(safeEmpresaId, safeCorreoId);

    await this.db.runTransaction(async (tx) => {
      const companySnap = await tx.get(companyRef);
      if (!companySnap.exists) {
        throw repositoryError('Empresa no encontrada', 'COMPANY_NOT_FOUND');
      }

      const existingSnap = await tx.get(emailRef);
      const existing = existingSnap.exists ? (existingSnap.data() || {}) : {};
      const currentStatus = String(existing?.status || '').trim().toLowerCase();
      if (existingSnap.exists && currentStatus !== 'deleted') {
        throw repositoryError('Alias ya existe', 'ALIAS_ALREADY_EXISTS');
      }

      const now = Timestamp.now();
      tx.set(
        emailRef,
        {
          ...payload,
          empresaId: safeEmpresaId,
          createdAt: now,
          updatedAt: now,
          deletedAt: FieldValue.delete(),
        },
        { merge: true }
      );
    });

    const created = await this.getCorporateEmailById(safeEmpresaId, safeCorreoId);
    return created;
  }

  async markCorporateEmailDeleted({
    empresaId,
    correoId,
    patch = {},
  }) {
    const safeEmpresaId = cleanId(empresaId, 140);
    const safeCorreoId = cleanId(correoId, 240);
    if (!safeEmpresaId || !safeCorreoId) {
      throw repositoryError('empresaId y correoId son requeridos', 'INVALID_INPUT');
    }

    const ref = this.getCorporateEmailRef(safeEmpresaId, safeCorreoId);
    const now = Timestamp.now();

    await ref.set(
      {
        ...patch,
        status: 'deleted',
        updatedAt: now,
        deletedAt: now,
      },
      { merge: true }
    );

    const updated = await this.getCorporateEmailById(safeEmpresaId, safeCorreoId);
    return updated;
  }
}

