import { FieldValue, Timestamp } from 'firebase-admin/firestore';
import crypto from 'node:crypto';
import { db } from '../firebaseAdmin.js';
import {
  buildCorporateEmailRecordId,
  normalizeEmailAddress,
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

function buildDestinationRecordId(email = '') {
  const safeEmail = normalizeEmailAddress(email);
  if (!safeEmail) return '';
  const hash = crypto.createHash('sha1').update(safeEmail).digest('hex');
  return `dest_${hash}`;
}

export class FirestoreCorporateEmailRepository {
  constructor({
    dbClient = db,
    companiesCollection = 'Negocios',
    subcollectionName = 'corporateEmails',
    destinationSubcollectionName = 'corporateEmailDestinations',
  } = {}) {
    this.db = dbClient;
    this.companiesCollection = companiesCollection;
    this.subcollectionName = subcollectionName;
    this.destinationSubcollectionName = destinationSubcollectionName;
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

  getDestinationRefByDocId(empresaId, destinationId) {
    const safeEmpresaId = cleanId(empresaId, 140);
    const safeDestinationId = cleanId(destinationId, 260);
    return this.getCompanyRef(safeEmpresaId)
      .collection(this.destinationSubcollectionName)
      .doc(safeDestinationId);
  }

  getDestinationRefByEmail(empresaId, email) {
    const safeEmpresaId = cleanId(empresaId, 140);
    const destinationId = buildDestinationRecordId(email);
    return this.getDestinationRefByDocId(safeEmpresaId, destinationId);
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

  async listCorporateEmailDestinationsByCompany(empresaId) {
    const safeEmpresaId = cleanId(empresaId, 140);
    if (!safeEmpresaId) return [];
    const snap = await this.getCompanyRef(safeEmpresaId)
      .collection(this.destinationSubcollectionName)
      .orderBy('updatedAt', 'desc')
      .get();

    return snap.docs.map((doc) => ({
      id: doc.id,
      ...(doc.data() || {}),
    }));
  }

  async getCorporateEmailDestinationByEmail(empresaId, destinationEmail) {
    const safeEmpresaId = cleanId(empresaId, 140);
    const safeEmail = normalizeEmailAddress(destinationEmail);
    if (!safeEmpresaId || !safeEmail) return null;
    const snap = await this.getDestinationRefByEmail(safeEmpresaId, safeEmail).get();
    if (!snap.exists) return null;
    return {
      id: snap.id,
      ...(snap.data() || {}),
    };
  }

  async upsertCorporateEmailDestination({
    empresaId,
    destinationEmail,
    payload = {},
  }) {
    const safeEmpresaId = cleanId(empresaId, 140);
    const safeDestinationEmail = normalizeEmailAddress(destinationEmail);
    const destinationId = buildDestinationRecordId(safeDestinationEmail);
    if (!safeEmpresaId || !safeDestinationEmail || !destinationId) {
      throw repositoryError('empresaId y destinationEmail son requeridos', 'INVALID_INPUT');
    }

    const companyRef = this.getCompanyRef(safeEmpresaId);
    const destinationRef = this.getDestinationRefByDocId(safeEmpresaId, destinationId);

    await this.db.runTransaction(async (tx) => {
      const companySnap = await tx.get(companyRef);
      if (!companySnap.exists) {
        throw repositoryError('Empresa no encontrada', 'COMPANY_NOT_FOUND');
      }

      const destinationSnap = await tx.get(destinationRef);
      const destinationData = destinationSnap.exists ? (destinationSnap.data() || {}) : {};
      const now = Timestamp.now();
      tx.set(
        destinationRef,
        {
          ...payload,
          empresaId: safeEmpresaId,
          destinationEmail: safeDestinationEmail,
          email: safeDestinationEmail,
          createdAt: destinationData.createdAt || now,
          updatedAt: now,
        },
        { merge: true }
      );
    });

    const updated = await this.getCorporateEmailDestinationByEmail(
      safeEmpresaId,
      safeDestinationEmail
    );
    return updated;
  }
}
