import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import dotenv from 'dotenv';
import admin from 'firebase-admin';
import { parsePhoneNumberFromString } from 'libphonenumber-js';

dotenv.config();

const DEFAULT_META_CUSTOMER_MARKERS = [
  'cliente',
  'clientes',
  'customer',
  'customers',
  'ganado',
  'ganada',
  'cerrado_ganado',
  'closed_won',
  'compro',
  'comprado',
  'compra_confirmada',
];

function normalizeMetaKey(value = '') {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function splitCsvEnv(value = '', fallback = []) {
  const items = String(value || '')
    .split(/[;,\n]+/)
    .map((item) => normalizeMetaKey(item))
    .filter(Boolean);
  const base = items.length > 0 ? items : fallback.map((item) => normalizeMetaKey(item)).filter(Boolean);
  return new Set(base);
}

function normalizeMetaOptionalEventName(value = '') {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const normalized = raw.toLowerCase();
  if (['0', 'false', 'off', 'none', 'disabled'].includes(normalized)) return '';
  return raw;
}

function parseBooleanArg(flag) {
  return process.argv.includes(flag);
}

function readArgValue(prefix, fallback = '') {
  const entry = process.argv.find((item) => item.startsWith(prefix + '='));
  if (!entry) return fallback;
  return entry.slice(prefix.length + 1).trim();
}

function hashSha256(value = '') {
  return crypto.createHash('sha256').update(String(value)).digest('hex');
}

function toE164(num, defaultCountry = 'MX') {
  const raw = String(num || '').replace(/\D/g, '');
  const parsed = parsePhoneNumberFromString(raw, defaultCountry);
  if (parsed && parsed.isValid()) return parsed.number;
  if (/^\d{10}$/.test(raw)) return `+52${raw}`;
  if (/^\d{11,15}$/.test(raw) && raw.startsWith('521')) return `+${raw}`;
  if (/^\d{11,15}$/.test(raw) && raw.startsWith('52')) return `+${raw}`;
  return raw ? `+${raw}` : '';
}

function normalizePhoneDigits(value = '') {
  return String(value || '').replace(/\D/g, '');
}

function expandPhoneCandidates(value = '') {
  const digits = normalizePhoneDigits(value);
  if (!digits) return [];

  const set = new Set([digits]);
  if (/^\d{10}$/.test(digits)) {
    set.add(`52${digits}`);
    set.add(`521${digits}`);
  } else if (/^52\d{10}$/.test(digits) && !digits.startsWith('521')) {
    const tail = digits.slice(2);
    set.add(tail);
    set.add(`521${tail}`);
  } else if (/^521\d{10}$/.test(digits)) {
    const tail = digits.slice(3);
    set.add(tail);
    set.add(`52${tail}`);
  }

  return Array.from(set);
}

function normalizeMetaPhone(value = '') {
  if (!value) return '';
  return normalizePhoneDigits(toE164(value) || value);
}

function normalizeMetaEmail(value = '') {
  return String(value || '').trim().toLowerCase();
}

function splitPersonName(fullName = '') {
  const parts = String(fullName || '').trim().replace(/\s+/g, ' ').split(' ').filter(Boolean);
  if (parts.length === 0) return { first: '', last: '' };
  return {
    first: parts[0] || '',
    last: parts.slice(1).join(' '),
  };
}

function buildMetaUserData({ leadId = '', phone = '', email = '', name = '' } = {}) {
  const userData = {};
  const normalizedPhone = normalizeMetaPhone(phone);
  const normalizedEmail = normalizeMetaEmail(email);
  const { first, last } = splitPersonName(name);

  if (normalizedPhone) userData.ph = [hashSha256(normalizedPhone)];
  if (normalizedEmail) userData.em = [hashSha256(normalizedEmail)];
  if (first) userData.fn = hashSha256(first.trim().toLowerCase());
  if (last) userData.ln = hashSha256(last.trim().toLowerCase());

  const externalIdSeed = String(leadId || normalizedPhone || normalizedEmail).trim();
  if (externalIdSeed) userData.external_id = hashSha256(externalIdSeed);

  return userData;
}

function hasMetaUserData(userData = {}) {
  return Boolean(
    userData?.external_id
      || (Array.isArray(userData?.ph) && userData.ph.length > 0)
      || (Array.isArray(userData?.em) && userData.em.length > 0)
  );
}

function buildConfig() {
  const datasetId = String(
    process.env.META_CAPI_DATASET_ID
      || process.env.META_DATASET_ID
      || process.env.META_PIXEL_ID
      || ''
  ).trim();
  const accessToken = String(
    process.env.META_CAPI_ACCESS_TOKEN
      || process.env.META_ACCESS_TOKEN
      || ''
  ).trim();

  return {
    enabled: ['1', 'true', 'yes', 'on'].includes(String(process.env.META_CAPI_ENABLED || '1').trim().toLowerCase()),
    datasetId,
    accessToken,
    graphVersion: String(process.env.META_CAPI_GRAPH_VERSION || 'v21.0').trim() || 'v21.0',
    actionSource: String(process.env.META_CAPI_ACTION_SOURCE || 'system_generated').trim() || 'system_generated',
    customerEventName: String(process.env.META_CAPI_CUSTOMER_EVENT_NAME || 'CRMCustomer').trim() || 'CRMCustomer',
    customerMirrorEventName: normalizeMetaOptionalEventName(
      process.env.META_CAPI_CUSTOMER_STANDARD_EVENT_NAME || 'CompleteRegistration'
    ),
    testEventCode: String(process.env.META_CAPI_TEST_EVENT_CODE || '').trim(),
    customerMarkers: splitCsvEnv(process.env.META_CAPI_CUSTOMER_MARKERS, DEFAULT_META_CUSTOMER_MARKERS),
  };
}

function shouldTrackMetaCustomer({ status = '', stageName = '', stageKey = '' } = {}, customerMarkers) {
  const candidates = [status, stageName, stageKey]
    .map((item) => normalizeMetaKey(item))
    .filter(Boolean);
  return candidates.some((candidate) => customerMarkers.has(candidate));
}

function readServiceAccount() {
  const candidatePaths = [
    '/etc/secrets/serviceAccountKey.json',
    process.env.GOOGLE_APPLICATION_CREDENTIALS || '',
    path.join(process.cwd(), 'serviceAccountKey.json'),
  ].filter(Boolean);

  for (const candidate of candidatePaths) {
    if (fs.existsSync(candidate)) {
      return JSON.parse(fs.readFileSync(candidate, 'utf8'));
    }
  }

  if (process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
    return JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);
  }

  throw new Error('No se encontro serviceAccountKey.json ni FIREBASE_SERVICE_ACCOUNT_JSON.');
}

function getDb() {
  if (!admin.apps.length) {
    const serviceAccount = readServiceAccount();
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      storageBucket: 'merkagrama-crm.firebasestorage.app',
    });
  }
  return admin.firestore();
}

async function resolveNegocioByIdentity(db, { negocioId = '', leadId = '', phoneDigits = '' } = {}) {
  const negociosCol = db.collection('Negocios');
  const requestedNegocioId = String(negocioId || '').trim();
  const finalLeadId = String(leadId || '').trim();
  const phoneCandidates = expandPhoneCandidates(phoneDigits);

  let negocioSnap = null;

  if (requestedNegocioId) {
    const byId = await negociosCol.doc(requestedNegocioId).get();
    if (byId.exists) negocioSnap = byId;
  }

  if (!negocioSnap && finalLeadId) {
    const byLeadId = await negociosCol.where('leadId', '==', finalLeadId).limit(1).get();
    if (!byLeadId.empty) negocioSnap = byLeadId.docs[0];
  }

  if (!negocioSnap) {
    for (const candidate of phoneCandidates) {
      const byLeadPhone = await negociosCol.where('leadPhone', '==', candidate).limit(1).get();
      if (!byLeadPhone.empty) {
        negocioSnap = byLeadPhone.docs[0];
        break;
      }
    }
  }

  return {
    negocioId: negocioSnap?.id || '',
    negocioData: negocioSnap?.data?.() || null,
  };
}

async function postMetaEvent(config, {
  eventName = '',
  eventId = '',
  eventTime = Math.floor(Date.now() / 1000),
  userData = {},
  customData = {},
} = {}) {
  if (!config.enabled || !config.datasetId || !config.accessToken) {
    return { ok: false, skipped: true, reason: 'not-configured' };
  }
  if (!eventName) {
    return { ok: false, skipped: true, reason: 'missing-event-name' };
  }
  if (!hasMetaUserData(userData)) {
    return { ok: false, skipped: true, reason: 'missing-user-data' };
  }

  const payload = {
    data: [
      {
        event_name: eventName,
        event_time: eventTime,
        event_id: eventId,
        action_source: config.actionSource,
        user_data: userData,
        custom_data: customData,
      },
    ],
  };

  if (config.testEventCode) {
    payload.test_event_code = config.testEventCode;
  }

  const url = new URL(`https://graph.facebook.com/${config.graphVersion}/${config.datasetId}/events`);
  url.searchParams.set('access_token', config.accessToken);

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const detail = String(body?.error?.message || body?.message || '').trim();
    throw new Error(detail || `Meta Conversions API HTTP ${response.status}`);
  }

  return { ok: true, body };
}

async function main() {
  const config = buildConfig();
  const commit = parseBooleanArg('--commit');
  const limitArg = Number(readArgValue('--limit', '0'));
  const limit = Number.isFinite(limitArg) && limitArg > 0 ? Math.floor(limitArg) : 0;
  const db = getDb();

  if (!config.enabled || !config.datasetId || !config.accessToken) {
    throw new Error('Falta configurar META_CAPI_ENABLED/META_CAPI_DATASET_ID/META_CAPI_ACCESS_TOKEN.');
  }

  const snapshot = await db.collection('leads').get();
  const candidates = [];

  for (const docSnap of snapshot.docs) {
    const data = docSnap.data() || {};
    const customerMeta = data?.metaConversions?.customer && typeof data.metaConversions.customer === 'object'
      ? data.metaConversions.customer
      : {};
    const primaryAlreadySent = Boolean(customerMeta?.sentAt);
    const mirrorConfigured = Boolean(config.customerMirrorEventName);
    const mirrorAlreadySent = mirrorConfigured ? Boolean(customerMeta?.mirrorSentAt) : true;

    if (primaryAlreadySent && mirrorAlreadySent) continue;

    const status = String(data.estado || '').trim();
    const stageName = String(data.etapaNombre || '').trim();
    const stageKey = String(data.etapa || '').trim();
    if (!shouldTrackMetaCustomer({ status, stageName, stageKey }, config.customerMarkers)) continue;

    const phoneDigits = normalizePhoneDigits(data.telefono || docSnap.id.split('@')[0] || '');
    const negocioCtx = await resolveNegocioByIdentity(db, {
      leadId: docSnap.id,
      phoneDigits,
    });

    const displayName = String(data.nombre || negocioCtx.negocioData?.companyInfo || '').trim();
    const email = String(negocioCtx.negocioData?.contactEmail || '').trim();
    const userData = buildMetaUserData({
      leadId: docSnap.id,
      phone: phoneDigits,
      email,
      name: displayName,
    });
    if (!hasMetaUserData(userData)) continue;

    candidates.push({
      id: docSnap.id,
      ref: docSnap.ref,
      data,
      status,
      stageName,
      stageKey,
      phoneDigits,
      negocioId: negocioCtx.negocioId,
      userData,
      primaryAlreadySent,
      mirrorAlreadySent,
    });
  }

  const targetItems = limit > 0 ? candidates.slice(0, limit) : candidates;

  console.log(`[backfill-meta-customers] modo=${commit ? 'commit' : 'dry-run'} total_candidatos=${candidates.length} procesar=${targetItems.length}`);
  if (!commit) {
    targetItems.slice(0, 20).forEach((item, index) => {
      console.log(`[dry-run ${index + 1}] lead=${item.id} status=${item.status || '-'} stage=${item.stageKey || item.stageName || '-'} negocio=${item.negocioId || '-'} primarySent=${item.primaryAlreadySent} mirrorSent=${item.mirrorAlreadySent}`);
    });
    if (targetItems.length > 20) {
      console.log(`[backfill-meta-customers] mostrando solo primeros 20 de ${targetItems.length}`);
    }
    return;
  }

  let sentPrimary = 0;
  let sentMirror = 0;
  let skipped = 0;
  let failed = 0;

  for (const item of targetItems) {
    try {
      const customerMeta = item.data?.metaConversions?.customer && typeof item.data.metaConversions.customer === 'object'
        ? item.data.metaConversions.customer
        : {};
      const sendPrimary = !Boolean(customerMeta?.sentAt);
      const sendMirror = Boolean(config.customerMirrorEventName) && !Boolean(customerMeta?.mirrorSentAt);
      if (!sendPrimary && !sendMirror) {
        skipped += 1;
        console.log(`[skip] lead=${item.id} reason=already-sent`);
        continue;
      }

      const eventSeed = normalizeMetaKey(item.id || item.phoneDigits || 'lead');
      const eventTime = Math.floor(Date.now() / 1000);
      const baseCustomData = {
        source: 'historical-backfill',
        lead_id: item.id,
        lead_status: item.status,
        stage_key: item.stageKey,
        stage_name: item.stageName,
        negocio_id: item.negocioId,
        channel: 'whatsapp_crm',
      };

      const patch = {
        metaConversions: {
          customer: {
            lastAttemptAt: admin.firestore.Timestamp.now(),
            lastError: '',
            source: 'historical-backfill',
            status: item.status,
            stageKey: item.stageKey,
            stageName: item.stageName,
          },
        },
      };

      if (sendPrimary) {
        const eventId = `crm_customer:${eventSeed}:${eventTime}`;
        const result = await postMetaEvent(config, {
          eventName: config.customerEventName,
          eventId,
          eventTime,
          userData: item.userData,
          customData: baseCustomData,
        });
        patch.metaConversions.customer.sentAt = admin.firestore.Timestamp.now();
        patch.metaConversions.customer.eventId = eventId;
        patch.metaConversions.customer.eventName = config.customerEventName;
        patch.metaConversions.customer.fbtraceId = String(result?.body?.fbtrace_id || '').trim();
        sentPrimary += 1;
      }

      if (sendMirror) {
        const mirrorEventId = `crm_customer_alias:${eventSeed}:${eventTime}`;
        const result = await postMetaEvent(config, {
          eventName: config.customerMirrorEventName,
          eventId: mirrorEventId,
          eventTime,
          userData: item.userData,
          customData: baseCustomData,
        });
        patch.metaConversions.customer.mirrorSentAt = admin.firestore.Timestamp.now();
        patch.metaConversions.customer.mirrorEventId = mirrorEventId;
        patch.metaConversions.customer.mirrorEventName = config.customerMirrorEventName;
        patch.metaConversions.customer.mirrorFbtraceId = String(result?.body?.fbtrace_id || '').trim();
        sentMirror += 1;
      }

      await item.ref.set(patch, { merge: true });
      console.log(`[sent] lead=${item.id} primary=${sendPrimary ? config.customerEventName : 'no'} mirror=${sendMirror ? config.customerMirrorEventName : 'no'}`);
    } catch (error) {
      failed += 1;
      await item.ref.set({
        metaConversions: {
          customer: {
            lastAttemptAt: admin.firestore.Timestamp.now(),
            lastError: String(error?.message || error),
            source: 'historical-backfill',
          },
        },
      }, { merge: true }).catch(() => {});
      console.error(`[error] lead=${item.id} message=${error?.message || error}`);
    }
  }

  console.log(`[backfill-meta-customers] done primary=${sentPrimary} mirror=${sentMirror} skipped=${skipped} failed=${failed}`);
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error('[backfill-meta-customers] fatal:', error?.message || error);
    process.exit(1);
  });
