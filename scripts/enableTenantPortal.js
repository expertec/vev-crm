// scripts/enableTenantPortal.js
//
// Habilita uno o más negocios en la colección `negociosapi`, que es el "gateway"
// que valida validateTenantHeaders() del portal de cliente. Sin este documento,
// cualquier ruta que pase por el BFF del portal (ej. Marketing / Meta Ads) falla
// con "El negocio no está habilitado en negociosapi".
//
// Uso:
//   node scripts/enableTenantPortal.js <negocioId> [negocioId2 ...]
//   node scripts/enableTenantPortal.js <negocioId> --scopes=cliente_portal,*
//   node scripts/enableTenantPortal.js <negocioId> --dry        (solo muestra, no escribe)
//   node scripts/enableTenantPortal.js <negocioId> --disable     (deshabilita en vez de habilitar)
//
// Credenciales: usa serviceAccountKey.json en el cwd, GOOGLE_APPLICATION_CREDENTIALS,
// /etc/secrets/serviceAccountKey.json o FIREBASE_SERVICE_ACCOUNT_JSON (igual que
// los demás scripts del CRM).
//
import fs from 'node:fs';
import path from 'node:path';
import dotenv from 'dotenv';
import admin from 'firebase-admin';

dotenv.config();

const COLLECTION = 'negociosapi';
const DEFAULT_SCOPES = ['cliente_portal'];

function parseFlag(flag) {
  return process.argv.includes(flag);
}

function readArgValue(prefix, fallback = '') {
  const entry = process.argv.find((item) => item.startsWith(prefix + '='));
  return entry ? entry.slice(prefix.length + 1).trim() : fallback;
}

function readServiceAccount() {
  const inlineJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  if (inlineJson && inlineJson.trim()) {
    return JSON.parse(inlineJson);
  }

  const candidates = [
    process.env.GOOGLE_APPLICATION_CREDENTIALS || '',
    path.join(process.cwd(), 'serviceAccountKey.json'),
    '/etc/secrets/serviceAccountKey.json',
  ].filter(Boolean);

  for (const candidate of candidates) {
    if (candidate && fs.existsSync(candidate)) {
      return JSON.parse(fs.readFileSync(candidate, 'utf8'));
    }
  }

  throw new Error(
    'No se encontró serviceAccountKey.json. Pásalo vía GOOGLE_APPLICATION_CREDENTIALS, ' +
      'FIREBASE_SERVICE_ACCOUNT_JSON, o colócalo en la carpeta del servidor.'
  );
}

async function main() {
  const dryRun = parseFlag('--dry');
  const disable = parseFlag('--disable');
  const scopes = readArgValue('--scopes', DEFAULT_SCOPES.join(','))
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);

  const negocioIds = process.argv
    .slice(2)
    .filter((arg) => !arg.startsWith('--'))
    .map((id) => String(id).trim())
    .filter(Boolean);

  if (negocioIds.length === 0) {
    console.error('Uso: node scripts/enableTenantPortal.js <negocioId> [negocioId2 ...] [--scopes=a,b] [--dry] [--disable]');
    process.exit(1);
  }

  const serviceAccount = readServiceAccount();
  const app = admin.initializeApp({ credential: admin.credential.cert(serviceAccount) }, 'enable-tenant-portal');
  const db = app.firestore();

  console.log(
    `[enable-tenant-portal] modo=${dryRun ? 'dry-run' : 'commit'} acción=${disable ? 'deshabilitar' : 'habilitar'} ` +
      `scopes=[${scopes.join(', ')}] negocios=${negocioIds.length}`
  );

  for (const negocioId of negocioIds) {
    // Sanity check: avisa si el negocio no existe en Negocios (no bloquea).
    const negocioSnap = await db.collection('Negocios').doc(negocioId).get();
    if (!negocioSnap.exists) {
      console.warn(`  ⚠️  ${negocioId}: no existe en la colección Negocios (¿ID correcto?). Continúo de todos modos.`);
    }

    const ref = db.collection(COLLECTION).doc(negocioId);
    const existing = await ref.get();

    const patch = {
      enabled: !disable,
      scopes,
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      source: 'enableTenantPortal',
    };
    if (!existing.exists) {
      patch.createdAt = admin.firestore.FieldValue.serverTimestamp();
    }

    if (dryRun) {
      console.log(`  • ${negocioId}: ${existing.exists ? 'actualizaría' : 'crearía'} negociosapi/${negocioId} -> enabled=${!disable}, scopes=[${scopes.join(', ')}]`);
      continue;
    }

    await ref.set(patch, { merge: true });
    console.log(`  ✓ ${negocioId}: ${existing.exists ? 'actualizado' : 'creado'} (enabled=${!disable})`);
  }

  console.log('[enable-tenant-portal] listo.');
  await app.delete();
}

main().catch((error) => {
  console.error('[enable-tenant-portal] Error:', error?.message || error);
  process.exit(1);
});
