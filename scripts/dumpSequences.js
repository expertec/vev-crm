// scripts/dumpSequences.js
//
// Vuelca la colección `secuencias` (id + datos) en JSON, para revisar/corregir
// el copy. Usa una llave de servicio (GOOGLE_APPLICATION_CREDENTIALS o 1er arg).
//
//   node scripts/dumpSequences.js /ruta/serviceAccountKey.json
//
import fs from 'node:fs';
import admin from 'firebase-admin';

async function main() {
  const keyPath = process.argv[2] || process.env.GOOGLE_APPLICATION_CREDENTIALS || '';
  if (!keyPath || !fs.existsSync(keyPath)) {
    console.error('Falta la llave de servicio (argumento o GOOGLE_APPLICATION_CREDENTIALS).');
    process.exit(1);
  }
  const serviceAccount = JSON.parse(fs.readFileSync(keyPath, 'utf8'));
  const app = admin.initializeApp({ credential: admin.credential.cert(serviceAccount) }, 'dump-seq');
  const db = app.firestore();

  const snap = await db.collection('secuencias').get();
  const out = snap.docs.map((d) => ({ id: d.id, ...(d.data() || {}) }));
  process.stdout.write(JSON.stringify(out, null, 2) + '\n');
  console.error(`\n[OK] ${out.length} secuencias volcadas.`);
  process.exit(0);
}

main().catch((e) => { console.error('[dump] Error:', e?.message || e); process.exit(1); });
