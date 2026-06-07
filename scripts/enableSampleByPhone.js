// scripts/enableSampleByPhone.js
//
// Habilita la muestra (sampleFlow) de los leads cuyo teléfono coincide (últimos
// 10 dígitos) con el número dado. Desbloqueo inmediato mientras se despliega el
// fix de auto-habilitación.
//
//   node scripts/enableSampleByPhone.js /ruta/key.json 5214776097721 [días]
//
import fs from 'node:fs';
import admin from 'firebase-admin';

async function main() {
  const keyPath = process.argv[2];
  const phoneArg = String(process.argv[3] || '').replace(/\D/g, '');
  const days = Math.max(1, Number(process.argv[4] || 14));
  if (!keyPath || !fs.existsSync(keyPath) || !phoneArg) {
    console.error('Uso: node scripts/enableSampleByPhone.js <key.json> <telefono> [días]');
    process.exit(1);
  }
  const last10 = phoneArg.slice(-10);
  const serviceAccount = JSON.parse(fs.readFileSync(keyPath, 'utf8'));
  const app = admin.initializeApp({ credential: admin.credential.cert(serviceAccount) }, 'enable-sample');
  const db = app.firestore();

  const snap = await db.collection('leads').get();
  const now = new Date();
  const expiry = new Date(now.getTime() + days * 24 * 60 * 60 * 1000);
  let count = 0;
  for (const doc of snap.docs) {
    const d = doc.data() || {};
    const tel = String(d.telefono || '').replace(/\D/g, '');
    const jid = String(d.resolvedJid || d.jid || '');
    const jidDigits = (jid.match(/(\d{10,15})@/) || [])[1] || '';
    const matches = tel.slice(-10) === last10 || jidDigits.slice(-10) === last10;
    if (!matches) continue;
    await doc.ref.set({
      sampleFlow: {
        ...(d.sampleFlow && typeof d.sampleFlow === 'object' ? d.sampleFlow : {}),
        enabled: true,
        phone: phoneArg,
        expiresAt: expiry,
        autoEnabledAt: now,
        source: (d.sampleFlow && d.sampleFlow.source) || 'manual_unblock',
      },
    }, { merge: true });
    count += 1;
    console.log(`habilitado: ${doc.id} (telefono=${tel})`);
  }
  console.log(`\n[HECHO] ${count} lead(s) habilitado(s). Expira: ${expiry.toISOString()}`);
  process.exit(0);
}

main().catch((e) => { console.error('[enable] Error:', e?.message || e); process.exit(1); });
