// scripts/runBiReport.js
//
// Corre el informe BI desde la terminal usando una llave de servicio indicada
// por GOOGLE_APPLICATION_CREDENTIALS (o el primer argumento). Imprime el
// Markdown a stdout y lo guarda en bi-report.md.
//
// Uso:
//   GOOGLE_APPLICATION_CREDENTIALS=/ruta/serviceAccountKey.json \
//     node scripts/runBiReport.js
//   ó:  node scripts/runBiReport.js /ruta/serviceAccountKey.json
//
import fs from 'node:fs';
import path from 'node:path';
import admin from 'firebase-admin';
import { generateBiReport } from '../services/biReport.js';

async function main() {
  const keyPath = process.argv[2] || process.env.GOOGLE_APPLICATION_CREDENTIALS || '';
  if (!keyPath || !fs.existsSync(keyPath)) {
    console.error('Falta la llave de servicio. Pasa la ruta como argumento o en GOOGLE_APPLICATION_CREDENTIALS.');
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(keyPath, 'utf8'));
  const app = admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  }, 'bi-report');
  const db = app.firestore();

  const { markdown } = await generateBiReport({ dbOverride: db });
  const outPath = path.join(process.cwd(), 'bi-report.md');
  fs.writeFileSync(outPath, markdown, 'utf8');
  process.stdout.write(markdown + '\n');
  console.error(`\n[OK] Informe guardado en ${outPath}`);
  process.exit(0);
}

main().catch((error) => {
  console.error('[bi-report] Error:', error?.message || error);
  process.exit(1);
});
