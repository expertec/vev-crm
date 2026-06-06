// scripts/applySequenceFixes.js
//
// Aplica correcciones puntuales y verificadas al copy de la colección `secuencias`.
// Cada fix comprueba el texto actual antes de cambiarlo (no pisa lo inesperado)
// y es idempotente (si ya está corregido, lo salta).
//
//   node scripts/applySequenceFixes.js /ruta/serviceAccountKey.json [--dry]
//
import fs from 'node:fs';
import admin from 'firebase-admin';

const SOFT_PASO10 = '{{nombre}}, no quiero presionarte. El precio de $990 es de esta semana; si arrancas ahora yo personalmente preparo tu estrategia y te acompaño en todo el proceso. Te aparto el lugar.';
const LINKABIERTO_P1 = '{{nombre}}, ¿qué te pareció tu página? 😊 Para que lo veas en números: cuesta $990 al año = $82 al mes, menos que una recarga, y está abierta 24/7, no cierra domingos ni pide vacaciones.';

// docId -> función que recibe el array messages y devuelve { changed, notes }
const FIXES = [
  {
    docId: 'SinInteraccion',
    label: 'SinInteraccion · precio $790 → $990',
    apply(messages) {
      const notes = [];
      const m = messages[0];
      if (m && typeof m.contenido === 'string' && m.contenido.includes('$790')) {
        m.contenido = m.contenido.replace(/\$790/g, '$990');
        notes.push('paso 1: $790 → $990');
      }
      return { changed: notes.length > 0, notes };
    },
  },
  {
    docId: '4oO37oWJdLHifbCrqqVR', // PlanRedes
    label: 'PlanRedes · quitar línea duplicada (p5) + suavizar p10',
    apply(messages) {
      const notes = [];
      const m5 = messages[4];
      if (m5 && typeof m5.contenido === 'string') {
        const before = m5.contenido;
        m5.contenido = m5.contenido.replace(
          /(¿Agendamos llamada o iniciamos el proceso\?)(\s*\n\s*¿Agendamos llamada o iniciamos el proceso\?)+/g,
          '$1'
        );
        if (m5.contenido !== before) notes.push('paso 5: línea duplicada eliminada');
      }
      const m10 = messages[9];
      if (m10 && typeof m10.contenido === 'string' && /ÚLTIMO AVISO/i.test(m10.contenido)) {
        m10.contenido = SOFT_PASO10;
        notes.push('paso 10: suavizado (garantía personal)');
      }
      return { changed: notes.length > 0, notes };
    },
  },
  {
    docId: 'interesados',
    label: 'interesados · typo + manda el link directo',
    apply(messages) {
      const notes = [];
      const m = messages[0];
      if (m && typeof m.contenido === 'string') {
        const before = m.contenido;
        m.contenido = m.contenido.replace(/generamos\s+un\s+tu\s+muestra/gi, 'generamos tu muestra');
        if (m.contenido !== before) notes.push('paso 1: typo "un tu muestra" corregido');
        if (m.contenido.includes('¿Te mando el link para que lo llenes ahora mismo?')) {
          m.contenido = m.contenido.replace(
            '¿Te mando el link para que lo llenes ahora mismo?',
            'Aquí te dejo el link para que lo llenes ahora mismo: ${linkMuestra}'
          );
          notes.push('paso 1: ahora manda el link directo (${linkMuestra})');
        }
      }
      return { changed: notes.length > 0, notes };
    },
  },
  {
    docId: 'k7Z7mJFnjbBMzpSaxMut', // LinkAbierto
    label: 'LinkAbierto · subir el argumento $82/mes al paso 1',
    apply(messages) {
      const notes = [];
      const m = messages[0];
      if (m && typeof m.contenido === 'string' && m.contenido.trim().startsWith('¿Qué te pareció?')) {
        m.contenido = LINKABIERTO_P1;
        notes.push('paso 1: reescrito con el argumento $82/mes');
      }
      return { changed: notes.length > 0, notes };
    },
  },
];

async function main() {
  const keyPath = process.argv[2] || process.env.GOOGLE_APPLICATION_CREDENTIALS || '';
  const dry = process.argv.includes('--dry');
  if (!keyPath || !fs.existsSync(keyPath)) {
    console.error('Falta la llave de servicio (argumento o GOOGLE_APPLICATION_CREDENTIALS).');
    process.exit(1);
  }
  const serviceAccount = JSON.parse(fs.readFileSync(keyPath, 'utf8'));
  const app = admin.initializeApp({ credential: admin.credential.cert(serviceAccount) }, 'apply-fixes');
  const db = app.firestore();

  for (const fix of FIXES) {
    const ref = db.collection('secuencias').doc(fix.docId);
    const snap = await ref.get();
    if (!snap.exists) {
      console.log(`SKIP  ${fix.label} → doc ${fix.docId} no existe`);
      continue;
    }
    const data = snap.data() || {};
    const messages = Array.isArray(data.messages) ? JSON.parse(JSON.stringify(data.messages)) : [];
    const { changed, notes } = fix.apply(messages);
    if (!changed) {
      console.log(`OK    ${fix.label} → sin cambios (ya estaba o no coincidió)`);
      continue;
    }
    if (dry) {
      console.log(`DRY   ${fix.label} → ${notes.join('; ')}`);
    } else {
      await ref.set({ messages }, { merge: true });
      console.log(`APLIC ${fix.label} → ${notes.join('; ')}`);
    }
  }

  console.log(`\n[${dry ? 'DRY-RUN' : 'HECHO'}]`);
  process.exit(0);
}

main().catch((e) => { console.error('[apply] Error:', e?.message || e); process.exit(1); });
