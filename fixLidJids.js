// fixLidJids.js
// Script para corregir JIDs @lid en leads existentes
// Ejecutar con: node fixLidJids.js

import { db } from './firebaseAdmin.js';

/**
 * Normaliza número de teléfono para WhatsApp (México)
 */
function normalizePhoneForWA(phone) {
  let num = String(phone || '').replace(/\D/g, '');
  // 52 + 10 → forzar 521 + 10
  if (num.length === 12 && num.startsWith('52') && !num.startsWith('521')) {
    return '521' + num.slice(2);
  }
  // 10 → 521 + 10
  if (num.length === 10) return '521' + num;
  // si ya viene 521…, dejarlo
  return num;
}

/**
 * Corrige JIDs @lid en la base de datos
 */
async function fixLidJids() {
  console.log('🔍 Buscando leads con JID @lid...\n');

  try {
    // Obtener todos los leads (en producción, hacer esto por lotes)
    const snapshot = await db.collection('leads').get();

    if (snapshot.empty) {
      console.log('✅ No hay leads en la base de datos.');
      return 0;
    }

    console.log(`📊 Total de leads encontrados: ${snapshot.size}\n`);

    let fixedCount = 0;
    let skippedCount = 0;
    let errorCount = 0;

    for (const doc of snapshot.docs) {
      const leadId = doc.id;
      const data = doc.data();
      const currentJid = data.jid;
      const telefono = data.telefono;

      // Verificar si el JID actual contiene @lid
      if (currentJid && currentJid.includes('@lid')) {
        console.log(`⚠️  Lead con JID @lid detectado:`);
        console.log(`   ID: ${leadId}`);
        console.log(`   JID actual: ${currentJid}`);
        console.log(`   Teléfono: ${telefono || 'N/A'}`);

        try {
          let newJid = null;

          // Opción 1: Intentar extraer número del JID @lid
          if (currentJid.includes('@lid')) {
            const phoneDigits = currentJid.replace('@lid', '').replace(/\D/g, '');
            if (phoneDigits.length >= 10) {
              const normalized = normalizePhoneForWA(phoneDigits);
              newJid = `${normalized}@s.whatsapp.net`;
              console.log(`   ✅ JID extraído del @lid: ${newJid}`);
            }
          }

          // Opción 2: Usar el campo telefono
          if (!newJid && telefono) {
            const normalized = normalizePhoneForWA(telefono);
            newJid = `${normalized}@s.whatsapp.net`;
            console.log(`   ✅ JID construido desde teléfono: ${newJid}`);
          }

          // Opción 3: Usar el leadId si es un número válido
          if (!newJid && leadId.includes('@s.whatsapp.net')) {
            newJid = leadId;
            console.log(`   ✅ Usando leadId como JID: ${newJid}`);
          }

          if (newJid) {
            // Actualizar el JID en Firebase
            await doc.ref.update({
              jid: newJid,
              jidFixedAt: new Date(),
              previousJid: currentJid
            });

            console.log(`   💾 JID actualizado correctamente\n`);
            fixedCount++;
          } else {
            console.warn(`   ❌ No se pudo construir un JID válido para este lead\n`);
            errorCount++;
          }
        } catch (error) {
          console.error(`   ❌ Error al actualizar lead ${leadId}:`, error.message);
          errorCount++;
        }
      } else if (!currentJid) {
        // Lead sin JID - construir desde teléfono
        if (telefono) {
          try {
            const normalized = normalizePhoneForWA(telefono);
            const newJid = `${normalized}@s.whatsapp.net`;

            await doc.ref.update({
              jid: newJid,
              jidFixedAt: new Date()
            });

            console.log(`✅ JID agregado a lead sin JID: ${leadId} → ${newJid}`);
            fixedCount++;
          } catch (error) {
            console.error(`❌ Error al agregar JID a ${leadId}:`, error.message);
            errorCount++;
          }
        } else {
          skippedCount++;
        }
      } else {
        // JID válido, no hacer nada
        skippedCount++;
      }
    }

    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('📊 RESUMEN:');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log(`✅ Leads corregidos:  ${fixedCount}`);
    console.log(`⏭️  Leads sin cambios:  ${skippedCount}`);
    console.log(`❌ Errores:           ${errorCount}`);
    console.log(`📈 Total procesados:  ${snapshot.size}`);
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

    if (fixedCount > 0) {
      console.log('✨ ¡Los JIDs @lid han sido corregidos exitosamente!');
      console.log('   Las secuencias ahora se enviarán a los números reales.\n');
    }

    return fixedCount;
  } catch (error) {
    console.error('💥 Error general al procesar leads:', error);
    throw error;
  }
}

// Ejecutar el script
fixLidJids()
  .then((count) => {
    console.log(`\n🎉 Script completado. ${count} leads fueron corregidos.`);
    process.exit(0);
  })
  .catch((error) => {
    console.error('\n💥 Error al ejecutar el script:', error);
    process.exit(1);
  });
