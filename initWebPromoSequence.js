// initWebPromoSequence.js
// Script para crear/actualizar la secuencia WebPromo en Firebase
// Ejecutar con: node initWebPromoSequence.js

import { db } from './firebaseAdmin.js';

/**
 * Crea o actualiza la secuencia WebPromo en Firestore
 * Esta secuencia se activa cuando un lead llega desde campañas de Facebook Ads con #webPromo
 */
async function initWebPromoSequence() {
  console.log('🚀 Inicializando secuencia WebPromo...\n');

  const secuenciaWebPromo = {
    name: 'Secuencia Meta Ads - Web Promo',
    trigger: 'WebPromo',
    active: true,
    createdAt: new Date(),
    updatedAt: new Date(),
    description: 'Secuencia automática para leads que llegan desde campañas de Facebook Ads con código #webPromo',
    messages: [
      {
        type: 'texto',
        contenido: '¡Hola {{nombre}}! 👋 Gracias por tu interés en nuestros servicios web. Vi que vienes desde nuestra campaña de Facebook.',
        delay: 0 // Se envía inmediatamente
      },
      {
        type: 'texto',
        contenido: 'Te cuento que creamos sitios web profesionales y modernos para negocios como el tuyo. ¿Te gustaría conocer más sobre cómo podemos ayudarte? 🚀',
        delay: 2 // 2 minutos después del mensaje anterior
      },
      {
        type: 'texto',
        contenido: 'Tenemos paquetes desde páginas informativas hasta tiendas online completas. ¿Qué tipo de sitio web necesitas para tu negocio? 💼',
        delay: 5 // 5 minutos después del mensaje anterior (7 minutos desde el inicio)
      },
      {
        type: 'texto',
        contenido: 'Si quieres, puedo enviarte ejemplos de sitios que hemos creado para otros clientes. Solo responde "SÍ" y te comparto el portafolio 📱',
        delay: 10 // 10 minutos después del mensaje anterior (17 minutos desde el inicio)
      }
    ]
  };

  try {
    // Verificar si ya existe una secuencia con el trigger "WebPromo"
    const existingQuery = await db.collection('secuencias')
      .where('trigger', '==', 'WebPromo')
      .limit(1)
      .get();

    if (!existingQuery.empty) {
      // Actualizar secuencia existente
      const docId = existingQuery.docs[0].id;
      await db.collection('secuencias').doc(docId).update({
        ...secuenciaWebPromo,
        updatedAt: new Date()
      });
      console.log('✅ Secuencia WebPromo ACTUALIZADA exitosamente');
      console.log(`   📄 ID: ${docId}`);
    } else {
      // Crear nueva secuencia
      const docRef = await db.collection('secuencias').add(secuenciaWebPromo);
      console.log('✅ Secuencia WebPromo CREADA exitosamente');
      console.log(`   📄 ID: ${docRef.id}`);
    }

    console.log('\n📋 Detalles de la secuencia:');
    console.log(`   - Nombre: ${secuenciaWebPromo.name}`);
    console.log(`   - Trigger: ${secuenciaWebPromo.trigger}`);
    console.log(`   - Mensajes: ${secuenciaWebPromo.messages.length}`);
    console.log(`   - Estado: ${secuenciaWebPromo.active ? 'ACTIVA ✅' : 'INACTIVA ❌'}`);

    console.log('\n📱 Flujo de mensajes:');
    let totalMinutos = 0;
    secuenciaWebPromo.messages.forEach((msg, index) => {
      console.log(`   ${index + 1}. [${totalMinutos} min] ${msg.contenido.substring(0, 50)}...`);
      totalMinutos += msg.delay;
    });

    console.log('\n🎯 La secuencia se activará automáticamente cuando:');
    console.log('   1. Llegue un mensaje desde Facebook Ads (dominio @lid)');
    console.log('   2. El mensaje contenga el hashtag #webPromo (o variantes)');
    console.log('   3. O cuando defaultTriggerMetaAds esté configurado como "WebPromo"');

    console.log('\n✨ ¡Listo! La secuencia WebPromo está configurada y funcionando.\n');

  } catch (error) {
    console.error('❌ Error al inicializar la secuencia WebPromo:', error);
    throw error;
  }
}

// Ejecutar el script
initWebPromoSequence()
  .then(() => {
    console.log('🎉 Script ejecutado correctamente');
    process.exit(0);
  })
  .catch((error) => {
    console.error('💥 Error al ejecutar el script:', error);
    process.exit(1);
  });
