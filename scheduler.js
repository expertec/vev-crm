// src/server/scheduler.js
import { db } from './firebaseAdmin.js';
import { getWhatsAppSock } from './whatsappService.js';
import admin from 'firebase-admin';
import { Configuration, OpenAIApi } from 'openai';

const { FieldValue } = admin.firestore;

// Asegúrate de que la API key esté definida
if (!process.env.OPENAI_API_KEY) {
  throw new Error("Falta la variable de entorno OPENAI_API_KEY");
}

// Configuración de OpenAI
const configuration = new Configuration({
  apiKey: process.env.OPENAI_API_KEY,
});
const openai = new OpenAIApi(configuration);

/**
 * Reemplaza placeholders en plantillas de texto.
 * {{campo}} se sustituye por leadData.campo si existe.
 */
function replacePlaceholders(template, leadData) {
  return template.replace(/\{\{(\w+)\}\}/g, (_, field) => {
    const value = leadData[field] || '';
    if (field === 'nombre') {
      // devolver sólo la primera palabra del nombre completo
      return value.split(' ')[0] || '';
    }
    return value;
  });
}

/**
 * Envía un mensaje de WhatsApp según su tipo.
 * Usa exactamente el número que viene en lead.telefono (sin anteponer country code).
 */
async function enviarMensaje(lead, mensaje) {
  try {
    const sock = getWhatsAppSock();
    if (!sock) return;

    const phone = (lead.telefono || '').replace(/\D/g, '');
    const jid = `${phone}@s.whatsapp.net`;

    switch (mensaje.type) {
      case 'texto': {
        const text = replacePlaceholders(mensaje.contenido, lead).trim();
        if (text) await sock.sendMessage(jid, { text });
        break;
      }
      case 'formulario': {
        const rawTemplate = mensaje.contenido || '';
        const nameVal = encodeURIComponent(lead.nombre || '');
        const text = rawTemplate
          .replace('{{telefono}}', phone)
          .replace('{{nombre}}', nameVal)
          .replace(/\r?\n/g, ' ')
          .trim();
        if (text) await sock.sendMessage(jid, { text });
        break;
      }
      case 'audio':
        await sock.sendMessage(jid, {
          audio: { url: replacePlaceholders(mensaje.contenido, lead) },
          ptt: true
        });
        break;
      case 'imagen':
        await sock.sendMessage(jid, {
          image: { url: replacePlaceholders(mensaje.contenido, lead) }
        });
        break;
      case 'video':
        await sock.sendMessage(jid, {
          video: { url: replacePlaceholders(mensaje.contenido, lead) },
          // si quieres un caption, descomenta la línea siguiente y añade mensaje.contenidoCaption en tu secuencia
          // caption: replacePlaceholders(mensaje.contenidoCaption || '', lead)
        });
        break;
      default:
        console.warn(`Tipo desconocido: ${mensaje.type}`);
    }
  } catch (err) {
    console.error("Error al enviar mensaje:", err);
  }
}


/**
 * Procesa las secuencias activas de cada lead.
 */
async function processSequences() {
  try {
    const leadsSnap = await db
      .collection('leads')
      .where('secuenciasActivas', '!=', null)
      .get();

    for (const doc of leadsSnap.docs) {
      const lead = { id: doc.id, ...doc.data() };
      if (!Array.isArray(lead.secuenciasActivas) || !lead.secuenciasActivas.length) continue;

      let dirty = false;
      for (const seq of lead.secuenciasActivas) {
        const { trigger, startTime, index } = seq;
        const seqSnap = await db
          .collection('secuencias')
          .where('trigger', '==', trigger)
          .get();
        if (seqSnap.empty) continue;

        const msgs = seqSnap.docs[0].data().messages;
        if (index >= msgs.length) {
          seq.completed = true;
          dirty = true;
          continue;
        }

        const msg = msgs[index];
        const sendAt = new Date(startTime).getTime() + msg.delay * 60000;
        if (Date.now() < sendAt) continue;

        // Enviar y luego registrar en Firestore
        await enviarMensaje(lead, msg);
        await db
          .collection('leads')
          .doc(lead.id)
          .collection('messages')
          .add({
            content: `Se envió el ${msg.type} de la secuencia ${trigger}`,
            sender: 'system',
            timestamp: new Date()
          });

        seq.index++;
        dirty = true;
      }

      if (dirty) {
        const rem = lead.secuenciasActivas.filter(s => !s.completed);
        await db.collection('leads').doc(lead.id).update({ secuenciasActivas: rem });
      }
    }
  } catch (err) {
    console.error("Error en processSequences:", err);
  }
}

/**
 * Genera guiones VSL para los registros en 'guionesVideo' con status 'Sin guion',
 * guarda el guion, marca status → 'enviarGuion' y añade marca de tiempo.
 */
async function generateGuiones() {
  console.log("▶️ generateGuiones: inicio");
  try {
    const snap = await db.collection('guionesVideo').where('status', '==', 'Sin guion').get();
    console.log(`✔️ encontrados ${snap.size} guiones pendientes`);

    for (const docSnap of snap.docs) {
      const data = docSnap.data();
      // Adaptamos tu prompt VSL con placeholders
      const prompt = `
Eres un generador de guiones para Video Sales Letters (VSL) de alta conversión.
Tu tarea es crear un guion de VSL de 1 minuto, dividido en segmentos temporales, que incluya:

- **Variables**:
  - Nombre del negocio: ${data.businessName}
  - Giro del negocio: ${data.giro}
  - Propósito del anuncio: ${data.purpose}
  - Promoción especial: ${data.promo || 'ninguna'}

- **Estructura** (con tiempos aproximados):
  1. **0:00–0:10 Hook y Promesa**
     - Gancho inmediato que detenga el scroll y prometa el principal beneficio de ${data.businessName}.
  2. **0:10–0:20 Prueba Social Rápida**
     - Una frase de testimonio o resultado contundente de un cliente que aprovechó ${data.promo || 'la promoción'}.
  3. **0:20–0:30 Dolor y Agitación**
     - Describe en una o dos oraciones el problema urgente que enfrenta tu audiencia en ${data.giro}.
  4. **0:30–0:40 Solución Express**
     - Explica brevemente cómo ${data.businessName} resuelve ese problema de forma única.
  5. **0:40–0:55 Llamado a la Acción con Urgencia**
     - Invita a la audiencia a aprovechar ${data.promo || 'la promoción'} ahora, añade un motivo de urgencia o escasez.
  6. **0:55–1:00 Cierre Visual y Contacto**
     - Pantalla final limpia con CTA directo, logotipo y datos de contacto.

- **Texto para voz en off**:
  Define exactamente qué dirá la voz en cada segmento y con qué tono (energético, confiable, urgente).

- **Notas de edición**:
  Ritmo muy dinámico: cortes cada 1–3 segundos, overlays de texto en negrita, transiciones rápidas. Música de fondo que suba intensidad en 0:20–0:30 y mantenga energía hasta el final.

- **Recomendaciones de imágenes/B-roll**:
  - 0:00–0:10: logo animado o escena impactante del problema.
  - 0:10–0:20: captura real o foto del cliente satisfecho.
  - 0:20–0:30: metáfora visual del dolor (ej. reloj corriendo).
  - 0:30–0:40: demo rápida del producto o servicio en acción.
  - 0:40–0:55: texto grande con “¡Oferta por tiempo limitado!” sobre fondo limpio.
  - 0:55–1:00: logotipo y botón animado de “Compra ahora” o “Contáctanos”.

Genera el guion completo en español, con cada segmento numerado y su texto para voz en off, más las notas de edición y las sugerencias de imágenes, todo listo para producir. Máximo 250–300 palabras.
      `.trim();

      console.log(`📝 prompt para ${docSnap.id}:\n${prompt}`);

      const response = await openai.createChatCompletion({
        model: 'gpt-4o',
        messages: [
          { role: 'system', content: 'Eres un experto creador de guiones de video persuasivos.' },
          { role: 'user', content: prompt }
        ]
      });

      const guion = response.data.choices?.[0]?.message?.content?.trim();
      if (guion) {
        console.log(`✅ guion generado para ${docSnap.id}`);
        await docSnap.ref.update({
          guion,
          status: 'enviarGuion',
          guionGeneratedAt: FieldValue.serverTimestamp()
        });
      }
    }
    console.log("▶️ generateGuiones: finalizado");
  } catch (err) {
    console.error("❌ Error generateGuiones:", err);
  }
}


/**
 * Envía por WhatsApp los guiones generados (status 'enviarGuion'),
 * añade trigger 'GuionEnviado' al lead y marca status → 'enviado'.
 * Solo envía si han pasado al menos 15 minutos desde 'guionGeneratedAt'.
 */
async function sendGuiones() {
  try {
    const now = Date.now();
    // Busca todos los guiones pendientes de envío
    const snap = await db
      .collection('guionesVideo')
      .where('status', '==', 'enviarGuion')
      .get();

    const VIDEO_URL = 'https://cantalab.com/wp-content/uploads/ejemplo-guion-video.mp4';

    for (const docSnap of snap.docs) {
      const data = docSnap.data();
      const { leadPhone, leadId, guion, guionGeneratedAt, requesterName } = data;

      // Asegura que existan los datos necesarios
      if (!leadPhone || !guion || !guionGeneratedAt) continue;

      // Verifica que hayan pasado al menos 15 minutos desde la generación
      const genTime = guionGeneratedAt.toDate().getTime();
      if (now - genTime < 15 * 60 * 1000) continue;

      const sock = getWhatsAppSock();
      if (!sock) continue;

      const phoneClean = leadPhone.replace(/\D/g, '');
      const jid = `${phoneClean}@s.whatsapp.net`;
      const firstName = (requesterName || '').trim().split(' ')[0] || '';

      // 1) Aviso inicial
      const aviso = `Hola ${firstName}, tu guion de video está listo. ¡Échale un vistazo!`;
      await sock.sendMessage(jid, { text: aviso });
      await db
        .collection('leads').doc(leadId).collection('messages')
        .add({ content: aviso, sender: 'business', timestamp: new Date() });

      // 2) Enviar el guion
      await sock.sendMessage(jid, { text: guion });
      await db
        .collection('leads').doc(leadId).collection('messages')
        .add({ content: guion, sender: 'business', timestamp: new Date() });

      // 3) (Opcional) Enviar un video demo o promocional
      await sock.sendMessage(jid, { video: { url: VIDEO_URL } });
      await db
        .collection('leads').doc(leadId).collection('messages')
        .add({
          mediaType: 'video',
          mediaUrl: VIDEO_URL,
          sender: 'business',
          timestamp: new Date()
        });

      // 4) Actualizar lead con etiqueta y secuencia de “GuionEnviado”
      await db.collection('leads').doc(leadId).update({
        etiquetas: FieldValue.arrayUnion('GuionEnviado'),
        secuenciasActivas: FieldValue.arrayUnion({
          trigger: 'GuionEnviado',
          startTime: new Date().toISOString(),
          index: 0
        })
      });

      // 5) Marcar el documento como enviado
      await docSnap.ref.update({ status: 'enviado' });
    }
  } catch (err) {
    console.error("❌ Error en sendGuiones:", err);
  }
}


export {
  processSequences,
  generateGuiones,
  sendGuiones
};

