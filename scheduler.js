// src/server/scheduler.js
import admin from 'firebase-admin';
import { getWhatsAppSock } from './whatsappService.js';
import { db } from './firebaseAdmin.js';
import { Configuration, OpenAIApi } from 'openai';
import axios from 'axios';        
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
const PEXELS_API_KEY = process.env.PEXELS_API_KEY;

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

async function generateSiteSchemas() {
  console.log("▶️ generateSiteSchemas: inicio");
  if (!PEXELS_API_KEY) throw new Error("Falta PEXELS_API_KEY en entorno");
  const snap = await db.collection('Negocios')  // o 'sitios', según tu nombre
                        .where('status', '==', 'Sin procesar')
                        .get();

  for (const doc of snap.docs) {
    const data = doc.data();
    try {
      // 1. Generar schema con OpenAI
      const prompt = `
        Eres un asistente que construye un JSON de site schema.
        Recibe estos datos: 
        nombre: ${data.companyInfo},
        giro: ${data.businessSector.join(', ')},
        historia: ${data.businessStory},
        items: ${JSON.stringify(data.keyItems)},
        paleta: ${JSON.stringify(data.palette)},
        redes: Instagram: ${data.socialInstagram}, Facebook: ${data.socialFacebook}.
        Devuelve SOLO el JSON del schema completo, sin texto adicional.
      `;
      const aiRes = await openai.createChatCompletion({
        model: 'gpt-4o',
        messages: [
          { role: 'system', content: 'Eres un generador de site schemas en JSON.' },
          { role: 'user', content: prompt }
        ]
      });
      const schema = JSON.parse(aiRes.data.choices[0].message.content.trim());

      // 2. Buscar foto en Pexels
      const query = data.businessSector.join(' ');
      const px = await axios.get('https://api.pexels.com/v1/search', {
        headers: { Authorization: PEXELS_API_KEY },
        params: { query, per_page: 1 }
      });
      const photo = px.data.photos[0]?.src?.large || null;

      if (photo) {
        // Asigna la imagen de fondo del hero al schema
        schema.hero = schema.hero || {};
        schema.hero.backgroundImageUrl = photo;
      }

      // 3. Actualiza en Firestore
      await doc.ref.update({
        schema,
        status: 'Procesado',
        processedAt: FieldValue.serverTimestamp()
      });
      console.log(`✅ Site schema generado para ${doc.id}`);
    } catch (err) {
      console.error(`❌ Error en generateSiteSchemas para ${doc.id}:`, err);
    }
  }
  console.log("▶️ generateSiteSchemas: finalizado");
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
      case 'audio': {
        const audioUrl = replacePlaceholders(mensaje.contenido, lead);
        console.log('→ Enviando PTT desde URL:', audioUrl);
        await sock.sendMessage(jid, {
          audio: { url: audioUrl },
          ptt: true
      
        });
        break;
      }
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






export {
  processSequences,

  
  generateSiteSchemas
};

