// src/server/scheduler.js
import admin from 'firebase-admin';
import { getWhatsAppSock } from './whatsappService.js';
import { db } from './firebaseAdmin.js';
import { Configuration, OpenAIApi } from 'openai'
import axios from 'axios';        
const { FieldValue } = admin.firestore;
// Asegúrate de que la API key esté definida
if (!process.env.OPENAI_API_KEY) {
  throw new Error("Falta la variable de entorno OPENAI_API_KEY");
}

const PEXELS_API_KEY = process.env.PEXELS_API_KEY
if (!process.env.OPENAI_API_KEY) {
  throw new Error("Falta OPENAI_API_KEY en entorno")
}

// 1) Inicializamos el cliente oficial v3/v4 compatible
const configuration = new Configuration({
  apiKey: process.env.OPENAI_API_KEY,
})
const openai = new OpenAIApi(configuration)


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
  console.log("▶️ generateSiteSchemas: inicio")
  if (!PEXELS_API_KEY) throw new Error("Falta PEXELS_API_KEY en entorno")

  // 1) Consulta todos los documentos pendientes
  const snap = await db
    .collection("Negocios")
    .where("status", "==", "Sin procesar")
    .get()

  for (const doc of snap.docs) {
    const data = doc.data()
    try {
      // 2) Construye el prompt dividido en system + user
      const promptSystem = `
Eres un redactor publicitario SENIOR, experto en copywriting persuasivo
para sitios web de cualquier sector. Tu misión:
1) Proponer títulos, subtítulos y llamadas a la acción claros y orientados a la conversión.
2) Mantener un tono profesional y cercano.
3) Devolver ÚNICAMENTE un JSON con la estructura exacta que se describe a continuación,
   sin explicaciones ni texto adicional.
`.trim()

      const promptUser = `
Negocio de giro: "${data.businessSector.join(", ")}"
Nombre: "${data.companyInfo}"
Historia: "${data.businessStory}"
Colores disponibles: ${JSON.stringify(data.palette)}
Servicios/productos: ${JSON.stringify(data.keyItems)}
WhatsApp: ${data.contactWhatsapp}
Instagram: ${data.socialInstagram}
Facebook: ${data.socialFacebook}

**IMPORTANTE:** Para cada elemento de la sección "features", analiza su "title" y su "text" y asigna **el icono de Ant Design** que mejor represente su contenido. Dispones de este set de iconos:

  SafetyOutlined, BulbOutlined, UsergroupAddOutlined,
  HeartOutlined, RocketOutlined, ExperimentOutlined

Cada feature debe tener la forma:
  {
    "icon":    "<nombre del icono>",
    "title":   "<título>",
    "text":    "<texto descriptivo>"
  }

Devuelve únicamente el JSON con la forma exacta descrita a continuación, sin texto adicional:

{
  "slug":    "<slug>",
  "logoUrl": "<URL del logo>",
  "colors": {
    "primary":   "<hex>",
    "secondary": "<hex>",
    "accent":    "<hex>",
    "text":      "<hex>"
  },
  "hero": {
    "title":               "<Título>",
    "subtitle":            "<Subtítulo>",
    "ctaText":             "<Texto CTA>",
    "ctaUrl":              "<URL CTA>",
    "backgroundImageUrl":  "<URL fondo>"
  },
  "features": {
    "title": "¿Qué nos hace únicos?",
    "items": [
      { "icon":"<Icono1>","title":"<T1>","text":"<D1>" },
      { "icon":"<Icono2>","title":"<T2>","text":"<D2>" },
      …
    ]
  },
  "products": {
    "title":"<Título sección productos>",
    "items":[
      {
        "title":"<nombre>",
        "text":"<desc>",
        "imageUrl":"<img>",
        "buttonText":"<botón>",
        "buttonUrl":"<url>"
      },
      …
    ]
  },
  "about": {
    "title":"<Título>",
    "text":"<Texto>"
  },
  "menu":[
    {"id":"services","label":"Servicios"},
    {"id":"about","label":"Nosotros"},
    {"id":"contact","label":"Contáctanos"}
  ],
  "contact": {
    "whatsapp":"<teléfono>",
    "email":"<email>",
    "facebook":"<url>",
    "instagram":"<url>",
    "youtube":"<url>"
  },
  "testimonials": {
    "title":"<Título testimonios>",
    "items":[
      { "text":"<t1>","author":"<a1>" },
      …
    ]
  }
}
`.trim()

      // 3) Generar schema
      const aiRes = await openai.createChatCompletion({
        model:       "gpt-4o",
        messages: [
          { role: "system", content: promptSystem },
          { role: "user",   content: promptUser  }
        ],
        temperature: 0.7,
        max_tokens:  800
      })

      // 4) Limpiar y parsear JSON
      let raw = aiRes.data.choices[0].message.content.trim()
      raw = raw.replace(/^```json\s*/i, "").replace(/```$/i, "").trim()
      const schema = JSON.parse(raw)

      // ────────────────────────────────────────────────────────
      // 5) Traduce giro a inglés para Pexels
      const sectorText = data.businessSector.join(", ")
      const translateRes = await openai.createChatCompletion({
        model:       "gpt-4o",
        messages: [
          {
            role:    "system",
            content: "Eres un asistente que convierte un sector de negocio en una frase de búsqueda en inglés para fotos de stock."
          },
          {
            role:    "user",
            content: `Dame una frase corta en inglés para buscar imágenes en Pexels relacionadas con este giro: "${sectorText}".`
          }
        ],
        temperature: 0.3,
        max_tokens:  50
      })
      const englishQuery = translateRes.data.choices[0].message.content.trim()
      // ────────────────────────────────────────────────────────

      // 6) Buscar fondo en Pexels
      const px = await axios.get("https://api.pexels.com/v1/search", {
        headers: { Authorization: PEXELS_API_KEY },
        params:  { query: englishQuery, per_page: 1 }
      })
      const photo = px.data.photos?.[0]?.src?.large
      if (photo) {
        schema.hero = schema.hero || {}
        schema.hero.backgroundImageUrl = photo
      }

      // 7) Guardar en Firestore
      await doc.ref.update({
        schema,
        status:      "Procesado",
        processedAt: FieldValue.serverTimestamp()
      })

      console.log(`✅ Site schema generado para ${doc.id}`)
    } catch (err) {
      console.error(`❌ Error en generateSiteSchemas para ${doc.id}:`, err)
    }
  }

  console.log("▶️ generateSiteSchemas: finalizado")
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

