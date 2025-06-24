// server.js
import express from 'express';
import cors from 'cors';
import bodyParser from 'body-parser';
import dotenv from 'dotenv';
import cron from 'node-cron';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import ffmpeg from 'fluent-ffmpeg';
import ffmpegInstaller from '@ffmpeg-installer/ffmpeg';
import { enviarSitiosPendientes } from './scheduler.js';


// Dile a fluent-ffmpeg dónde está el binario
ffmpeg.setFfmpegPath(ffmpegInstaller.path);


import { sendAudioMessage } from './whatsappService.js';  // ajusta ruta si es necesario


dotenv.config();

import { db } from './firebaseAdmin.js';
import {
  connectToWhatsApp,
  getLatestQR,
  getConnectionStatus,
  sendMessageToLead,
  getSessionPhone
} from './whatsappService.js';
import { processSequences, generateSiteSchemas, archivarNegociosAntiguos   } from './scheduler.js';



const app = express();
const port = process.env.PORT || 3001;

const upload = multer({ dest: path.resolve('./uploads') });

app.use(cors());
app.use(bodyParser.json());

// Endpoint para consultar el estado de WhatsApp (QR y conexión)
app.get('/api/whatsapp/status', (req, res) => {
  res.json({
    status: getConnectionStatus(),
    qr: getLatestQR()
  });
});

// Nuevo endpoint para obtener el número de sesión
app.get('/api/whatsapp/number', (req, res) => {
  const phone = getSessionPhone();
  if (phone) {
    res.json({ phone });
  } else {
    res.status(503).json({ error: 'WhatsApp no conectado' });
  }
});

// Endpoint para enviar mensaje de WhatsApp
app.post('/api/whatsapp/send-message', async (req, res) => {
  const { leadId, message } = req.body;
  if (!leadId || !message) {
    return res.status(400).json({ error: 'Faltan leadId o message en el body' });
  }

  try {
    const leadRef = db.collection('leads').doc(leadId);
    const leadDoc = await leadRef.get();
    if (!leadDoc.exists) {
      return res.status(404).json({ error: "Lead no encontrado" });
    }

    const { telefono } = leadDoc.data();
    if (!telefono) {
      return res.status(400).json({ error: "Lead sin número de teléfono" });
    }

    // Delega la normalización y el guardado a sendMessageToLead
    const result = await sendMessageToLead(telefono, message);
    return res.json(result);
  } catch (error) {
    console.error("Error enviando mensaje de WhatsApp:", error);
    return res.status(500).json({ error: error.message });
  }
});

// Recibe el audio, lo convierte a M4A y lo envía por Baileys
app.post(
  '/api/whatsapp/send-audio',
  upload.single('audio'),
  async (req, res) => {
    const { phone } = req.body;
    const uploadPath = req.file.path;           // WebM/Opus crudo
    const m4aPath   = `${uploadPath}.m4a`;      // destino M4A

    try {
      // 1) Transcodifica a M4A (AAC)
      await new Promise((resolve, reject) => {
        ffmpeg(uploadPath)
          .outputOptions(['-c:a aac', '-vn'])
          .toFormat('mp4')
          .save(m4aPath)
          .on('end', resolve)
          .on('error', reject);
      });

      // 2) Envía la nota de voz ya en M4A
      await sendAudioMessage(phone, m4aPath);

      // 3) Borra archivos temporales
      fs.unlinkSync(uploadPath);
      fs.unlinkSync(m4aPath);

      return res.json({ success: true });
    } catch (error) {
      console.error('Error enviando audio:', error);
      // limpia lo que haya quedado
      try { fs.unlinkSync(uploadPath); } catch {}
      try { fs.unlinkSync(m4aPath); }   catch {}
      return res.status(500).json({ success: false, error: error.message });
    }
  }
);

app.post('/api/crear-usuario', async (req, res) => {
  const { email, negocioId } = req.body;
  if (!email || !negocioId) {
    return res.status(400).json({ error: 'Faltan email o negocioId' });
  }
  try {
    // 1. Genera contraseña temporal segura
    const tempPassword = Math.random().toString(36).slice(-8);

    // 2. Intenta buscar el usuario, si no existe, lo crea
    let userRecord, isNewUser = false;
    try {
      userRecord = await admin.auth().getUserByEmail(email);
    } catch (err) {
      userRecord = await admin.auth().createUser({ email, password: tempPassword });
      isNewUser = true;
    }

    // 3. Actualiza el negocio con UID y email del owner
    await db.collection('Negocios').doc(negocioId).update({
      ownerUID: userRecord.uid,
      ownerEmail: email
    });

    // 4. Toma datos del negocio para el mensaje
    const negocioDoc = await db.collection('Negocios').doc(negocioId).get();
    const negocio = negocioDoc.data();
    const telefono = negocio?.contactWhatsapp || negocio?.leadPhone;

    // 5. Calcula la fecha de corte (planRenewalDate)
    let fechaCorte = null;
    if (negocio.planRenewalDate?.toDate) {
      fechaCorte = dayjs(negocio.planRenewalDate.toDate()).format('DD/MM/YYYY');
    } else if (negocio.planRenewalDate instanceof Date) {
      fechaCorte = dayjs(negocio.planRenewalDate).format('DD/MM/YYYY');
    } else if (typeof negocio.planRenewalDate === 'string' || typeof negocio.planRenewalDate === 'number') {
      fechaCorte = dayjs(negocio.planRenewalDate).format('DD/MM/YYYY');
    } else {
      fechaCorte = '-';
    }

    // 6. Construye el mensaje de WhatsApp
    const urlAcceso = "https://negociosweb.mx/login"; // ← Cambia por tu URL real
    let mensaje = `¡Bienvenido a tu panel de administración de tu página web! 👋

🔗 Accede aquí: ${urlAcceso}
📧 Usuario: ${email}
`;

    if (isNewUser) {
      mensaje += `🔑 Contraseña temporal: ${tempPassword}\n`;
    } else {
      mensaje += `🔄 Si no recuerdas tu contraseña, usa el enlace "¿Olvidaste tu contraseña?"\n`;
    }

    mensaje += `
🗓️ Tu plan termina el día: ${fechaCorte}

Por seguridad, cambia tu contraseña después de ingresar.
`;

    // 7. Envía el mensaje por WhatsApp si hay teléfono
    if (telefono) {
      await sendMessageToLead(telefono, mensaje);
    }

    // 8. Si es usuario existente, puedes enviarle link de reset por correo
    if (!isNewUser) {
      await admin.auth().generatePasswordResetLink(email);
    }

    return res.json({ success: true, uid: userRecord.uid, email });
  } catch (err) {
    console.error('Error creando usuario:', err);
    return res.status(500).json({ error: err.message });
  }
});




// (Opcional) Marcar todos los mensajes de un lead como leídos
app.post('/api/whatsapp/mark-read', async (req, res) => {
  const { leadId } = req.body;
  if (!leadId) {
    return res.status(400).json({ error: "Falta leadId en el body" });
  }
  try {
    await db.collection('leads')
            .doc(leadId)
            .update({ unreadCount: 0 });
    return res.json({ success: true });
  } catch (err) {
    console.error("Error marcando como leídos:", err);
    return res.status(500).json({ error: err.message });
  }
});

// Arranca el servidor y conecta WhatsApp
app.listen(port, () => {
  console.log(`Servidor corriendo en el puerto ${port}`);
  connectToWhatsApp().catch(err =>
    console.error("Error al conectar WhatsApp en startup:", err)
  );


  
});

 // Scheduler: ejecuta las secuencias activas cada 15 segundos
 cron.schedule('*/30 * * * * *', () => {
  console.log('⏱️ processSequences:', new Date().toISOString());
  processSequences().catch(err => console.error('Error en processSequences:', err));
});


// Cada 1 minutos busca nuevos sitios por procesar
cron.schedule('* * * * *', () => {
  console.log('⏱️ generateSiteSchemas:', new Date().toISOString());
  generateSiteSchemas().catch(err => console.error('Error en generateSiteSchemas:', err));
});

cron.schedule('*/5 * * * *', () => {
  console.log('⏱️ enviarSitiosPendientes:', new Date().toISOString());
  enviarSitiosPendientes().catch(err => console.error('Error en enviarSitiosPendientes:', err));
});

// Ejecutar cada hora en el minuto 0 (ejemplo: 13:00, 14:00, etc)
cron.schedule('0 * * * *', () => {
  console.log('⏱️ archivarNegociosAntiguos:', new Date().toISOString());
  archivarNegociosAntiguos().catch(err => console.error('Error en archivarNegociosAntiguos:', err));
});
