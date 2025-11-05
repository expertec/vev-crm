// scheduler-updated.js - REEMPLAZA tu scheduler.js actual

import admin from 'firebase-admin';
import { db } from './firebaseAdmin.js';
import { getWhatsAppSock } from './whatsappService.js';
import { Timestamp } from 'firebase-admin/firestore';
import * as Q from './queue.js';

// ⭐ IMPORTAR EL NUEVO GENERADOR
import { generateCompleteSchema } from './schemaGenerator.js';

const { FieldValue } = admin.firestore;

// =============== TASK LOCK ===============
const _taskLocks = new Map();

async function withTaskLock(taskName, timeoutMinutes = 5, fn) {
  const now = Date.now();
  const existing = _taskLocks.get(taskName);
  if (existing && now - existing < timeoutMinutes * 60 * 1000) {
    console.log(`[withTaskLock] ${taskName} ya se está ejecutando, skip.`);
    return 0;
  }
  _taskLocks.set(taskName, now);
  try {
    return await fn();
  } finally {
    _taskLocks.delete(taskName);
  }
}

// =============== TELÉFONOS ===============
import { parsePhoneNumberFromString } from 'libphonenumber-js';

function toE164(num, defaultCountry = 'MX') {
  const raw = String(num || '').replace(/\D/g, '');
  const p = parsePhoneNumberFromString(raw, defaultCountry);
  if (p && p.isValid()) return p.number;
  if (/^\d{10}$/.test(raw)) return `+52${raw}`;
  if (/^\d{11,15}$/.test(raw) && raw.startsWith('521')) return `+${raw}`;
  if (/^\d{11,15}$/.test(raw) && raw.startsWith('52')) return `+${raw}`;
  return `+${raw}`;
}

function normalizePhoneForWA(phone) {
  let num = String(phone || '').replace(/\D/g, '');
  if (num.length === 12 && num.startsWith('52') && !num.startsWith('521')) {
    return '521' + num.slice(2);
  }
  if (num.length === 10) return '521' + num;
  return num;
}

function e164ToJid(e164) {
  const digits = String(e164 || '').replace(/\D/g, '');
  return `${normalizePhoneForWA(digits)}@s.whatsapp.net`;
}

function firstName(n = '') {
  return String(n).trim().split(/\s+/)[0] || '';
}

function replacePlaceholders(template, leadData) {
  const str = String(template || '');
  return str.replace(/\{\{(\w+)\}\}/g, (_, field) => {
    const value = leadData?.[field] || '';
    if (field === 'nombre') return firstName(value);
    return value;
  });
}

// =============== GENERACIÓN DE SCHEMAS ===============

/**
 * 🎯 FUNCIÓN PRINCIPAL: Genera schemas para negocios "Sin procesar"
 * 
 * Ahora usa el generador mejorado con IA que crea contenido profesional
 */
export async function generateSiteSchemas() {
  return withTaskLock('generateSiteSchemas', 10, async () => {
    console.log('🔍 Buscando negocios "Sin procesar" para generar schemas...');
    
    const snap = await db.collection('Negocios')
      .where('status', '==', 'Sin procesar')
      .limit(5)
      .get();

    if (snap.empty) {
      console.log('✅ No hay negocios pendientes por procesar.');
      return 0;
    }

    console.log(`📋 Encontrados ${snap.size} negocios para procesar.`);

    for (const doc of snap.docs) {
      const id = doc.id;
      const data = doc.data();
      
      try {
        console.log(`\n⚙️ Procesando negocio: ${id}`);
        console.log(`   - Nombre: ${data.companyInfo || 'N/A'}`);
        console.log(`   - Template: ${data.templateId || 'info'}`);
        console.log(`   - Slug: ${data.slug || 'N/A'}`);

        // Validaciones básicas
        if (!data.slug) {
          throw new Error('Falta el campo slug en el documento');
        }

        // 🚀 GENERAR SCHEMA COMPLETO CON IA
        console.log('   🤖 Generando contenido con IA...');
        const schema = await generateCompleteSchema(data);
        
        console.log('   ✅ Schema generado exitosamente');
        console.log(`   📄 Secciones incluidas: ${Object.keys(schema).join(', ')}`);

        // Guardar en Firestore
        await db.collection('Negocios').doc(id).set({
          schema,
          status: 'Procesado',
          processedAt: Timestamp.now(),
          lastGeneratedAt: Timestamp.now(),
          updatedAt: Timestamp.now()
        }, { merge: true });

        console.log(`   💾 Schema guardado en Firebase para: ${id}`);
        console.log(`   🌐 URL del sitio: https://negociosweb.mx/site/${data.slug}`);

      } catch (err) {
        console.error(`❌ Error procesando negocio ${id}:`, err?.message || err);
        
        await db.collection('Negocios').doc(id).set({
          status: 'Error',
          lastError: String(err?.message || err),
          errorAt: Timestamp.now()
        }, { merge: true });
      }
    }

    return snap.size;
  });
}

// =============== ENVÍO POR WHATSAPP ===============

export async function enviarMensaje(lead, mensaje) {
  try {
    const sock = getWhatsAppSock();
    if (!sock) return;

    const e164 = toE164(lead.telefono);
    const jid = e164ToJid(e164);

    switch ((mensaje?.type || 'texto').toLowerCase()) {
      case 'texto': {
        const text = replacePlaceholders(mensaje.contenido, lead).trim();
        if (text) await sock.sendMessage(jid, { text, linkPreview: false });
        break;
      }
      case 'formulario': {
        const raw = String(mensaje.contenido || '');
        const text = raw
          .replace('{{telefono}}', e164.replace(/\D/g, ''))
          .replace('{{nombre}}', encodeURIComponent(lead.nombre || ''))
          .replace(/\r?\n/g, ' ')
          .trim();
        if (text) await sock.sendMessage(jid, { text, linkPreview: false });
        break;
      }
      case 'audio': {
        const audioUrl = replacePlaceholders(mensaje.contenido, lead).trim();
        if (audioUrl) {
          await sock.sendMessage(jid, { audio: { url: audioUrl }, ptt: true });
        }
        break;
      }
      case 'imagen': {
        const url = replacePlaceholders(mensaje.contenido, lead).trim();
        if (url) await sock.sendMessage(jid, { image: { url } });
        break;
      }
      case 'video': {
        const url = replacePlaceholders(mensaje.contenido, lead).trim();
        if (url) await sock.sendMessage(jid, { video: { url } });
        break;
      }
      default:
        console.warn('Tipo desconocido:', mensaje?.type);
    }
  } catch (err) {
    console.error('Error al enviar mensaje:', err);
  }
}

export async function enviarSitioWebPorWhatsApp(negocio) {
  const slug = negocio?.slug || negocio?.schema?.slug;
  const phoneRaw = negocio?.leadPhone;
  
  if (!phoneRaw || !slug) {
    console.warn('Faltan datos para enviar el sitio web por WhatsApp', {
      leadPhone: phoneRaw,
      slug
    });
    return;
  }

  const e164 = toE164(phoneRaw);
  const jid = e164ToJid(e164);
  const sitioUrl = `https://negociosweb.mx/site/${slug}`;

  try {
    console.log(`📤 [ENVIANDO WHATSAPP] A: ${e164} | URL: ${sitioUrl}`);
    
    await enviarMensaje(
      { telefono: e164, nombre: negocio.companyInfo || '' },
      { 
        type: 'texto', 
        contenido: `¡Hola! 🎉 Tu sitio web ya está listo.\n\nPuedes verlo aquí: ${sitioUrl}\n\n✨ Este es tu sitio de muestra gratuito por 24 horas.\n\nSi te gusta y quieres mantenerlo activo, te enviaremos opciones de planes desde $397 MXN/año.` 
      }
    );
    
    console.log(`✅ WhatsApp enviado a ${e164}: ${sitioUrl}`);

    // Activar secuencia WebEnviada
    try {
      const leadId = jid;
      if (typeof Q.cancelSequences === 'function') {
        await Q.cancelSequences(leadId, ['NuevoLeadWeb', 'LeadWeb']).catch(() => {});
      }
      if (typeof Q.scheduleSequenceForLead === 'function') {
        await Q.scheduleSequenceForLead(leadId, 'WebEnviada', new Date()).catch(() => {});
      }
      
      const leadRef = db.collection('leads').doc(leadId);
      await leadRef.set(
        { etiquetas: FieldValue.arrayUnion('WebEnviada') },
        { merge: true }
      ).catch(() => {});
    } catch (seqErr) {
      console.warn('[enviarSitioWebPorWhatsApp] No se pudo activar secuencias:', seqErr?.message);
    }
  } catch (err) {
    console.error(`❌ Error enviando WhatsApp a ${e164}:`, err);
  }
}

/**
 * Busca negocios "Procesado" y los envía con retraso realista (15-25 min)
 */
export async function enviarSitiosPendientes() {
  return withTaskLock('enviarSitiosPendientes', 30, async () => {
    console.log('⏳ Buscando negocios procesados para enviar sitio web...');
    
    const snap = await db.collection('Negocios')
      .where('status', '==', 'Procesado')
      .get();
    
    console.log(`📋 Encontrados: ${snap.size} negocios procesados`);

    const nowMs = Date.now();

    for (const doc of snap.docs) {
      const data = doc.data();
      const hasReady = !!data.siteReadyAt;
      const readyMs = data.siteReadyAt?.toMillis?.() ?? null;

      // 1) Si NO tiene siteReadyAt, programarlo a +15–25 min
      if (!hasReady) {
        const jitter = Math.floor(Math.random() * (10 * 60 * 1000)); // 0-10 min
        const target = nowMs + (15 * 60 * 1000) + jitter; // 15-25 min
        
        await doc.ref.update({
          siteReadyAt: Timestamp.fromMillis(target),
          siteScheduleSetAt: FieldValue.serverTimestamp()
        });
        
        console.log(`⏰ Programado siteReadyAt para ${doc.id} en ${new Date(target).toISOString()}`);
        continue;
      }

      // 2) Si tiene siteReadyAt pero aún no llega, omitir
      if (readyMs && readyMs > nowMs) {
        console.log(`⏸️ ${doc.id} aún no alcanza siteReadyAt (${new Date(readyMs).toISOString()})`);
        continue;
      }

      // 3) Ya es hora: enviar
      console.log(`\n📤 Enviando sitio para negocio: ${doc.id}`, {
        leadPhone: data.leadPhone,
        slug: data.slug,
        status: data.status
      });

      await enviarSitioWebPorWhatsApp(data);

      // 4) Marcar como enviado
      await doc.ref.update({
        status: 'Web enviada',
        siteSentAt: FieldValue.serverTimestamp()
      });
      
      console.log(`✅ Sitio enviado y marcado como "Web enviada"`);
    }

    return snap.size;
  });
}

// =============== ARCHIVAR ===============

export async function archivarNegociosAntiguos() {
  const ahora = Date.now();
  const limite = ahora - 24 * 60 * 60 * 1000;
  const limiteTimestamp = Timestamp.fromMillis(limite);

  const snap = await db.collection('Negocios')
    .where('createdAt', '<', limiteTimestamp)
    .get();
  
  if (snap.empty) {
    console.log('✅ No hay negocios antiguos para archivar.');
    return 0;
  }

  let n = 0;
  for (const doc of snap.docs) {
    try {
      const data = doc.data();
      if (data.plan !== undefined && data.plan !== null && data.plan !== '') {
        console.log(`💎 Negocio ${doc.id} tiene plan (${data.plan}), no se archiva.`);
        continue;
      }
      
      await db.collection('ArchivoNegocios').doc(doc.id).set(data);
      await doc.ref.delete();
      
      console.log(`📦 Negocio ${doc.id} archivado correctamente.`);
      n++;
    } catch (err) {
      console.error(`❌ Error archivando negocio ${doc.id}:`, err);
    }
  }
  
  return n;
}

// =============== SECUENCIAS ===============

export async function processSequences() {
  const fn =
    typeof Q.processDueSequenceJobs === 'function'
      ? Q.processDueSequenceJobs
      : (typeof Q.processQueue === 'function' ? Q.processQueue : null);

  if (!fn) {
    console.warn('⚠️ No hay función de proceso de cola exportada.');
    return 0;
  }
  
  return await fn({ batchSize: 200 });
}