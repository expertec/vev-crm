// scheduler-updated.js - REEMPLAZA tu scheduler.js actual

import admin from 'firebase-admin';
import { db } from './firebaseAdmin.js';
import { getWhatsAppSock } from './whatsappService.js';
import { Timestamp } from 'firebase-admin/firestore';
import * as Q from './queue.js';
import puppeteer from 'puppeteer';

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

function getSampleSiteBaseUrl() {
  return String(
    process.env.SAMPLE_SITE_BASE_URL ||
      process.env.SITE_PUBLIC_BASE_URL ||
      'https://negociosweb.mx/site'
  ).replace(/\/+$/, '');
}

function resolveSampleSlug(leadData = {}) {
  const candidate = [
    leadData?.slug,
    leadData?.webSlug,
    leadData?.siteSlug,
    leadData?.briefWeb?.slug,
    leadData?.schema?.slug,
  ].find((v) => String(v || '').trim());
  return String(candidate || '').trim();
}

function buildLinkPagina(leadData = {}) {
  const slug = resolveSampleSlug(leadData);
  if (!slug) return '';
  return `${getSampleSiteBaseUrl()}/${encodeURIComponent(slug)}`;
}

function normalizeSlug(value = '') {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80) || 'site';
}

async function captureSitePreviewBuffer(slug) {
  const url = `${getSampleSiteBaseUrl()}/${encodeURIComponent(slug)}`;
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });
  try {
    const page = await browser.newPage();
    await page.setViewport({ width: 390, height: 844, isMobile: true, deviceScaleFactor: 2 });
    await page.goto(url, { waitUntil: 'networkidle2', timeout: 60_000 });
    await new Promise((resolve) => setTimeout(resolve, 1200));
    return await page.screenshot({
      type: 'jpeg',
      quality: 78,
      fullPage: false,
    });
  } finally {
    await browser.close().catch(() => {});
  }
}

async function uploadPreviewToStorage(buffer, slug) {
  const bucket = admin.storage().bucket();
  const safeSlug = normalizeSlug(slug);
  const filePath = `site-previews/${safeSlug}/mobile_${Date.now()}.jpg`;
  const file = bucket.file(filePath);

  await file.save(buffer, {
    contentType: 'image/jpeg',
    metadata: { cacheControl: 'public,max-age=31536000' },
    resumable: false,
    validation: false,
  });

  try {
    await file.makePublic();
    return {
      path: filePath,
      url: `https://storage.googleapis.com/${bucket.name}/${filePath}`,
    };
  } catch {
    const [signedUrl] = await file.getSignedUrl({
      action: 'read',
      expires: '2100-01-01',
    });
    return { path: filePath, url: signedUrl };
  }
}

async function ensureSitePreviewForNegocio(negocioId, negocioData = {}) {
  const currentUrl = String(negocioData?.previewImageUrl || '').trim();
  if (currentUrl) return { ok: true, url: currentUrl, path: String(negocioData?.previewImagePath || '') };

  const slug = resolveSampleSlug(negocioData);
  if (!slug) return { ok: false, url: '', path: '', error: 'missing_slug' };

  try {
    const buffer = await captureSitePreviewBuffer(slug);
    const uploaded = await uploadPreviewToStorage(buffer, slug);

    if (negocioId) {
      await db.collection('Negocios').doc(negocioId).set(
        {
          previewImageUrl: uploaded.url,
          previewImagePath: uploaded.path,
          previewGeneratedAt: Timestamp.now(),
        },
        { merge: true }
      );
    }

    return { ok: true, url: uploaded.url, path: uploaded.path };
  } catch (err) {
    console.warn(
      `[ensureSitePreviewForNegocio] No se pudo generar preview para ${negocioId || slug}:`,
      err?.message || err
    );
    return { ok: false, url: '', path: '', error: String(err?.message || err) };
  }
}

function resolveLeadIdFromNegocio(data = {}) {
  const rawLeadId = String(data?.leadId || '').trim();
  if (/@s\.whatsapp\.net$/i.test(rawLeadId)) return rawLeadId;

  const phoneRaw =
    data?.leadPhone ||
    data?.contactWhatsapp ||
    '';
  const phoneDigits = String(phoneRaw || '').replace(/\D/g, '');
  if (phoneDigits.length < 10) return '';
  const e164 = toE164(phoneRaw || '');
  const leadId = e164ToJid(e164);
  return /^\d+@s\.whatsapp\.net$/i.test(leadId) ? leadId : '';
}

async function activateArchiveSequenceForLead(negocioId, data = {}) {
  const leadId = resolveLeadIdFromNegocio(data);
  if (!leadId) {
    console.warn(
      `[archivarNegociosAntiguos] Negocio ${negocioId} sin leadId/leadPhone enrutable para activar #etapaLevamiento`
    );
    return { leadId: '', scheduled: false, trigger: null };
  }

  const triggerCandidates = [
    '#etapaLevamiento',
    'EtapaLevamiento',
    '#etapalevamiento',
    'etapaLevamiento',
    'etapalevamiento',
  ];

  let scheduled = false;
  let usedTrigger = null;

  if (typeof Q.scheduleSequenceForLead === 'function') {
    for (const trigger of triggerCandidates) {
      try {
        const programmed = await Q.scheduleSequenceForLead(
          leadId,
          trigger,
          new Date()
        );
        if (programmed > 0) {
          scheduled = true;
          usedTrigger = trigger;
          break;
        }
      } catch (err) {
        console.warn(
          `[archivarNegociosAntiguos] No se pudo programar trigger '${trigger}' para ${leadId}:`,
          err?.message || err
        );
      }
    }
  } else {
    console.warn(
      '[archivarNegociosAntiguos] Q.scheduleSequenceForLead no disponible'
    );
  }

  const etiquetas = ['NegocioArchivado', '#etapaLevamiento'];
  if (usedTrigger) etiquetas.push(usedTrigger);

  const leadPatch = {
    etapa: 'negocio_archivado',
    archivedNegocioAt: new Date(),
    archivedNegocioId: negocioId,
    archivedSequenceTrigger: usedTrigger || '#etapaLevamiento',
    archivedSequenceScheduled: scheduled,
    etiquetas: FieldValue.arrayUnion(...etiquetas),
  };
  if (scheduled) leadPatch.hasActiveSequences = true;

  await db
    .collection('leads')
    .doc(leadId)
    .set(leadPatch, { merge: true })
    .catch((err) => {
      console.warn(
        `[archivarNegociosAntiguos] No se pudo actualizar lead ${leadId}:`,
        err?.message || err
      );
    });

  return { leadId, scheduled, trigger: usedTrigger };
}

function replacePlaceholders(template, leadData) {
  const str = String(template || '');
  const linkPagina = buildLinkPagina(leadData);

  const resolveField = (field) => {
    const value = leadData?.[field] || '';
    if (field === 'nombre') return firstName(value);
    if (field === 'linkPagina' || field === 'link_pagina') return linkPagina;
    return value;
  };

  return str
    .replace(/\{\{\s*(\w+)\s*\}\}/g, (_, field) => resolveField(field))
    .replace(/\$\{\s*(\w+)\s*\}/g, (_, field) => resolveField(field));
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

        const preview = await ensureSitePreviewForNegocio(id, {
          ...data,
          schema,
        });
        if (preview.ok && preview.url) {
          console.log(`   🖼️ Preview generado para ${id}`);
        } else {
          console.warn(`   ⚠️ No se pudo generar preview para ${id}; se enviará sin imagen.`);
        }

        // Envío inmediato al quedar "Procesado" (sin esperar al cron)
        const sentNow = await enviarSitioWebPorWhatsApp({
          id,
          ...data,
          schema,
          previewImageUrl: preview.url || data.previewImageUrl || '',
          previewImagePath: preview.path || data.previewImagePath || '',
          status: 'Procesado',
        });
        if (sentNow) {
          await db.collection('Negocios').doc(id).set({
            status: 'Web enviada',
            siteSentAt: FieldValue.serverTimestamp(),
            siteReadyAt: FieldValue.delete(),
            siteScheduleSetAt: FieldValue.delete(),
            siteSendMode: 'immediate',
          }, { merge: true });
          console.log(`   ⚡ Sitio enviado al instante para: ${id}`);
        } else {
          await db.collection('Negocios').doc(id).set({
            siteSendMode: 'immediate',
            siteSendPending: true,
            siteSendLastAttemptAt: Timestamp.now(),
          }, { merge: true });
          console.warn(`   ⚠️ Envío inmediato falló para ${id}; queda para reintento por cron.`);
        }

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
    if (!sock) return false;

    const { jid, phone } = Q.resolveLeadJidAndPhone(lead);
    if (!jid) {
      console.warn('[enviarMensaje] No se pudo resolver JID para lead', lead?.id || lead?.telefono);
      return false;
    }

    switch ((mensaje?.type || 'texto').toLowerCase()) {
      case 'texto': {
        const text = replacePlaceholders(mensaje.contenido, lead).trim();
        if (!text) return false;
        await sock.sendMessage(jid, { text, linkPreview: false }, { timeoutMs: 120_000 });
        return true;
      }
      case 'formulario': {
        const raw = String(mensaje.contenido || '');
        const text = raw
          .replace('{{telefono}}', String(phone || '').replace(/\D/g, ''))
          .replace('{{nombre}}', encodeURIComponent(lead.nombre || ''))
          .replace(/\r?\n/g, ' ')
          .trim();
        if (!text) return false;
        await sock.sendMessage(jid, { text, linkPreview: false }, { timeoutMs: 120_000 });
        return true;
      }
      case 'audio': {
        const audioUrl = replacePlaceholders(mensaje.contenido, lead).trim();
        if (!audioUrl) return false;
        await sock.sendMessage(jid, { audio: { url: audioUrl }, ptt: true });
        return true;
      }
      case 'imagen': {
        const url = replacePlaceholders(mensaje.contenido, lead).trim();
        if (!url) return false;
        const caption = replacePlaceholders(mensaje.caption || '', lead).trim();
        await sock.sendMessage(
          jid,
          caption ? { image: { url }, caption } : { image: { url } }
        );
        return true;
      }
      case 'video': {
        const url = replacePlaceholders(mensaje.contenido, lead).trim();
        if (!url) return false;
        await sock.sendMessage(jid, { video: { url } }, { timeoutMs: 120_000 });
        return true;
      }
      default:
        console.warn('Tipo desconocido:', mensaje?.type);
        return false;
    }
  } catch (err) {
    console.error('Error al enviar mensaje:', err);
    return false;
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
    return false;
  }

  const e164 = toE164(phoneRaw);
  const jid = e164ToJid(e164);
  const sitioUrl = `https://negociosweb.mx/site/${slug}`;
  const linkPagina = sitioUrl;
  const textoSitioListo =
    `¡Tu página está lista! 🎉\n\n` +
    `Chécala aquí: ${linkPagina}\n\n` +
    `Baja para que veas todas las secciones: inicio, servicios, testimonios, contacto y el botón de WhatsApp.`;

  try {
    console.log(`📤 [ENVIANDO WHATSAPP] A: ${e164} | URL: ${sitioUrl}`);

    let previewUrl = String(negocio?.previewImageUrl || '').trim();
    if (!previewUrl && negocio?.id) {
      const preview = await ensureSitePreviewForNegocio(negocio.id, negocio);
      previewUrl = preview.url || '';
    }
    
    const delivered = await enviarMensaje(
      { telefono: e164, nombre: negocio.companyInfo || '' },
      previewUrl
        ? {
            type: 'imagen',
            contenido: previewUrl,
            caption: textoSitioListo,
          }
        : {
            type: 'texto',
            contenido: textoSitioListo,
          }
    );
    if (!delivered) {
      throw new Error('No se pudo entregar mensaje de sitio listo');
    }
    
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
    return true;
  } catch (err) {
    console.error(`❌ Error enviando WhatsApp a ${e164}:`, err);
    return false;
  }
}

/**
 * Fallback: busca negocios "Procesado" que no se pudieron enviar al instante
 */
export async function enviarSitiosPendientes() {
  return withTaskLock('enviarSitiosPendientes', 30, async () => {
    console.log('⏳ Buscando negocios procesados pendientes de envío...');
    
    const snap = await db.collection('Negocios')
      .where('status', '==', 'Procesado')
      .get();
    
    console.log(`📋 Encontrados: ${snap.size} negocios procesados`);

    for (const doc of snap.docs) {
      const data = doc.data();
      console.log(`\n📤 Enviando sitio para negocio: ${doc.id}`, {
        leadPhone: data.leadPhone,
        slug: data.slug,
        status: data.status
      });

      const sent = await enviarSitioWebPorWhatsApp({
        id: doc.id,
        ...data,
      });
      if (sent) {
        await doc.ref.set({
          status: 'Web enviada',
          siteSentAt: FieldValue.serverTimestamp(),
          siteSendPending: false,
          siteSendLastAttemptAt: Timestamp.now(),
          siteSendLastError: FieldValue.delete(),
        }, { merge: true });
        
        console.log(`✅ Sitio enviado y marcado como "Web enviada"`);
      } else {
        await doc.ref.set({
          siteSendPending: true,
          siteSendLastAttemptAt: Timestamp.now(),
          siteSendLastError: 'send_failed',
        }, { merge: true });
        console.warn(`⚠️ Reintento fallido para negocio ${doc.id}; seguirá pendiente.`);
      }
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

      await db.collection('ArchivoNegocios').doc(doc.id).set({
        ...data,
        archivedAt: Timestamp.now(),
        archivedReason: 'inactive_24h_no_plan',
      });

      const seqResult = await activateArchiveSequenceForLead(doc.id, data);
      if (seqResult.scheduled) {
        console.log(
          `🎯 Secuencia '${seqResult.trigger}' activada para ${seqResult.leadId} tras archivar ${doc.id}`
        );
      } else {
        console.warn(
          `⚠️ No se pudo activar secuencia #etapaLevamiento para ${doc.id} (lead: ${seqResult.leadId || 'N/A'})`
        );
      }

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
  if (typeof Q.processSequenceLeadsBatch === 'function') {
    return await Q.processSequenceLeadsBatch({ limit: 25 });
  }
  if (typeof Q.processDueSequenceJobs === 'function') {
    return await Q.processDueSequenceJobs({ limit: 25 });
  }
  if (typeof Q.processQueue === 'function') {
    return await Q.processQueue({ batchSize: 200 });
  }
  console.warn('⚠️ No hay función de proceso de cola exportada.');
  return 0;
}
