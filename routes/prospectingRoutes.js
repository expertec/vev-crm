import express from 'express';
import { db } from '../firebaseAdmin.js';
import { ProspectingService } from '../services/prospectingService.js';

function cleanText(value = '', maxLength = 280) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, maxLength);
}

function cleanDigits(value = '') {
  return String(value || '').replace(/\D/g, '');
}

function safeDocId(value = '') {
  return cleanText(value, 180)
    .replace(/[/?#[\].]/g, '_')
    .replace(/\s+/g, '_')
    .slice(0, 160);
}

function leadPayloadFromProspect(prospect = {}) {
  const phoneDigits = cleanDigits(prospect.phoneDigits || prospect.phone || '');
  const primaryEmail = cleanText(prospect.primaryEmail || prospect.email || prospect.emails?.[0] || '', 180);
  const name = cleanText(prospect.name || '', 180);
  const now = new Date();
  return {
    nombre: name || primaryEmail || phoneDigits || 'Prospecto',
    telefono: phoneDigits,
    email: primaryEmail,
    estado: 'Prospecto',
    source: 'prospecting_google_places',
    fecha_creacion: now,
    lastMessageAt: now,
    prospecting: {
      source: 'google_places',
      placeId: cleanText(prospect.placeId || '', 180),
      address: cleanText(prospect.address || '', 260),
      website: cleanText(prospect.website || '', 600),
      googleMapsUrl: cleanText(prospect.googleMapsUrl || '', 600),
      emails: Array.isArray(prospect.emails) ? prospect.emails.slice(0, 8).map((item) => cleanText(item, 180)) : [],
      socialLinks: prospect.socialLinks && typeof prospect.socialLinks === 'object' ? prospect.socialLinks : {},
      rating: Number.isFinite(Number(prospect.rating)) ? Number(prospect.rating) : null,
      userRatingCount: Number.isFinite(Number(prospect.userRatingCount)) ? Number(prospect.userRatingCount) : null,
      opportunity: prospect.opportunity && typeof prospect.opportunity === 'object' ? prospect.opportunity : null,
      fit: prospect.fit && typeof prospect.fit === 'object' ? prospect.fit : null,
      importedAt: now,
    },
  };
}

function prospectListPayload({ name = '', description = '', area = '', businessType = '', prospects = [] } = {}) {
  const safeProspects = (Array.isArray(prospects) ? prospects : []).slice(0, 250).map((prospect) => ({
    placeId: cleanText(prospect.placeId || '', 180),
    name: cleanText(prospect.name || '', 180),
    address: cleanText(prospect.address || '', 260),
    phone: cleanText(prospect.phone || '', 80),
    phoneDigits: cleanDigits(prospect.phoneDigits || prospect.phone || ''),
    website: cleanText(prospect.website || '', 600),
    googleMapsUrl: cleanText(prospect.googleMapsUrl || '', 600),
    emails: Array.isArray(prospect.emails) ? prospect.emails.slice(0, 8).map((item) => cleanText(item, 180)) : [],
    primaryEmail: cleanText(prospect.primaryEmail || prospect.email || prospect.emails?.[0] || '', 180),
    rating: Number.isFinite(Number(prospect.rating)) ? Number(prospect.rating) : null,
    userRatingCount: Number.isFinite(Number(prospect.userRatingCount)) ? Number(prospect.userRatingCount) : null,
    socialLinks: prospect.socialLinks && typeof prospect.socialLinks === 'object' ? prospect.socialLinks : {},
    opportunity: prospect.opportunity && typeof prospect.opportunity === 'object' ? prospect.opportunity : null,
    fit: prospect.fit && typeof prospect.fit === 'object' ? prospect.fit : null,
    importedLeadId: cleanText(prospect.importedLeadId || '', 180),
    addedAt: new Date(),
  })).filter((prospect) => prospect.name || prospect.placeId || prospect.phoneDigits || prospect.primaryEmail);

  return {
    name: cleanText(name, 140) || `Lista ${new Date().toISOString().slice(0, 10)}`,
    description: cleanText(description, 500),
    area: cleanText(area, 140),
    businessType: cleanText(businessType, 120),
    prospects: safeProspects,
    prospectCount: safeProspects.length,
    updatedAt: new Date(),
  };
}

function serializeListDoc(docSnap) {
  const data = docSnap.data() || {};
  return {
    id: docSnap.id,
    ...data,
    createdAt: data.createdAt?.toDate?.()?.toISOString?.() || data.createdAt || null,
    updatedAt: data.updatedAt?.toDate?.()?.toISOString?.() || data.updatedAt || null,
    prospects: (Array.isArray(data.prospects) ? data.prospects : []).map((prospect) => ({
      ...prospect,
      addedAt: prospect.addedAt?.toDate?.()?.toISOString?.() || prospect.addedAt || null,
    })),
  };
}

async function resolveLeadRefForProspect(prospect = {}) {
  const phoneDigits = cleanDigits(prospect.phoneDigits || prospect.phone || '');
  if (phoneDigits) {
    const existing = await db.collection('leads').where('telefono', '==', phoneDigits).limit(1).get();
    if (!existing.empty) {
      return {
        ref: existing.docs[0].ref,
        leadId: existing.docs[0].id,
        existing: true,
      };
    }
  }

  const placeId = safeDocId(prospect.placeId || '');
  const fallback = safeDocId(phoneDigits || prospect.name || `prospect_${Date.now()}`);
  const leadId = placeId ? `prospect_${placeId}` : `prospect_${fallback}`;
  const ref = db.collection('leads').doc(leadId);
  const snap = await ref.get();
  return {
    ref,
    leadId,
    existing: snap.exists,
  };
}

export function createProspectingRouter({ logger = console } = {}) {
  const service = new ProspectingService({ logger });
  const router = express.Router();

  router.get('/crm/prospecting/config', (_req, res) => {
    res.json({
      success: true,
      googlePlacesConfigured: service.isConfigured(),
      aiConfigured: service.isAiConfigured(),
      provider: 'google_places',
    });
  });

  router.post('/crm/prospecting/zones', async (req, res) => {
    try {
      const result = await service.recommendZones({
        area: req.body?.area,
        businessType: req.body?.businessType,
        serviceProfile: req.body?.serviceProfile,
      });
      return res.json({ success: true, ...result });
    } catch (error) {
      logger.error('[prospecting] zones error:', error?.message || error);
      return res.status(500).json({
        success: false,
        error: 'No se pudieron generar zonas recomendadas.',
      });
    }
  });

  router.post('/crm/prospecting/search', async (req, res) => {
    try {
      const result = await service.search({
        area: req.body?.area,
        businessType: req.body?.businessType,
        maxResults: req.body?.maxResults,
        scanWebsites: req.body?.scanWebsites !== false,
        pageToken: req.body?.pageToken,
        useAiClassification: req.body?.useAiClassification !== false,
      });

      try {
        await db.collection('prospectingRuns').add({
          provider: 'google_places',
          query: result.query,
          summary: result.summary,
          itemCount: result.items.length,
          searchedAt: new Date(),
        });
      } catch (saveError) {
        logger.warn('[prospecting] No se pudo guardar historial:', saveError?.message || saveError);
      }

      return res.json({ success: true, ...result });
    } catch (error) {
      logger.error('[prospecting] search error:', error?.details || error?.message || error);
      const status = Number.isInteger(error?.statusCode) ? error.statusCode : 500;
      return res.status(status).json({
        success: false,
        code: cleanText(error?.code || 'PROSPECTING_SEARCH_ERROR', 80),
        error: status >= 500 ? 'No se pudo completar la busqueda de prospectos.' : cleanText(error?.message || 'Solicitud invalida.'),
        details: status >= 500 ? undefined : error?.details,
      });
    }
  });

  router.post('/crm/prospecting/import-lead', async (req, res) => {
    try {
      const prospect = req.body?.prospect || req.body || {};
      const payload = leadPayloadFromProspect(prospect);
      if (!payload.nombre && !payload.telefono && !payload.email) {
        return res.status(400).json({ success: false, error: 'Falta informacion del prospecto.' });
      }

      const target = await resolveLeadRefForProspect(prospect);
      const patch = { ...payload };
      if (target.existing) {
        delete patch.fecha_creacion;
        delete patch.lastMessageAt;
      }
      await target.ref.set(patch, { merge: true });

      return res.json({
        success: true,
        leadId: target.leadId,
        existing: target.existing,
      });
    } catch (error) {
      logger.error('[prospecting] import lead error:', error?.message || error);
      return res.status(500).json({
        success: false,
        error: 'No se pudo importar el prospecto como lead.',
      });
    }
  });

  router.get('/crm/prospecting/lists', async (_req, res) => {
    try {
      const snap = await db.collection('prospectingLists')
        .orderBy('updatedAt', 'desc')
        .limit(80)
        .get();
      return res.json({
        success: true,
        items: snap.docs.map(serializeListDoc),
      });
    } catch (error) {
      logger.error('[prospecting] list collections error:', error?.message || error);
      return res.status(500).json({
        success: false,
        error: 'No se pudieron cargar las listas de prospectos.',
      });
    }
  });

  router.get('/crm/prospecting/lists/:listId', async (req, res) => {
    try {
      const docSnap = await db.collection('prospectingLists').doc(String(req.params.listId)).get();
      if (!docSnap.exists) {
        return res.status(404).json({ success: false, error: 'Lista no encontrada.' });
      }
      return res.json({ success: true, list: serializeListDoc(docSnap) });
    } catch (error) {
      logger.error('[prospecting] get list error:', error?.message || error);
      return res.status(500).json({
        success: false,
        error: 'No se pudo cargar la lista.',
      });
    }
  });

  router.post('/crm/prospecting/lists', async (req, res) => {
    try {
      const payload = {
        ...prospectListPayload(req.body || {}),
        createdAt: new Date(),
      };
      const ref = await db.collection('prospectingLists').add(payload);
      const snap = await ref.get();
      return res.status(201).json({
        success: true,
        list: serializeListDoc(snap),
      });
    } catch (error) {
      logger.error('[prospecting] create list error:', error?.message || error);
      return res.status(500).json({
        success: false,
        error: 'No se pudo guardar la lista de prospectos.',
      });
    }
  });

  router.post('/crm/prospecting/lists/:listId/prospects', async (req, res) => {
    try {
      const ref = db.collection('prospectingLists').doc(String(req.params.listId));
      const snap = await ref.get();
      if (!snap.exists) {
        return res.status(404).json({ success: false, error: 'Lista no encontrada.' });
      }
      const current = snap.data() || {};
      const currentProspects = Array.isArray(current.prospects) ? current.prospects : [];
      const incoming = prospectListPayload({ prospects: req.body?.prospects || [] }).prospects;
      const byKey = new Map();
      [...currentProspects, ...incoming].forEach((prospect) => {
        const key = prospect.placeId || prospect.phoneDigits || prospect.primaryEmail || prospect.name;
        if (!key) return;
        byKey.set(String(key).toLowerCase(), prospect);
      });
      const prospects = Array.from(byKey.values()).slice(0, 250);
      await ref.set({
        prospects,
        prospectCount: prospects.length,
        updatedAt: new Date(),
      }, { merge: true });
      const nextSnap = await ref.get();
      return res.json({ success: true, list: serializeListDoc(nextSnap) });
    } catch (error) {
      logger.error('[prospecting] add list prospects error:', error?.message || error);
      return res.status(500).json({
        success: false,
        error: 'No se pudieron agregar prospectos a la lista.',
      });
    }
  });

  router.delete('/crm/prospecting/lists/:listId', async (req, res) => {
    try {
      await db.collection('prospectingLists').doc(String(req.params.listId)).delete();
      return res.json({ success: true });
    } catch (error) {
      logger.error('[prospecting] delete list error:', error?.message || error);
      return res.status(500).json({
        success: false,
        error: 'No se pudo eliminar la lista.',
      });
    }
  });

  return router;
}
