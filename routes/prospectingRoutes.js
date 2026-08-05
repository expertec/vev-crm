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
      importedAt: now,
    },
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
      provider: 'google_places',
    });
  });

  router.post('/crm/prospecting/search', async (req, res) => {
    try {
      const result = await service.search({
        area: req.body?.area,
        businessType: req.body?.businessType,
        maxResults: req.body?.maxResults,
        scanWebsites: req.body?.scanWebsites !== false,
        pageToken: req.body?.pageToken,
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

  return router;
}
