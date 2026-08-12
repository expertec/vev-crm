import test from 'node:test';
import assert from 'node:assert/strict';

import { extractMetaAdAttribution } from '../utils/metaAdDetector.js';
import {
  resolveMetaAdRouteFromRules,
  shouldScheduleMetaAdNoContentTrigger,
} from '../utils/metaAdSequenceRouter.js';

test('extrae atribucion de un mensaje click-to-whatsapp con referral', () => {
  const attribution = extractMetaAdAttribution({
    message: {
      referral: {
        source_id: '120200000000001',
        source_url: 'https://fb.me/ad/demo',
        headline: 'Tienda online para tu negocio',
        body: 'Cotiza tu tienda en linea hoy',
        media_type: 'image',
        ctwa_clid: 'clid-123',
      },
      text: { body: 'Hola, quiero informes' },
    },
  });

  assert.equal(attribution.isFromMetaAd, true);
  assert.equal(attribution.indicator, 'referral');
  assert.equal(attribution.sourceId, '120200000000001');
  assert.equal(attribution.adId, '120200000000001');
  assert.equal(attribution.headline, 'Tienda online para tu negocio');
  assert.equal(attribution.ctwaClid, 'clid-123');
});

test('extrae atribucion desde contextInfo.externalAdReply de Baileys', () => {
  const attribution = extractMetaAdAttribution({
    message: {
      extendedTextMessage: {
        text: 'Info',
        contextInfo: {
          externalAdReply: {
            sourceId: 'ad-456',
            sourceUrl: 'https://facebook.com/ads/ad-456',
            title: 'Pagina web profesional',
            body: 'Planes desde $990',
            mediaType: 'IMAGE',
          },
          smbClientCampaignId: 'campaign-abc',
        },
      },
    },
  });

  assert.equal(attribution.isFromMetaAd, true);
  assert.equal(attribution.indicator, 'externalAdReply');
  assert.equal(attribution.adId, 'ad-456');
  assert.equal(attribution.campaignId, 'campaign-abc');
  assert.equal(attribution.headline, 'Pagina web profesional');
});

test('resuelve la ruta mas especifica por anuncio antes que campana', () => {
  const result = resolveMetaAdRouteFromRules({
    attribution: {
      adId: 'ad-tienda',
      campaignId: 'camp-general',
    },
    fallbackTrigger: 'LeadWhatsapp',
    rules: [
      { id: 'camp', campaignId: 'camp-general', trigger: 'LeadGeneral' },
      { id: 'ad', adId: 'ad-tienda', trigger: 'LeadTiendaOnline' },
    ],
  });

  assert.equal(result.trigger, 'LeadTiendaOnline');
  assert.equal(result.routeId, 'ad');
  assert.equal(result.source, 'meta_ad_route');
});

test('usa fallback cuando no hay regla compatible', () => {
  const result = resolveMetaAdRouteFromRules({
    attribution: { adId: 'ad-sin-regla' },
    fallbackTrigger: 'LeadWhatsapp',
    rules: [
      { id: 'web', adId: 'ad-web', trigger: 'LeadPaginaWeb' },
    ],
  });

  assert.equal(result.trigger, 'LeadWhatsapp');
  assert.equal(result.source, 'meta_ad_default');
  assert.equal(result.routeId, '');
});

test('infiere PlanRedes desde metadata del anuncio cuando el mensaje es generico', () => {
  const result = resolveMetaAdRouteFromRules({
    attribution: {
      headline: 'Redes sociales para tu negocio',
      body: 'Manejo de redes sociales para vender mas',
      campaignName: 'Plan redes agosto',
    },
    fallbackTrigger: 'LeadWhatsapp',
    rules: [],
  });

  assert.equal(result.trigger, 'PlanRedes');
  assert.equal(result.source, 'meta_ad_inferred');
  assert.equal(result.routeId, 'inferred:PlanRedes');
});

test('permite fallback de Meta Ads sin contenido cuando no hay ruta especifica', () => {
  assert.equal(
    shouldScheduleMetaAdNoContentTrigger({
      metaRoute: {
        trigger: 'LeadWhatsapp',
        source: 'meta_ad_default',
      },
      allowDefaultFallback: true,
    }),
    true
  );
});

test('bloquea fallback generico de Meta Ads sin contenido cuando esta deshabilitado', () => {
  assert.equal(
    shouldScheduleMetaAdNoContentTrigger({
      metaRoute: {
        trigger: 'LeadWhatsapp',
        source: 'meta_ad_default',
      },
      allowDefaultFallback: false,
    }),
    false
  );
});
