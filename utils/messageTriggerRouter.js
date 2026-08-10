const STATIC_HASHTAG_MAP = {
  '#webpro990': 'LeadWeb',
  '#leadweb': 'LeadWeb',
  '#nuevolead': 'NuevoLeadWeb',
  '#planredes990': 'PlanRedes',
  '#planredes': 'PlanRedes',
  '#redessociales': 'PlanRedes',
  '#redes': 'PlanRedes',
  '#info': 'LeadWeb',
  '#infoweb': 'NuevoLead',
};

const STATIC_CANCEL_BY_TRIGGER = {
  LeadWeb: ['NuevoLeadWeb', 'NuevoLead'],
  PlanRedes: [
    'LeadWeb',
    'LeadWhatsapp',
    'LeadPaginaWeb',
    'NuevoLead',
    'NuevoLeadWeb',
    'WebPromo',
    'WebEnviada',
  ],
};

const STATIC_TEXT_TRIGGER_RULES = [
  {
    trigger: 'PlanRedes',
    includes: [
      'plan de redes sociales',
      'redes sociales para tu negocio',
      'info de redes sociales',
      'informacion de redes sociales',
      'información de redes sociales',
      'info del plan de redes',
      'manejo de redes sociales',
      'servicio de redes sociales',
    ],
  },
];

export function extractHashtags(text = '') {
  const found = String(text).toLowerCase().match(/#[\p{L}\p{N}_-]+/gu);
  return found ? Array.from(new Set(found)) : [];
}

function normalizeText(text = '') {
  return String(text || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim();
}

export function resolveStaticTriggerFromMessage(text, defaultTrigger = 'NuevoLeadWeb') {
  const tags = extractHashtags(text);

  for (const tag of tags) {
    const trigger = STATIC_HASHTAG_MAP[tag];
    if (trigger) {
      return {
        trigger,
        cancel: STATIC_CANCEL_BY_TRIGGER[trigger] || [],
        source: 'hashtag',
      };
    }
  }

  const normalized = normalizeText(text);
  for (const rule of STATIC_TEXT_TRIGGER_RULES) {
    if (rule.includes.some((needle) => normalized.includes(normalizeText(needle)))) {
      return {
        trigger: rule.trigger,
        cancel: STATIC_CANCEL_BY_TRIGGER[rule.trigger] || [],
        source: 'text',
      };
    }
  }

  return { trigger: defaultTrigger, cancel: [], source: 'default' };
}

export function shouldPreferMessageTriggerOverMetaRoute(messageRule = {}, metaRoute = null) {
  const source = String(messageRule?.source || '').toLowerCase();
  if (source === 'db' || source === 'hashtag') return true;
  if (source !== 'text') return false;

  const routeSource = String(metaRoute?.source || '').toLowerCase();
  return !routeSource || routeSource === 'meta_ad_default';
}
