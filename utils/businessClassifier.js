// server/utils/businessClassifier.js

// --- Heurístico rápido: mapea palabras a sectores
const HEURISTIC_MAP = [
  { sector: 'restaurante',  kw: [/restaurante|comida|taquer|pizza|cafeter/i] },
  { sector: 'spa y belleza', kw: [/spa|estética|uñas|barber|belleza|facial/i] },
  { sector: 'clínica/salud', kw: [/clínica|consultorio|dent|médic|terapia|psicolog/i] },
  { sector: 'taller mecánico', kw: [/mecánic|auto|llanta|fren|alineaci/i] },
  { sector: 'inmobiliaria', kw: [/inmobili|bienes raíces|departament|casa|renta|venta/i] },
  { sector: 'gimnasio/fitness', kw: [/gym|gimnas|crossfit|entren|fitness|yoga/i] },
  { sector: 'tienda de ropa', kw: [/ropa|boutique|moda|vestid|playera|jean/i] },
  { sector: 'ferretería/construcción', kw: [/ferreter|construc|cemen|acero|obra/i] },
  { sector: 'escuela/cursos', kw: [/escuela|curso|clase|academ|taller/i] },
  { sector: 'servicios profesionales', kw: [/abogad|conta|marketing|diseñ|consultor/i] },
];

function heuristicClassify({ name = '', description = '', templateId = '' }) {
  const haystack = `${name} ${description} ${templateId}`.toLowerCase();
  for (const { sector, kw } of HEURISTIC_MAP) {
    if (kw.some((re) => re.test(haystack))) return sector;
  }
  // Fallback según objetivo
  const t = String(templateId || '').toLowerCase();
  if (t.includes('ecommerce')) return 'tienda online';
  if (t.includes('booking'))   return 'servicios con reservas';
  return 'negocio local';
}

// ⚠️ Hook opcional para LLM (puedes conectar OpenAI/Azure/etc. aquí)
async function llmClassify(_summary) {
  // Por defecto, no llama a ningún LLM.
  // Si luego quieres, conecta tu proveedor aquí y retorna un string (sector).
  return null;
}

/**
 * Clasifica sector y genera keywords para imágenes
 * @returns {{ sector: string, keywords: string }}
 */
export async function classifyBusiness(summary = {}) {
  const base = {
    name: summary.companyName || summary.name || '',
    description: summary.description || summary.businessStory || '',
    templateId: summary.templateId || '',
  };

  // 1) Probar LLM si decides activarlo
  let sector = await llmClassify(base);

  // 2) Si no hay LLM o no respondió, usar heurístico
  if (!sector) sector = heuristicClassify(base);

  // 3) Armar keywords de imagen (sector + extracto limpio de descripción)
  const descTop = String(base.description)
    .replace(/https?:\/\/\S+/g, '')
    .replace(/[^\p{L}\p{N}\s]/gu, '')
    .split(/\s+/).filter(Boolean).slice(0, 4).join(' ');

  const keywords = [sector, descTop].filter(Boolean).join(' ').trim() || sector;
  return { sector, keywords };
}
