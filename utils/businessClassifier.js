// server/utils/businessClassifier.js

// ⚠️ LLM opcional (OpenAI): requiere OPENAI_API_KEY en el entorno
import axios from 'axios';

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

// --- LLM (OpenAI) — si no hay OPENAI_API_KEY, devuelve null
async function llmClassify(base) {
  try {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) return null;

    const model = process.env.OPENAI_MODEL || 'gpt-4o-mini';

    // Lista controlada de sectores para respuestas consistentes
    const LABELS = [
      'pastelería/panadería', 'restaurante', 'cafetería', 'spa y belleza',
      'clínica/salud', 'taller mecánico', 'inmobiliaria', 'gimnasio/fitness',
      'tienda de ropa', 'ferretería/construcción', 'escuela/cursos',
      'servicios profesionales', 'tienda online', 'servicios con reservas',
      'negocio local'
    ];

    const sys = `Eres un clasificador muy conciso.
Devuelve SOLO uno de estos sectores (texto exacto): ${LABELS.join(', ')}.
Si dudas, elige el más cercano. No expliques nada.`;

    const user = `Nombre: ${base.name || '-'}
Descripción: ${base.description || '-'}
Objetivo/Plantilla: ${base.templateId || '-'}`;

    const { data } = await axios.post(
      'https://api.openai.com/v1/chat/completions',
      {
        model,
        messages: [
          { role: 'system', content: sys },
          { role: 'user', content: user }
        ],
        temperature: 0
      },
      {
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json'
        },
        timeout: 10000
      }
    );

    let out = (data?.choices?.[0]?.message?.content || '').trim().toLowerCase();

    // Normaliza y mapea a la etiqueta más cercana de LABELS
    const norm = (s) => String(s).normalize('NFD').replace(/[\u0300-\u036f]/g, '');
    const nout = norm(out);
    let best = null;

    // Coincidencia directa/contiene
    for (const lab of LABELS) {
      if (nout.includes(norm(lab))) { best = lab; break; }
    }

    // Fuzzy básico si no hubo match directo
    if (!best) {
      if (/pastel|panader|repost/i.test(nout)) best = 'pastelería/panadería';
      else if (/restaur|comid|taquer|pizz/i.test(nout)) best = 'restaurante';
      else if (/cafe|barista/i.test(nout)) best = 'cafetería';
      else if (/spa|belleza|uñas|barber/i.test(nout)) best = 'spa y belleza';
      else if (/clinic|dent|medic|salud/i.test(nout)) best = 'clínica/salud';
      else if (/auto|mecan|llant|taller/i.test(nout)) best = 'taller mecánico';
      else if (/propied|inmobili|bienes/i.test(nout)) best = 'inmobiliaria';
      else if (/gym|fitn|yoga/i.test(nout)) best = 'gimnasio/fitness';
      else if (/ropa|boutique|moda/i.test(nout)) best = 'tienda de ropa';
      else if (/ferret|constr|obr|albañ/i.test(nout)) best = 'ferretería/construcción';
      else if (/escuel|curso|academ|clase/i.test(nout)) best = 'escuela/cursos';
      else if (/abog|conta|market|consult/i.test(nout)) best = 'servicios profesionales';
      else if (/e-?commerce|tienda online/i.test(nout)) best = 'tienda online';
      else if (/reserva|booking|agenda/i.test(nout)) best = 'servicios con reservas';
      else best = 'negocio local';
    }

    return best || 'negocio local';
  } catch {
    return null; // si falla, el heurístico cubrirá
  }
}

/**
 * Clasifica sector y genera keywords para imágenes
 * @param {{ companyName?: string, name?: string, description?: string, businessStory?: string, templateId?: string }} summary
 * @returns {{ sector: string, keywords: string }}
 */
export async function classifyBusiness(summary = {}) {
  const base = {
    name: summary.companyName || summary.name || '',
    description: summary.description || summary.businessStory || '',
    templateId: summary.templateId || '',
  };

  // 1) Probar LLM (si está habilitado)
  let sector = await llmClassify(base);

  // 2) Fallback al heurístico si no hubo respuesta del LLM
  if (!sector) sector = heuristicClassify(base);

  // 3) Keywords para imágenes (sector + extracto limpio de descripción)
  const descTop = String(base.description)
    .replace(/https?:\/\/\S+/g, '')
    .replace(/[^\p{L}\p{N}\s]/gu, '')
    .split(/\s+/).filter(Boolean).slice(0, 4).join(' ');

  const keywords = [sector, descTop].filter(Boolean).join(' ').trim() || sector;

  return { sector, keywords };
}
