import test from 'node:test';
import assert from 'node:assert/strict';
import { canAutoStartSampleAgent, isWebsiteInquiry } from '../services/sampleCampaignAgent.js';

test('la plantilla de campaña inicia el agente sin pagar una clasificación adicional', async () => {
  for (const text of ['Hola, tengo un negocio y quiero información sobre la página web de $990 al año.', 'HOLA tengo un negocio y quiero informacion sobre la pagina web de $ 990 al año!']) {
    assert.equal(await isWebsiteInquiry({ text }, async () => assert.fail('La plantilla ya es explícita')), true);
  }
});

test('las variantes se interpretan por intención con el texto y contexto del anuncio', async () => {
  const cases = [
    ['Busco un sitio para mi restaurante, ¿cuánto cuesta?', null],
    ['Me interesa, dame informes', { headline: 'Página web para tu negocio' }],
    ['Quiero cotizar una página para mi negocio', { campaignName: 'Web 990' }],
  ];
  for (const [text, campaign] of cases) {
    assert.equal(await isWebsiteInquiry({ text, campaign }, async (resource, body) => {
      assert.equal(resource, 'responses'); assert.equal(body.store, false);
      assert.deepEqual(JSON.parse(body.input[0].content[0].text), { text, campaign });
      assert.equal(body.text.format.strict, true);
      return { output_text: '{"intent":"website"}' };
    }), true);
  }
});

test('rechazo, soporte y redes sociales no se enrutan como solicitud de muestra', async () => {
  for (const [text, intent] of [['Mi página web no funciona y ya pagué', 'other'], ['Quiero una página de Facebook', 'other'], ['No quiero la página web, no me escriban', 'stop']]) {
    assert.equal(await isWebsiteInquiry({ text }, async () => ({ output_text: JSON.stringify({ intent }) })), false);
  }
  assert.equal(await isWebsiteInquiry({ text: 'Quiero manejo de Instagram' }, async () => assert.fail('No es una consulta web')), false);
  assert.equal(await isWebsiteInquiry({ text: 'Hola' }, async () => assert.fail('No hay contexto suficiente')), false);
});

test('la activación respeta pausas, clientes, atención humana y muestras ya entregadas', () => {
  assert.equal(canAutoStartSampleAgent({}), true);
  assert.equal(canAutoStartSampleAgent({ sampleLinkSentAt: new Date(), sampleLinkSentUrl: 'https://app.negociosweb.mx/muestra/52123' }), true);
  for (const lead of [
    { sampleAgent: { enabled: false, status: 'paused' } },
    { sampleAgent: { enabled: true } },
    { sampleLinkSentAt: new Date(), sampleLinkSentUrl: 'https://negociosweb.mx/sam' },
    { humanControl: true }, { salesBrainHumanControl: true }, { stopSequences: true },
    { estado: 'cliente' }, { estado: 'compro' }, { estado: 'no interesado' }, { archived: true }, { queue: { status: 'claimed' } },
  ]) assert.equal(canAutoStartSampleAgent(lead), false);
});

test('una clasificación incompleta o fallida no activa el agente', async () => {
  await assert.rejects(isWebsiteInquiry({ text: 'Quiero mi página web' }, async () => ({ status: 'incomplete' })), /clasificación/);
  await assert.rejects(isWebsiteInquiry({ text: 'Quiero una web para mi negocio' }, async () => { throw Error('Temporal'); }), /Temporal/);
});
