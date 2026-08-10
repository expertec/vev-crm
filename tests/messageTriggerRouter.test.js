import test from 'node:test';
import assert from 'node:assert/strict';

import {
  extractHashtags,
  resolveStaticTriggerFromMessage,
  shouldPreferMessageTriggerOverMetaRoute,
} from '../utils/messageTriggerRouter.js';

test('resuelve PlanRedes desde el mensaje predefinido de la campana social', () => {
  const result = resolveStaticTriggerFromMessage(
    'Hola Sergio, quiero info del plan de redes sociales',
    'NuevoLeadWeb'
  );

  assert.equal(result.trigger, 'PlanRedes');
  assert.equal(result.source, 'text');
});

test('resuelve PlanRedes desde hashtag normalizado', () => {
  const result = resolveStaticTriggerFromMessage(
    'Hola Sergio, quiero info #PlanRedes990',
    'NuevoLeadWeb'
  );

  assert.equal(result.trigger, 'PlanRedes');
  assert.equal(result.source, 'hashtag');
});

test('resuelve PlanRedes desde #RedesSociales', () => {
  const result = resolveStaticTriggerFromMessage(
    'Hola Sergio, quiero info #RedesSociales',
    'NuevoLeadWeb'
  );

  assert.equal(result.trigger, 'PlanRedes');
  assert.equal(result.source, 'hashtag');
});

test('resuelve PlanRedes desde texto directo de redes sociales', () => {
  const result = resolveStaticTriggerFromMessage(
    'Hola, quiero info de redes sociales',
    'NuevoLeadWeb'
  );

  assert.equal(result.trigger, 'PlanRedes');
  assert.equal(result.source, 'text');
});

test('conserva el default cuando no hay hashtag ni frase conocida', () => {
  const result = resolveStaticTriggerFromMessage('Hola, quiero informacion', 'NuevoLeadWeb');

  assert.equal(result.trigger, 'NuevoLeadWeb');
  assert.equal(result.source, 'default');
});

test('extrae hashtags sin duplicar y en minusculas', () => {
  assert.deepEqual(extractHashtags('#PlanRedes990 texto #planredes990'), ['#planredes990']);
});

test('prioriza hashtag del mensaje sobre fallback de Meta Ads', () => {
  assert.equal(
    shouldPreferMessageTriggerOverMetaRoute(
      { trigger: 'PlanRedes', source: 'hashtag' },
      { trigger: 'WebPromo', source: 'meta_ad_default' }
    ),
    true
  );
});

test('mantiene ruta especifica de Meta Ads sobre texto inferido del mensaje', () => {
  assert.equal(
    shouldPreferMessageTriggerOverMetaRoute(
      { trigger: 'PlanRedes', source: 'text' },
      { trigger: 'LeadTiendaOnline', source: 'meta_ad_route' }
    ),
    false
  );
});
