import test from 'node:test';
import assert from 'node:assert/strict';

import {
  extractHashtags,
  resolveStaticTriggerFromMessage,
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

test('conserva el default cuando no hay hashtag ni frase conocida', () => {
  const result = resolveStaticTriggerFromMessage('Hola, quiero informacion', 'NuevoLeadWeb');

  assert.equal(result.trigger, 'NuevoLeadWeb');
  assert.equal(result.source, 'default');
});

test('extrae hashtags sin duplicar y en minusculas', () => {
  assert.deepEqual(extractHashtags('#PlanRedes990 texto #planredes990'), ['#planredes990']);
});

