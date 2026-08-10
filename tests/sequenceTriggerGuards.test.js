import test from 'node:test';
import assert from 'node:assert/strict';

import {
  getSequenceTriggerFamily,
  shouldBlockSequenceByLeadContext,
} from '../utils/sequenceTriggerGuards.js';

test('clasifica familias de secuencias comerciales', () => {
  assert.equal(getSequenceTriggerFamily('PlanRedes'), 'social');
  assert.equal(getSequenceTriggerFamily('NuevoLeadWeb'), 'web');
  assert.equal(getSequenceTriggerFamily('WebPromo'), 'web');
  assert.equal(getSequenceTriggerFamily('OtroTrigger'), '');
});

test('bloquea secuencia web si el lead ya entro por PlanRedes', () => {
  const result = shouldBlockSequenceByLeadContext(
    {
      etiquetas: ['MetaAds', 'PlanRedes'],
      secuenciasActivas: [
        { trigger: 'PlanRedes', index: 1, completed: false },
      ],
    },
    'NuevoLeadWeb'
  );

  assert.equal(result.blocked, true);
  assert.equal(result.reason, 'social_campaign_sequence_lock');
});

test('bloquea web aunque PlanRedes solo exista en historial', () => {
  const result = shouldBlockSequenceByLeadContext(
    {
      sequenceDeliveredTriggers: ['PlanRedes'],
      secuenciasActivas: [],
    },
    'WebPromo'
  );

  assert.equal(result.blocked, true);
});

test('no bloquea triggers no web por contexto de redes', () => {
  const result = shouldBlockSequenceByLeadContext(
    { etiquetas: ['PlanRedes'] },
    'CierreManual'
  );

  assert.equal(result.blocked, false);
});

