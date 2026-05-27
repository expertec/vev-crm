import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildLeadFollowupVariant,
  evaluateLeadForAlwaysOn,
  evaluateLeadForReactivation,
  getPreviousCalendarWeekWindow,
} from '../services/leadReactivationService.js';

test('calcula la semana calendario anterior respecto a 2026-05-26 en America/Monterrey', () => {
  const window = getPreviousCalendarWeekWindow({
    now: new Date('2026-05-26T18:00:00.000Z'),
    timezone: 'America/Monterrey',
  });

  assert.equal(window.fromDate, '2026-05-18');
  assert.equal(window.toDate, '2026-05-24');
  assert.equal(window.campaignId, 'last-week-2026-05-18_2026-05-24');
});

test('usa contexto de muestra para variar el mensaje', () => {
  const variant = buildLeadFollowupVariant(
    {
      id: 'lead-1',
      nombre: 'Maria Lopez',
      etiquetas: ['MuestraLista'],
    },
    { campaignId: 'last-week-2026-05-18_2026-05-24' }
  );

  assert.match(variant.message, /muestra/i);
  assert.equal(variant.contextKey, 'sample_sent');
  assert.match(variant.variationKey, /^o\d-b\d-c\d-x\d$/);
});

test('omite leads con mensajes no leidos', () => {
  const window = getPreviousCalendarWeekWindow({
    now: new Date('2026-05-26T18:00:00.000Z'),
    timezone: 'America/Monterrey',
  });

  const result = evaluateLeadForReactivation(
    {
      id: 'lead-2',
      nombre: 'Pedro',
      telefono: '5215551112233',
      fecha_creacion: new Date('2026-05-20T12:00:00.000Z'),
      lastMessageAt: new Date('2026-05-20T12:00:00.000Z'),
      unreadCount: 1,
    },
    {
      window,
      now: new Date('2026-05-26T18:00:00.000Z'),
      minSilenceHours: 24,
    }
  );

  assert.equal(result.eligible, false);
  assert.equal(result.reason, 'has_unread_messages');
});

test('always-on considera Leads nuevos cuando no hay etapa', () => {
  const result = evaluateLeadForAlwaysOn(
    {
      id: 'lead-new',
      nombre: 'Ana',
      telefono: '5215559998877',
      fecha_creacion: new Date('2026-05-20T12:00:00.000Z'),
      lastMessageAt: new Date('2026-05-20T12:00:00.000Z'),
      unreadCount: 0,
      hasActiveSequences: false,
    },
    {
      settings: {
        targetStages: ['leads_nuevos'],
        minSilenceHours: 24,
        maxTouches: 6,
        cadenceHours: [24, 72, 168],
      },
      now: new Date('2026-05-26T18:00:00.000Z'),
    }
  );

  assert.equal(result.eligible, true);
  assert.equal(result.stageKey, 'leads_nuevos');
});

test('always-on omite leads fuera de etapas objetivo', () => {
  const result = evaluateLeadForAlwaysOn(
    {
      id: 'lead-stage-out',
      nombre: 'Luis',
      telefono: '5215559991122',
      etapa: 'seguimiento',
      fecha_creacion: new Date('2026-05-20T12:00:00.000Z'),
      lastMessageAt: new Date('2026-05-20T12:00:00.000Z'),
      unreadCount: 0,
      hasActiveSequences: false,
    },
    {
      settings: {
        targetStages: ['leads_nuevos'],
        minSilenceHours: 24,
        maxTouches: 6,
        cadenceHours: [24, 72, 168],
      },
      now: new Date('2026-05-26T18:00:00.000Z'),
    }
  );

  assert.equal(result.eligible, false);
  assert.equal(result.reason, 'outside_target_stage');
});
