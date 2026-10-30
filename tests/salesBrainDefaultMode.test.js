import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildNewInboundLeadSalesBrainDefaults,
  getDefaultSalesBrainMode,
  isSalesBrainEnabled,
} from '../services/salesBrain/defaultMode.js';

test('leads entrantes nuevos mantienen Sales Brain apagado', () => {
  const previous = process.env.SALES_BRAIN_DEFAULT_MODE;
  delete process.env.SALES_BRAIN_DEFAULT_MODE;
  try {
    const defaults = buildNewInboundLeadSalesBrainDefaults();
    assert.equal(getDefaultSalesBrainMode(), 'off');
    assert.equal(defaults.salesBrainMode, 'off');
    assert.equal(defaults.queue.status, 'automation');
    assert.equal(defaults.queue.priority, 0);
    assert.equal(defaults.salesContext.businessType, null);
  } finally {
    if (previous === undefined) delete process.env.SALES_BRAIN_DEFAULT_MODE;
    else process.env.SALES_BRAIN_DEFAULT_MODE = previous;
  }
});

test('interruptor global apaga el modo predeterminado sin cambiar registros de contactos', () => {
  const previous = process.env.SALES_BRAIN_ENABLED;
  process.env.SALES_BRAIN_ENABLED = 'off';
  try {
    assert.equal(isSalesBrainEnabled(), false);
    assert.equal(getDefaultSalesBrainMode(), 'off');
  } finally {
    if (previous === undefined) delete process.env.SALES_BRAIN_ENABLED;
    else process.env.SALES_BRAIN_ENABLED = previous;
  }
});

test('SALES_BRAIN_DEFAULT_MODE=off desactiva Sales Brain en leads entrantes nuevos', () => {
  const previous = process.env.SALES_BRAIN_DEFAULT_MODE;
  process.env.SALES_BRAIN_DEFAULT_MODE = 'off';
  try {
    assert.equal(getDefaultSalesBrainMode(), 'off');
    assert.equal(buildNewInboundLeadSalesBrainDefaults().salesBrainMode, 'off');
  } finally {
    if (previous === undefined) delete process.env.SALES_BRAIN_DEFAULT_MODE;
    else process.env.SALES_BRAIN_DEFAULT_MODE = previous;
  }
});

test('Sales Brain permanece apagado incluso con la configuración antigua de producción', () => {
  const previousEnabled = process.env.SALES_BRAIN_ENABLED;
  const previousMode = process.env.SALES_BRAIN_DEFAULT_MODE;
  process.env.SALES_BRAIN_ENABLED = 'true';
  process.env.SALES_BRAIN_DEFAULT_MODE = 'copilot';
  try {
    assert.equal(isSalesBrainEnabled(), false);
    assert.equal(buildNewInboundLeadSalesBrainDefaults().salesBrainMode, 'off');
  } finally {
    if (previousEnabled === undefined) delete process.env.SALES_BRAIN_ENABLED;
    else process.env.SALES_BRAIN_ENABLED = previousEnabled;
    if (previousMode === undefined) delete process.env.SALES_BRAIN_DEFAULT_MODE;
    else process.env.SALES_BRAIN_DEFAULT_MODE = previousMode;
  }
});
