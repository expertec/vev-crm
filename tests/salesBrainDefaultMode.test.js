import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildNewInboundLeadSalesBrainDefaults,
  getDefaultSalesBrainMode,
} from '../services/salesBrain/defaultMode.js';

test('leads entrantes nuevos activan Sales Brain copilot por defecto', () => {
  const previous = process.env.SALES_BRAIN_DEFAULT_MODE;
  delete process.env.SALES_BRAIN_DEFAULT_MODE;
  try {
    const defaults = buildNewInboundLeadSalesBrainDefaults();
    assert.equal(getDefaultSalesBrainMode(), 'copilot');
    assert.equal(defaults.salesBrainMode, 'copilot');
    assert.equal(defaults.queue.status, 'automation');
    assert.equal(defaults.queue.priority, 0);
    assert.equal(defaults.salesContext.businessType, null);
  } finally {
    if (previous === undefined) delete process.env.SALES_BRAIN_DEFAULT_MODE;
    else process.env.SALES_BRAIN_DEFAULT_MODE = previous;
  }
});

test('SALES_BRAIN_DEFAULT_MODE=off no apaga leads entrantes nuevos', () => {
  const previous = process.env.SALES_BRAIN_DEFAULT_MODE;
  process.env.SALES_BRAIN_DEFAULT_MODE = 'off';
  try {
    assert.equal(getDefaultSalesBrainMode(), 'copilot');
    assert.equal(buildNewInboundLeadSalesBrainDefaults().salesBrainMode, 'copilot');
  } finally {
    if (previous === undefined) delete process.env.SALES_BRAIN_DEFAULT_MODE;
    else process.env.SALES_BRAIN_DEFAULT_MODE = previous;
  }
});
