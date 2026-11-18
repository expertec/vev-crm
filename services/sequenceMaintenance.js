// Keep delivery responsive while throttling repairs of historical records.
export function createSequenceProcessor(queue, { now = Date.now, logger = console, maintenanceIntervalMs = 5 * 60000 } = {}) {
  let running = false, nextMaintenanceAt = -Infinity;
  return async function processSequences() {
    if (running) return 0;
    running = true;
    try {
      if (now() >= nextMaintenanceAt) {
        nextMaintenanceAt = now() + maintenanceIntervalMs;
        for (const [name, options] of [
          ['hydrateNextSequenceRun', { limit: 50 }],
          ['pauseLegacyRecoveredMissingDestinationSequences', { limit: 100 }],
          ['recoverMissingDestinationSequences', { limit: 5 }],
          ['resumeSalesQueuePausedSequences', { limit: 5 }],
        ]) {
          if (typeof queue[name] !== 'function') continue;
          try { await queue[name](options); }
          catch (error) { logger.warn(`[sequence-maintenance] ${name}:`, error?.message || error); }
        }
      }
      let processed = 0;
      if (typeof queue.processSequenceLeadsBatch === 'function') processed += await queue.processSequenceLeadsBatch({ limit: 25 }) || 0;
      else if (typeof queue.processDueSequenceJobs === 'function') processed += await queue.processDueSequenceJobs({ limit: 25 }) || 0;
      if (typeof queue.processQueue === 'function') processed += await queue.processQueue({ batchSize: 200 }) || 0;
      return processed;
    } finally { running = false; }
  };
}

// Pagination avoids repairing the same first 50 empty arrays indefinitely.
export function createLegacySequenceScanner(db) {
  let cursor = null;
  return async function scan(limit) {
    let query = db.collection('leads').where('secuenciasActivas', '!=', null)
      .orderBy('secuenciasActivas').orderBy('__name__')
      .select('secuenciasActivas', 'nextSequenceRunAt', 'archived', 'isArchived', 'archivedAt');
    if (cursor) query = query.startAfter(cursor);
    const page = await query.limit(limit).get();
    cursor = page.docs.length === limit ? page.docs.at(-1) : null;
    return page;
  };
}
