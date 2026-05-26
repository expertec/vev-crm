import { runLastWeekLeadReactivation } from '../services/leadReactivationService.js';

function parseBooleanArg(flag) {
  return process.argv.includes(flag);
}

function readArgValue(prefix, fallback = '') {
  const entry = process.argv.find((item) => item.startsWith(`${prefix}=`));
  if (!entry) return fallback;
  return entry.slice(prefix.length + 1).trim();
}

function readNumberArg(prefix, fallback = 0) {
  const raw = readArgValue(prefix, '');
  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function printSection(title, items = [], formatter) {
  if (!items.length) return;
  console.log(`\n${title}`);
  items.forEach((item, index) => {
    console.log(formatter(item, index));
  });
}

async function main() {
  const commit = parseBooleanArg('--commit');
  const limit = Math.max(0, readNumberArg('--limit', 0));
  const fromDate = readArgValue('--from', '');
  const toDate = readArgValue('--to', '');
  const timezone = readArgValue('--tz', 'America/Monterrey') || 'America/Monterrey';
  const minSilenceHours = Math.max(12, readNumberArg('--min-silence-hours', 24));
  const baseDelayMinutes = Math.max(1, readNumberArg('--base-delay-minutes', 3));
  const spacingSeconds = Math.max(45, readNumberArg('--spacing-seconds', 95));

  const result = await runLastWeekLeadReactivation({
    commit,
    limit,
    fromDate,
    toDate,
    timezone,
    minSilenceHours,
    baseDelayMinutes,
    spacingSeconds,
  });

  console.log(`campana=${result.window.campaignId}`);
  console.log(`rango=${result.window.fromDate}..${result.window.toDate} tz=${result.window.timezone}`);
  console.log(`modo=${result.query.mode} cargados=${result.query.loadedCount} elegibles=${result.summary.eligibleCount} programados=${result.summary.scheduledCount} omitidos=${result.summary.skippedCount}`);
  if (result.query.queryError) {
    console.log(`query_fallback=${result.query.queryError}`);
  }

  const scheduledPreview = commit ? result.scheduled : result.eligible;
  printSection(
    commit ? 'Programados' : 'Dry run',
    scheduledPreview.slice(0, 20),
    (item, index) => {
      const dueAt = item.dueAt ? ` dueAt=${item.dueAt}` : '';
      return `${index + 1}. ${item.leadId} ${item.nombre || item.telefono || ''} variant=${item.variationKey} context=${item.contextKey}${dueAt}\n   ${item.message}`;
    }
  );

  printSection(
    'Omitidos',
    result.skipped.slice(0, 20),
    (item, index) => `${index + 1}. ${item.leadId} ${item.nombre || item.telefono || ''} reason=${item.reason}`
  );
}

main().catch((error) => {
  console.error('[runLastWeekLeadFollowup] fatal:', error?.message || error);
  process.exit(1);
});
