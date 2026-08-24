import { getFactValue } from './memory.js';

function safeStatus(value = '') {
  return String(value || '').trim().toLowerCase();
}

function hasTag(lead = {}, tag = '') {
  const target = safeStatus(tag);
  const tags = Array.isArray(lead?.etiquetas) ? lead.etiquetas.map(safeStatus) : [];
  return tags.includes(target);
}

function hasMetaSource(lead = {}, analysis = {}) {
  const source = safeStatus(lead?.source || '');
  const campaign = safeStatus(lead?.campaign || '');
  return source === 'meta_ads'
    || campaign === 'whatsapp_click_to_chat'
    || Boolean(lead?.lastMetaAttribution || lead?.metaAttribution)
    || analysis?.signals?.includes?.('meta_ad');
}

function hasWebSalesEvent(lead = {}, path = '') {
  const [group, field] = String(path || '').split('.');
  if (!group || !field) return false;
  return Boolean(lead?.webSales?.[group]?.[field]);
}

export function calculateLeadScore({ lead = {}, analysis = {}, memory = {} } = {}) {
  const signals = new Set(Array.isArray(analysis?.signals) ? analysis.signals : []);
  const previousApplied = lead?.salesScoreState?.appliedSignals
    && typeof lead.salesScoreState.appliedSignals === 'object'
    ? lead.salesScoreState.appliedSignals
    : {};
  const appliedSignals = { ...previousApplied };
  const newlyApplied = {};
  const mark = (key, points, active = false) => {
    if (!points || !active) return;
    if (!appliedSignals[key]) {
      newlyApplied[key] = points;
    }
    appliedSignals[key] = points;
  };

  mark('meta_ad', 10, hasMetaSource(lead, analysis));
  mark('answered', 5, signals.has('answered'));
  mark('business_identified', 10, Boolean(analysis?.businessType || getFactValue(memory, 'businessType')));
  mark('primary_need_identified', 10, Boolean(analysis?.primaryNeed || getFactValue(memory, 'primaryNeed')));
  mark('asked_price', 15, analysis?.intent === 'wants_price' || signals.has('asked_price'));
  mark('asked_examples', 10, analysis?.intent === 'wants_examples' || signals.has('asked_examples'));
  mark('asks_how_to_start', 25, analysis?.intent === 'asks_how_to_start' || signals.has('asks_how_to_start'));
  mark('ready_to_buy', 30, analysis?.intent === 'ready_to_buy' || signals.has('ready_to_buy') || signals.has('wants_to_buy'));
  mark('sample_or_web_sent', 8, hasTag(lead, 'WebEnviada') || Boolean(getFactValue(memory, 'receivedSample')));
  mark('examples_sent', 5, Boolean(getFactValue(memory, 'receivedExamples') || signals.has('examples_sent')));
  mark('web_sample_offered', 5, hasWebSalesEvent(lead, 'sample.offeredAt'));
  mark('web_form_opened', 5, hasWebSalesEvent(lead, 'sample.formOpenedAt'));
  mark('web_form_started', 7, hasWebSalesEvent(lead, 'sample.formStartedAt'));
  mark('web_form_completed', 10, hasWebSalesEvent(lead, 'sample.formCompletedAt'));
  mark('web_sample_opened', 10, hasWebSalesEvent(lead, 'sample.firstOpenedAt'));
  mark('web_sample_opened_multiple', 6, Number(lead?.webSales?.sample?.openCount || 0) >= 3);
  mark('web_sample_feedback', 15, hasWebSalesEvent(lead, 'sample.feedbackReceivedAt') || signals.has('sample_feedback'));
  mark('web_sample_change_request', 12, signals.has('sample_change_request'));
  mark('web_asks_domain', 8, signals.has('asks_domain'));
  mark('web_ready_for_deposit', 20, signals.has('ready_for_deposit'));
  mark('warm_interest', 5, analysis?.interestLevel === 'warm');
  mark('hot_interest', 10, analysis?.interestLevel === 'hot');
  mark('no_interest', -50, analysis?.intent === 'no_interest' || signals.has('no_interest'));
  mark('stop_requested', -100, signals.has('stop_requested'));
  mark('automated_reply', -15, analysis?.automated || signals.has('automated_reply'));

  const breakdown = Object.fromEntries(
    Object.entries(appliedSignals)
      .filter(([, value]) => Number.isFinite(Number(value)) && Number(value) !== 0)
  );
  const rawTotal = Object.values(breakdown).reduce((acc, value) => acc + Number(value || 0), 0);
  const total = Math.max(0, Math.min(100, rawTotal));
  return {
    total,
    rawTotal,
    breakdown,
    newlyApplied,
    appliedSignals: breakdown,
  };
}
