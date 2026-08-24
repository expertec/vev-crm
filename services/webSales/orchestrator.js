import {
  WEB_CONVERSION_GOALS,
  WEB_SALES_STAGES,
  ensureWebSalesState,
  isLikelyWebLead,
} from './state.js';

function cleanText(value = '', max = 500) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, max);
}

function hasTerminalStatus(lead = {}) {
  const status = cleanText(lead?.estado || '', 80).toLowerCase();
  const tags = Array.isArray(lead?.etiquetas) ? lead.etiquetas.map((tag) => cleanText(tag, 80).toLowerCase()) : [];
  return lead?.stopSequences === true
    || status === 'compro'
    || status === 'cliente'
    || status === 'no interesado'
    || tags.includes('compro')
    || tags.includes('detenersecuencia')
    || tags.includes('stopsequences');
}

function hasHumanOwner(lead = {}) {
  return Boolean(
    lead?.salesBrainHumanControl === true
    || lead?.humanControl === true
    || cleanText(lead?.salesOwner || lead?.assignedTo || '', 180)
    || cleanText(lead?.queue?.status || '', 80) === 'claimed'
  );
}

export function determineNextWebSalesAction(lead = {}) {
  if (!isLikelyWebLead(lead)) {
    return { action: 'wait', reason: 'not_web_lead', sequenceTrigger: '', priority: 0 };
  }
  const webSales = ensureWebSalesState(lead);
  if (hasTerminalStatus(lead) || webSales.stage === WEB_SALES_STAGES.WON || webSales.stage === WEB_SALES_STAGES.LOST) {
    return { action: 'stop', reason: 'terminal_or_stop', sequenceTrigger: '', priority: 0, webSales };
  }
  if (hasHumanOwner(lead)) {
    return { action: 'wait', reason: 'human_control', sequenceTrigger: '', priority: 0, webSales };
  }

  const goal = webSales.nextConversionGoal;
  if (goal === WEB_CONVERSION_GOALS.GET_SAMPLE_FORM_COMPLETED) {
    return {
      action: 'send_sequence',
      reason: 'form_pending',
      sequenceTrigger: webSales.sample?.formStartedAt ? 'Web_FormAbandoned' : 'Web_FormPending',
      priority: 35,
      webSales,
    };
  }
  if (goal === WEB_CONVERSION_GOALS.GET_SAMPLE_OPEN) {
    return {
      action: 'send_sequence',
      reason: 'sample_not_opened',
      sequenceTrigger: 'Web_SampleNotOpened',
      priority: 45,
      webSales,
    };
  }
  if (goal === WEB_CONVERSION_GOALS.GET_SAMPLE_FEEDBACK) {
    return {
      action: 'sales_queue',
      reason: 'sample_opened_needs_feedback',
      sequenceTrigger: 'Web_SampleOpenedNoReply',
      priority: Number(webSales.sample?.openCount || 0) >= 3 ? 82 : 65,
      webSales,
    };
  }
  if (goal === WEB_CONVERSION_GOALS.GET_PURCHASE_DECISION || goal === WEB_CONVERSION_GOALS.GET_DEPOSIT) {
    return {
      action: 'sales_queue',
      reason: 'closing_required',
      sequenceTrigger: goal === WEB_CONVERSION_GOALS.GET_DEPOSIT ? 'Web_PaymentPending' : 'Web_Closing',
      priority: goal === WEB_CONVERSION_GOALS.GET_DEPOSIT ? 95 : 85,
      webSales,
    };
  }
  if (goal === WEB_CONVERSION_GOALS.STOP) {
    return { action: 'stop', reason: 'goal_stop', sequenceTrigger: '', priority: 0, webSales };
  }
  return { action: 'wait', reason: 'no_automation_for_goal', sequenceTrigger: '', priority: 0, webSales };
}
