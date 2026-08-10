export const CONVERSATIONAL_WELCOME_TRIGGER = 'BienvenidaConversacional';

export const CONVERSATIONAL_WELCOME_SEQUENCE = Object.freeze({
  id: CONVERSATIONAL_WELCOME_TRIGGER,
  trigger: CONVERSATIONAL_WELCOME_TRIGGER,
  active: true,
  messages: Object.freeze([
    {
      type: 'question',
      message: 'Hola 👋 Para orientarte mejor, ¿a qué se dedica tu negocio?',
      delay: 0,
      saveTo: 'salesContext.businessType',
      objective: 'understand_business',
      waitForReply: true,
    },
    {
      type: 'question',
      message: '¿Cómo consigues clientes normalmente?',
      delay: 0,
      saveTo: 'salesContext.customerAcquisition',
      objective: 'understand_customer_acquisition',
      waitForReply: true,
    },
    {
      type: 'question',
      message: '¿Qué te gustaría mejorar más en este momento: recibir más clientes, vender más o que más personas conozcan tu negocio?',
      delay: 0,
      saveTo: 'salesContext.primaryGoal',
      objective: 'understand_primary_goal',
      waitForReply: true,
    },
    {
      type: 'question',
      message: '¿Ya has intentado anunciar tu negocio en internet? ¿Cómo te fue?',
      delay: 0,
      saveTo: 'salesContext.previousExperience',
      objective: 'understand_previous_experience',
      waitForReply: true,
    },
  ]),
});

export function getBuiltinSequenceDefinition(trigger = '') {
  const safe = String(trigger || '').trim().toLowerCase();
  if (safe === CONVERSATIONAL_WELCOME_TRIGGER.toLowerCase()) {
    return {
      ...CONVERSATIONAL_WELCOME_SEQUENCE,
      messages: CONVERSATIONAL_WELCOME_SEQUENCE.messages.map((step) => ({ ...step })),
    };
  }
  return null;
}
