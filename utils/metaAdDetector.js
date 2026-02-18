const AD_CONTEXT_TYPES = [
  'extendedTextMessage',
  'imageMessage',
  'videoMessage',
  'documentMessage',
  'buttonsResponseMessage',
  'templateButtonReplyMessage',
];

function hasExternalAdReplyInMessageContainer(container) {
  if (!container || typeof container !== 'object') return false;

  for (const type of AD_CONTEXT_TYPES) {
    if (container?.[type]?.contextInfo?.externalAdReply) return true;
  }

  // Fallback: detectar contextInfo.externalAdReply en cualquier tipo de mensaje.
  for (const value of Object.values(container)) {
    if (value && typeof value === 'object' && value?.contextInfo?.externalAdReply) return true;
  }

  return false;
}

export function isMessageFromMetaAd(msg) {
  const root = msg?.message;
  if (!root || typeof root !== 'object') return false;

  const stack = [root];
  const visited = new Set();

  while (stack.length > 0) {
    const node = stack.pop();
    if (!node || typeof node !== 'object' || visited.has(node)) continue;
    visited.add(node);

    if (hasExternalAdReplyInMessageContainer(node)) return true;

    const wrappedMessages = [
      node?.ephemeralMessage?.message,
      node?.viewOnceMessage?.message,
      node?.viewOnceMessageV2?.message,
      node?.viewOnceMessageV2Extension?.message,
      node?.deviceSentMessage?.message,
      node?.editedMessage?.message,
      node?.protocolMessage?.editedMessage,
    ];

    for (const nested of wrappedMessages) {
      if (nested && typeof nested === 'object') stack.push(nested);
    }

    for (const value of Object.values(node)) {
      if (!value || typeof value !== 'object') continue;
      if (value?.message && typeof value.message === 'object') stack.push(value.message);
      if (value?.contextInfo?.quotedMessage && typeof value.contextInfo.quotedMessage === 'object') {
        stack.push(value.contextInfo.quotedMessage);
      }
    }
  }

  return false;
}

