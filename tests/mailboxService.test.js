import test from 'node:test';
import assert from 'node:assert/strict';
import { MailboxService } from '../services/mailboxService.js';

function buildService() {
  const inboundMessages = [];
  const sentMessages = [];

  const repo = {
    buildInboxMessageId(providerMessageId) {
      return `in_${providerMessageId}`;
    },
    async saveInboundMessage(payload) {
      inboundMessages.push(payload);
      return { id: payload.messageId, ...payload.payload };
    },
  };

  const corporateEmailService = {
    async importCorporateEmailMessage(payload) {
      sentMessages.push(payload);
      return { id: `sent_${sentMessages.length}`, duplicate: false, ...payload };
    },
  };

  return {
    service: new MailboxService({
      mailboxRepository: repo,
      corporateEmailService,
      jwtSecret: 'test-secret',
      logger: { warn() {}, error() {} },
    }),
    inboundMessages,
    sentMessages,
  };
}

const rawSentMbox = `From - Tue Jul 28 10:00:00 2026
Message-ID: <sent-1@example.com>
From: Ventas <ventas@cliente.com>
To: Cliente <cliente@example.com>
Cc: Copia <copia@example.com>
Subject: Propuesta
Date: Tue, 28 Jul 2026 10:00:00 -0600
Content-Type: text/plain; charset=utf-8

Hola, envio la propuesta.
`;

test('importInbox with folder=sent stores messages in sent history', async () => {
  const { service, inboundMessages, sentMessages } = buildService();

  const result = await service.importInbox({
    empresaId: 'empresa-1',
    correoId: 'ventas_cliente_com',
    mailboxEmail: 'ventas@cliente.com',
    raw: rawSentMbox,
    fileName: 'Sent.mbox',
    folder: 'sent',
  });

  assert.equal(result.folder, 'sent');
  assert.equal(result.imported, 1);
  assert.equal(inboundMessages.length, 0);
  assert.equal(sentMessages.length, 1);
  assert.equal(sentMessages[0].fromAlias, 'ventas@cliente.com');
  assert.deepEqual(sentMessages[0].to, ['cliente@example.com']);
  assert.deepEqual(sentMessages[0].cc, ['copia@example.com']);
  assert.equal(sentMessages[0].subject, 'Propuesta');
});

test('importInbox without folder keeps importing into inbox', async () => {
  const { service, inboundMessages, sentMessages } = buildService();

  const result = await service.importInbox({
    empresaId: 'empresa-1',
    correoId: 'ventas_cliente_com',
    mailboxEmail: 'ventas@cliente.com',
    raw: rawSentMbox,
    fileName: 'INBOX.mbox',
  });

  assert.equal(result.folder, 'inbox');
  assert.equal(result.imported, 1);
  assert.equal(inboundMessages.length, 1);
  assert.equal(sentMessages.length, 0);
  assert.equal(inboundMessages[0].payload.from, 'ventas@cliente.com');
});
