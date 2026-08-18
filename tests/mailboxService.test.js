import test from 'node:test';
import assert from 'node:assert/strict';
import { MailboxService } from '../services/mailboxService.js';
import { parseEmailMessage } from '../services/mailboxImportParser.js';
import { hashPassword } from '../utils/mailboxAuth.js';

function buildService() {
  const inboundMessages = [];
  const sentMessages = [];
  const inboundAttachments = [];
  const mailingLists = new Map();
  const drafts = new Map();
  const mailboxRecords = new Map([
    ['empresa-1/ventas_cliente_com', {
      id: 'ventas_cliente_com',
      email: 'ventas@cliente.com',
      mailboxEnabled: true,
      passwordHash: hashPassword('Actual123'),
      displayName: 'Ventas',
      domain: 'cliente.com',
    }],
  ]);

  const repo = {
    buildInboxMessageId(providerMessageId) {
      return `in_${providerMessageId}`;
    },
    async saveInboundAttachment(payload) {
      inboundAttachments.push(payload);
      return { storagePath: `mailbox/test/${payload.attachmentId}` };
    },
    async saveInboundMessage(payload) {
      inboundMessages.push(payload);
      return { id: payload.messageId, ...payload.payload };
    },
    async listMailingLists() {
      return Array.from(mailingLists.values());
    },
    async saveMailingList({ listId, payload }) {
      const id = listId || `list_${mailingLists.size + 1}`;
      const item = { id, ...payload };
      mailingLists.set(id, item);
      return item;
    },
    async deleteMailingList({ listId }) {
      return mailingLists.delete(listId);
    },
    async listDrafts() {
      return Array.from(drafts.values());
    },
    async saveDraft({ draftId, payload }) {
      const id = draftId || `draft_${drafts.size + 1}`;
      const item = {
        id,
        ...(drafts.get(id) || { createdAt: new Date('2026-07-30T10:00:00.000Z') }),
        ...payload,
        updatedAt: new Date('2026-07-30T10:05:00.000Z'),
      };
      drafts.set(id, item);
      return item;
    },
    async deleteDraft({ draftId }) {
      return drafts.delete(draftId);
    },
    async getCorporateEmailById(empresaId, correoId) {
      return mailboxRecords.get(`${empresaId}/${correoId}`) || null;
    },
    async findCorporateEmailByAddress(address) {
      const normalized = String(address || '').trim().toLowerCase();
      for (const [key, data] of mailboxRecords.entries()) {
        if (String(data.email || '').trim().toLowerCase() === normalized) {
          const [empresaId, correoId] = key.split('/');
          return { empresaId, correoId, data };
        }
      }
      return null;
    },
    async setMailboxConfig({ empresaId, correoId, patch = {} }) {
      const key = `${empresaId}/${correoId}`;
      const next = { ...(mailboxRecords.get(key) || {}), ...patch };
      mailboxRecords.set(key, next);
      return next;
    },
  };

  const corporateEmailService = {
    async importCorporateEmailMessage(payload) {
      sentMessages.push(payload);
      return { id: `sent_${sentMessages.length}`, duplicate: false, ...payload };
    },
    async sendCorporateEmail(payload) {
      sentMessages.push(payload);
      return { provider: 'test', ...payload };
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
    inboundAttachments,
    mailingLists,
    drafts,
    mailboxRecords,
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

const inlineImageMbox = `From - Tue Jul 28 11:00:00 2026
Message-ID: <inline-1@example.com>
From: Winnie <winnie@example.com>
To: Ventas <ventas@cliente.com>
Subject: Imagen inline
Date: Tue, 28 Jul 2026 11:00:00 -0600
Content-Type: multipart/related; boundary="rel-1"

--rel-1
Content-Type: text/html; charset=utf-8
Content-Transfer-Encoding: quoted-printable

<html><body><p>Hola</p><img src=3D"cid:logo-1@example.com"></body></html>
--rel-1
Content-Type: image/png; name="logo.png"
Content-Transfer-Encoding: base64
Content-ID: <logo-1@example.com>
Content-Disposition: inline; filename="logo.png"

aW1hZ2UtYnl0ZXM=
--rel-1--
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

test('parseEmailMessage preserves inline cid images as data urls', () => {
  const parsed = parseEmailMessage(inlineImageMbox);

  assert.equal(parsed.subject, 'Imagen inline');
  assert.equal(parsed.attachments.length, 1);
  assert.equal(parsed.attachments[0].contentId, 'logo-1@example.com');
  assert.equal(parsed.attachments[0].contentType, 'image/png');
  assert.match(parsed.htmlBody, /src="data:image\/png;base64,aW1hZ2UtYnl0ZXM="/);
});

test('importInbox stores imported inline images as attachments metadata', async () => {
  const { service, inboundMessages, inboundAttachments } = buildService();

  const result = await service.importInbox({
    empresaId: 'empresa-1',
    correoId: 'ventas_cliente_com',
    mailboxEmail: 'ventas@cliente.com',
    raw: inlineImageMbox,
    fileName: 'INBOX.mbox',
  });

  assert.equal(result.imported, 1);
  assert.equal(inboundAttachments.length, 1);
  assert.equal(inboundAttachments[0].filename, 'logo.png');
  assert.equal(inboundMessages[0].payload.attachments.length, 1);
  assert.equal(inboundMessages[0].payload.attachments[0].contentId, 'logo-1@example.com');
  assert.match(inboundMessages[0].payload.htmlBody, /data:image\/png;base64,aW1hZ2UtYnl0ZXM=/);
});

test('mailing lists are validated, deduplicated and persisted per mailbox', async () => {
  const { service } = buildService();

  const saved = await service.saveMailingList({
    empresaId: 'empresa-1',
    correoId: 'ventas_cliente_com',
    name: 'Prospectos',
    members: ['UNO@EXAMPLE.COM', 'uno@example.com', 'dos@example.com', 'invalido'],
  });

  assert.equal(saved.name, 'Prospectos');
  assert.deepEqual(saved.members, ['uno@example.com', 'dos@example.com']);
  assert.equal(saved.memberCount, 2);

  const items = await service.listMailingLists({
    empresaId: 'empresa-1',
    correoId: 'ventas_cliente_com',
  });
  assert.equal(items.length, 1);
  assert.equal(items[0].id, saved.id);

  const result = await service.deleteMailingList({
    empresaId: 'empresa-1',
    correoId: 'ventas_cliente_com',
    listId: saved.id,
  });
  assert.equal(result.deleted, true);
});

test('send rejects more recipients than mailbox limit', async () => {
  const { service } = buildService();

  await assert.rejects(
    () => service.send({
      empresaId: 'empresa-1',
      mailboxEmail: 'ventas@cliente.com',
      to: Array.from({ length: 101 }, (_v, index) => `persona${index}@example.com`),
      subject: 'Hola',
      text: 'Mensaje',
    }),
    /Máximo 100 destinatarios/
  );
});

test('changePassword verifies current password and updates mailbox credentials', async () => {
  const { service } = buildService();

  await assert.rejects(
    () => service.changePassword({
      empresaId: 'empresa-1',
      correoId: 'ventas_cliente_com',
      mailboxEmail: 'ventas@cliente.com',
      currentPassword: 'Incorrecta123',
      newPassword: 'Nueva123',
    }),
    /contraseña actual no es correcta/i
  );

  const result = await service.changePassword({
    empresaId: 'empresa-1',
    correoId: 'ventas_cliente_com',
    mailboxEmail: 'ventas@cliente.com',
    currentPassword: 'Actual123',
    newPassword: 'Nueva123',
  });

  assert.equal(result.changed, true);
  await assert.rejects(
    () => service.login({ email: 'ventas@cliente.com', password: 'Actual123' }),
    /Correo o contraseña incorrectos/
  );
  const login = await service.login({ email: 'ventas@cliente.com', password: 'Nueva123' });
  assert.ok(login.token);
});

test('refreshSession renews mailbox tokens and is revoked after password change', async () => {
  const { service, mailboxRecords } = buildService();

  const login = await service.login({ email: 'ventas@cliente.com', password: 'Actual123' });
  assert.ok(login.token);
  assert.ok(login.refreshToken);

  const refreshed = await service.refreshSession({ refreshToken: login.refreshToken });
  assert.ok(refreshed.token);
  assert.ok(refreshed.refreshToken);
  assert.equal(refreshed.mailbox.email, 'ventas@cliente.com');

  mailboxRecords.set('empresa-1/ventas_cliente_com', {
    ...mailboxRecords.get('empresa-1/ventas_cliente_com'),
    passwordUpdatedAt: new Date(Date.now() + 1000),
  });

  await assert.rejects(
    () => service.refreshSession({ refreshToken: login.refreshToken }),
    /contraseña cambió/i
  );
});

test('drafts are saved, listed and deleted per mailbox', async () => {
  const { service } = buildService();

  const saved = await service.saveDraft({
    empresaId: 'empresa-1',
    correoId: 'ventas_cliente_com',
    to: 'cliente@example.com',
    cc: ['copia@example.com'],
    subject: 'Cotización',
    bodyText: 'Hola cliente',
    bodyHtml: '<p>Hola cliente</p>',
  });

  assert.equal(saved.id, 'draft_1');
  assert.deepEqual(saved.to, ['cliente@example.com']);
  assert.deepEqual(saved.cc, ['copia@example.com']);
  assert.equal(saved.subject, 'Cotización');

  const updated = await service.saveDraft({
    empresaId: 'empresa-1',
    correoId: 'ventas_cliente_com',
    draftId: saved.id,
    to: ['cliente@example.com', 'cliente@example.com'],
    subject: 'Cotización actualizada',
    bodyText: 'Actualizado',
  });
  assert.equal(updated.id, saved.id);
  assert.deepEqual(updated.to, ['cliente@example.com']);
  assert.equal(updated.subject, 'Cotización actualizada');

  const items = await service.listDrafts({
    empresaId: 'empresa-1',
    correoId: 'ventas_cliente_com',
  });
  assert.equal(items.length, 1);
  assert.equal(items[0].id, saved.id);

  const result = await service.deleteDraft({
    empresaId: 'empresa-1',
    correoId: 'ventas_cliente_com',
    draftId: saved.id,
  });
  assert.equal(result.deleted, true);
});
