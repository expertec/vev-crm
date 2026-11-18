import test from 'node:test';
import assert from 'node:assert/strict';
import sharp from 'sharp';
import { MailboxService } from '../services/mailboxService.js';
import { CloudflareEmailSendingClient } from '../services/cloudflareEmailSendingClient.js';
import { withMailboxPhoto } from '../utils/mailboxProfile.js';
import { prepareMailboxPhoto } from '../utils/mailboxProfileImage.js';
import { createMailboxController } from '../controllers/mailboxController.js';

const context = { empresaId: 'company', correoId: 'sender', mailboxEmail: 'sender@example.com' };
function setup() {
  const record = { email: context.mailboxEmail, displayName: 'Nombre anterior', mailboxEnabled: true, photoUrl: '' };
  const sent = [];
  let uploads = 0;
  const service = new MailboxService({
    mailboxRepository: {
      async getCorporateEmailById(company, id) { return company === 'company' && id === 'sender' ? { ...record } : null; },
      async setMailboxConfig({ patch }) { Object.assign(record, patch); },
      async saveMailboxPhoto() { uploads += 1; return 'https://images.example.com/profile.webp'; },
    },
    corporateEmailService: { async sendCorporateEmail(payload) { sent.push(payload); return payload; } },
  });
  return { service, record, sent, uploads: () => uploads };
}

test('guarda nombre y foto en el buzón y los usa en los próximos envíos', async () => {
  const { service, sent } = setup();
  const profile = await service.updateProfile({ ...context, displayName: 'Sergio & Ventas', photo: { buffer: Buffer.from('photo') } });
  assert.equal(profile.displayName, 'Sergio & Ventas');
  assert.equal(profile.photoUrl, 'https://images.example.com/profile.webp');
  await service.send({ ...context, to: ['to@example.com'], subject: 'Hola', text: 'Mensaje <seguro>' });
  assert.equal(sent[0].fromName, profile.displayName);
  assert.equal(sent[0].fromAlias, context.mailboxEmail);
  assert.match(sent[0].html, /Mensaje &lt;seguro&gt;/);
  assert.match(sent[0].html, /Sergio &amp; Ventas/);
  assert.match(sent[0].html, /<img src="https:\/\/images.example.com\/profile.webp"/);
});

test('quitar foto conserva nombre y deja de incluirla en futuros correos', async () => {
  const { service, sent, record } = setup();
  record.photoUrl = 'https://images.example.com/old.webp';
  await service.updateProfile({ ...context, removePhoto: true, displayName: 'Sergio' });
  await service.send({ ...context, to: ['to@example.com'], subject: 'Hola', text: 'Mensaje', html: '<p>Mensaje</p>' });
  assert.equal(sent[0].fromName, 'Sergio');
  assert.equal(sent[0].html, '<p>Mensaje</p>');
});

test('rechaza otro buzón, nombres con cabeceras y archivos demasiado grandes antes de guardar', async () => {
  const { service, uploads } = setup();
  await assert.rejects(service.updateProfile({ ...context, mailboxEmail: 'other@example.com', displayName: 'Otro' }), { code: 'MAILBOX_PROFILE_NOT_ALLOWED' });
  await assert.rejects(service.updateProfile({ ...context, displayName: 'Sergio\r\nBcc:other@example.com' }), { code: 'MAILBOX_DISPLAY_NAME_INVALID' });
  await assert.rejects(service.updateProfile({ ...context, photo: { buffer: Buffer.alloc(512 * 1024 + 1) } }), { code: 'MAILBOX_PHOTO_SIZE_INVALID' });
  assert.equal(uploads(), 0);
});

test('la firma queda dentro del body y escapa atributos del perfil', () => {
  const result = withMailboxPhoto({ html: '<html><body><p>Hola</p></body></html>', profile: { photoUrl: 'https://example.com/a?x="', displayName: '<script>x</script>', email: context.mailboxEmail } });
  assert.ok(result.indexOf('data-mailbox-profile') < result.indexOf('</body>'));
  assert.match(result, /&lt;script&gt;x&lt;\/script&gt;/);
  assert.match(result, /&quot;/);
});

test('procesa una imagen real a WebP cuadrada y rechaza SVG y archivos falsos', async () => {
  const input = await sharp({ create: { width: 400, height: 100, channels: 3, background: '#008844' } }).png().toBuffer();
  const output = await prepareMailboxPhoto(input);
  const metadata = await sharp(output).metadata();
  assert.equal(metadata.format, 'webp');
  assert.equal(metadata.width, 256);
  assert.equal(metadata.height, 256);
  assert.equal(metadata.exif, undefined);
  await assert.rejects(prepareMailboxPhoto(Buffer.from('fake image')), { code: 'MAILBOX_PHOTO_INVALID' });
  await assert.rejects(prepareMailboxPhoto(Buffer.from('<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10"></svg>')), { code: 'MAILBOX_PHOTO_INVALID' });
});

test('Cloudflare recibe nombre con mayúsculas y dirección validada; sin nombre mantiene compatibilidad', async () => {
  const client = new CloudflareEmailSendingClient({ apiToken: 'test', accountId: 'account' });
  const calls = [];
  client.http.post = async (_url, payload) => { calls.push(payload); return { data: { success: true, result: {} } }; };
  await client.sendEmail({ from: 'Sender@Example.com', fromName: 'Sergio Álvarez', to: ['to@example.com'], text: 'Mensaje' });
  assert.deepEqual(calls[0].from, { address: 'sender@example.com', name: 'Sergio Álvarez' });
  await client.sendEmail({ from: 'Sender@Example.com', to: ['to@example.com'], text: 'Mensaje' });
  assert.equal(calls[1].from, 'sender@example.com');
});

test('el controlador usa el buzón autenticado aunque el cuerpo intente indicar otro', async () => {
  const { service } = setup();
  const controller = createMailboxController({ service });
  let response;
  await controller.updateProfile({ mailbox: { empresaId: context.empresaId, correoId: context.correoId, email: context.mailboxEmail }, body: { displayName: 'Sergio', empresaId: 'other', correoId: 'other', email: 'other@example.com' } }, { json: (data) => { response = data; } });
  assert.equal(response.success, true);
  assert.equal(response.mailbox.email, context.mailboxEmail);
  assert.equal(response.mailbox.displayName, 'Sergio');
});
