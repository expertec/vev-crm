import test from 'node:test';
import assert from 'node:assert/strict';
import {
  hashPassword,
  verifyPassword,
  signMailboxToken,
  verifyMailboxToken,
} from '../utils/mailboxAuth.js';

test('hashPassword/verifyPassword: acepta la correcta y rechaza la incorrecta', () => {
  const hash = hashPassword('SuperSecreta123');
  assert.match(hash, /^scrypt\$[0-9a-f]+\$[0-9a-f]+$/);
  assert.equal(verifyPassword('SuperSecreta123', hash), true);
  assert.equal(verifyPassword('otra', hash), false);
  assert.equal(verifyPassword('SuperSecreta123', 'basura'), false);
});

test('signMailboxToken/verifyMailboxToken: firma y valida claims', () => {
  const secret = 'test-secret';
  const token = signMailboxToken(
    { empresaId: 'n1', correoId: 'c1', email: 'ventas@dominio.com' },
    { secret, expiresInSeconds: 60 }
  );
  const claims = verifyMailboxToken(token, { secret });
  assert.equal(claims.empresaId, 'n1');
  assert.equal(claims.correoId, 'c1');
  assert.equal(claims.email, 'ventas@dominio.com');
});

test('verifyMailboxToken: rechaza firma con otro secret y token manipulado', () => {
  const token = signMailboxToken({ email: 'a@b.com' }, { secret: 'uno', expiresInSeconds: 60 });
  assert.equal(verifyMailboxToken(token, { secret: 'dos' }), null);
  const [data] = token.split('.');
  assert.equal(verifyMailboxToken(`${data}.firmafalsa`, { secret: 'uno' }), null);
});

test('verifyMailboxToken: rechaza token expirado', () => {
  const secret = 'test-secret';
  const token = signMailboxToken({ email: 'a@b.com' }, { secret, expiresInSeconds: -10 });
  assert.equal(verifyMailboxToken(token, { secret }), null);
});
