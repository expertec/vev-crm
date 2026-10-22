import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { createSampleAudioTranscriber, storedSampleAudioPath } from '../services/sampleAudioTranscription.js';

const bucketName = 'crm.example.app';
const url = `https://storage.googleapis.com/${bucketName}/audios/5218311760335-123.ogg?signature=fixture`;
const bucket = (chunks = [Buffer.from('audio-fixture')]) => ({ name: bucketName, file: (path) => { assert.equal(path, 'audios/5218311760335-123.ogg'); return { createReadStream: () => Readable.from(chunks) }; } });

test('transcribe el OGG de WhatsApp con multipart y conserva la respuesta en español', async () => {
  const transcribe = createSampleAudioTranscriber({ bucket: bucket(), apiKey: () => 'test-key', model: () => 'gpt-transcribe', fetchImpl: async (endpoint, options) => {
    assert.equal(endpoint, 'https://api.openai.com/v1/audio/transcriptions');
    assert.equal(options.headers.Authorization, 'Bearer test-key');
    assert.equal(options.headers['Content-Type'], undefined, 'fetch debe agregar el boundary multipart');
    assert.equal(options.body.get('model'), 'gpt-transcribe');
    assert.equal(options.body.get('language'), 'es');
    assert.equal(options.body.get('file').name, 'nota-de-voz.ogg');
    assert.equal(options.body.get('file').type, 'audio/ogg');
    assert.equal(await options.body.get('file').text(), 'audio-fixture');
    return { ok: true, json: async () => ({ text: 'Mi negocio se llama Audio Norte' }) };
  } });
  assert.equal(await transcribe({ mediaUrl: url }), 'Mi negocio se llama Audio Norte');
});

test('solo lee la carpeta de audios del bucket propio, sin acceder a URLs externas', async () => {
  assert.equal(storedSampleAudioPath(url, bucketName), 'audios/5218311760335-123.ogg');
  assert.equal(storedSampleAudioPath(`https://firebasestorage.googleapis.com/v0/b/${bucketName}/o/audios%2Fvoice.ogg?alt=media`, bucketName), 'audios/voice.ogg');
  const invalid = ['https://evil.example/audio.ogg', 'http://localhost/audio.ogg', `https://storage.googleapis.com/another-bucket/audios/voice.ogg`, `https://storage.googleapis.com/${bucketName}/docs/private.ogg`, `https://storage.googleapis.com/${bucketName}/audios%2F..%2Fprivate.ogg`];
  let requests = 0;
  const transcribe = createSampleAudioTranscriber({ bucket: bucket(), apiKey: () => 'key', fetchImpl: async () => { requests++; } });
  for (const mediaUrl of invalid) await assert.rejects(transcribe({ mediaUrl }), /almacenamiento/);
  assert.equal(requests, 0);
});

test('rechaza audios vacíos o demasiado grandes antes de consumir la API', async () => {
  let requests = 0;
  for (const chunks of [[], [Buffer.alloc(24 * 1024 * 1024 + 1)]]) {
    const transcribe = createSampleAudioTranscriber({ bucket: bucket(chunks), apiKey: () => 'key', fetchImpl: async () => { requests++; } });
    await assert.rejects(transcribe({ mediaUrl: url }), /vacío|24 MB/);
  }
  assert.equal(requests, 0);
});

test('detecta fallos de API y transcripciones vacías para pedir al cliente que reenvíe el audio', async () => {
  const denied = createSampleAudioTranscriber({ bucket: bucket(), apiKey: () => 'key', fetchImpl: async () => ({ ok: false, status: 429, json: async () => ({ error: { message: 'rate limit' } }) }) });
  await assert.rejects(denied({ mediaUrl: url }), /HTTP 429/);
  const silence = createSampleAudioTranscriber({ bucket: bucket(), apiKey: () => 'key', fetchImpl: async () => ({ ok: true, json: async () => ({ text: ' ' }) }) });
  await assert.rejects(silence({ mediaUrl: url }), /voz comprensible/);
});
