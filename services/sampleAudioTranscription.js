import { recordOpenAiUsage } from './openAiUsage.js';

const MAX_AUDIO_BYTES = 24 * 1024 * 1024;
const AUDIO_TYPES = { ogg: 'audio/ogg', mp3: 'audio/mpeg', mp4: 'audio/mp4', m4a: 'audio/mp4', wav: 'audio/wav', webm: 'audio/webm', flac: 'audio/flac' };

// Only read audio already stored by our WhatsApp receiver; never fetch a supplied URL.
export function storedSampleAudioPath(mediaUrl, bucketName) {
  try {
    const url = new URL(mediaUrl);
    if (url.protocol !== 'https:' || url.username || url.password || url.port) return '';
    let name = '';
    if (url.hostname === 'storage.googleapis.com') {
      const prefix = `/${bucketName}/`;
      if (url.pathname.startsWith(prefix)) name = decodeURIComponent(url.pathname.slice(prefix.length));
    } else if (url.hostname === 'firebasestorage.googleapis.com') {
      const prefix = `/v0/b/${bucketName}/o/`;
      if (url.pathname.startsWith(prefix)) name = decodeURIComponent(url.pathname.slice(prefix.length));
    }
    return /^audios\/[^/\\\u0000]+\.(ogg|mp3|mp4|m4a|wav|webm|flac)$/i.test(name) ? name : '';
  } catch { return ''; }
}

async function readAudio(file) {
  const stream = file.createReadStream();
  const timer = setTimeout(() => stream.destroy(new Error('La descarga del audio tardó demasiado.')), 30_000);
  const chunks = [];
  let size = 0;
  try {
    for await (const chunk of stream) {
      size += chunk.length;
      if (size > MAX_AUDIO_BYTES) throw new Error('El audio supera el límite de 24 MB.');
      chunks.push(chunk);
    }
    if (!size) throw new Error('El archivo de audio está vacío.');
    return Buffer.concat(chunks);
  } finally {
    clearTimeout(timer);
    stream.destroy();
  }
}

export function createSampleAudioTranscriber({ bucket, fetchImpl = fetch, apiKey = () => process.env.OPENAI_API_KEY, model = () => process.env.OPENAI_SAMPLE_TRANSCRIPTION_MODEL || 'gpt-transcribe' }) {
  return async ({ mediaUrl }) => {
    const name = storedSampleAudioPath(mediaUrl, bucket.name);
    if (!name) throw new Error('El audio no pertenece al almacenamiento de WhatsApp.');
    const token = apiKey();
    if (!token) throw new Error('Falta OPENAI_API_KEY para transcribir el audio.');
    const buffer = await readAudio(bucket.file(name));
    const extension = name.split('.').at(-1).toLowerCase();
    const form = new FormData();
    form.append('file', new Blob([buffer], { type: AUDIO_TYPES[extension] }), `nota-de-voz.${extension}`);
    form.append('model', model());
    form.append('language', 'es');
    form.append('response_format', 'json');
    const startedAt = Date.now();
    const modelName = form.get('model');
    const response = await fetchImpl('https://api.openai.com/v1/audio/transcriptions', {
      method: 'POST', headers: { Authorization: `Bearer ${token}` }, body: form, signal: AbortSignal.timeout(90_000),
    });
    const result = await response.json();
    await recordOpenAiUsage({ resource: 'audio/transcriptions', purpose: 'sample_transcription', model: modelName, usage: result.usage, requestId: response.headers?.get?.('x-request-id') || '', status: response.status || 200, durationMs: Date.now() - startedAt });
    if (!response.ok) throw new Error(`No se pudo transcribir el audio (HTTP ${response.status}).`);
    const text = String(result.text || '').trim();
    if (!text) throw new Error('No se detectó voz comprensible en el audio.');
    if (text.length > 12_000) throw new Error('El audio es demasiado largo; envía notas más cortas.');
    return text;
  };
}
