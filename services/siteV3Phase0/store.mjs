import { fail } from './contracts.mjs';

// Serializable transactional double. Snapshotting/reinstantiating represents a
// process restart in tests; application services have no process-local job map.
export class MemoryStore {
  constructor(snapshot = {}) { this.docs = new Map(Object.entries(structuredClone(snapshot))); this.tail = Promise.resolve(); }
  async get(path) { return structuredClone(this.docs.get(path) ?? null); }
  snapshot() { return structuredClone(Object.fromEntries(this.docs)); }
  async runTransaction(callback) {
    const previous = this.tail;
    let release; this.tail = new Promise(resolve => { release = resolve; });
    await previous;
    const docs = new Map(structuredClone([...this.docs])); let writing = false;
    const tx = {
      get: async path => { fail(!writing, 'TRANSACTION_READ_AFTER_WRITE', 500); return structuredClone(docs.get(path) ?? null); },
      set: (path, value) => { writing = true; docs.set(path, structuredClone(value)); },
      create: (path, value) => { fail(!docs.has(path), 'DOCUMENT_ALREADY_EXISTS', 409); writing = true; docs.set(path, structuredClone(value)); },
    };
    try { const result = await callback(tx); this.docs = docs; return result; } finally { release(); }
  }
  async expiredJobs(now, limit = 100) {
    return [...this.docs.entries()].filter(([path, j]) => path.startsWith('SiteV3Jobs/') && ['queued', 'planning', 'generating_copy', 'resolving_assets', 'validating'].includes(j.state) && j.leaseExpiresAt <= now).sort((a, b) => a[1].leaseExpiresAt - b[1].leaseExpiresAt).slice(0, limit).map(([, j]) => structuredClone(j));
  }
}
export class FirestoreStore {
  constructor(db) { fail(typeof db?.runTransaction === 'function' && typeof db?.doc === 'function', 'STORE_NOT_CONFIGURED', 503); this.db = db; }
  async get(path) { const snap = await this.db.doc(path).get(); return snap.exists ? snap.data() : null; }
  runTransaction(callback) {
    return this.db.runTransaction(native => callback({
      get: async path => { const snap = await native.get(this.db.doc(path)); return snap.exists ? snap.data() : null; },
      set: (path, value) => native.set(this.db.doc(path), value),
      create: (path, value) => native.create(this.db.doc(path), value),
    }));
  }
  async expiredJobs(now, limit = 100) {
    const result = await this.db.collection('SiteV3Jobs').where('state', 'in', ['queued', 'planning', 'generating_copy', 'resolving_assets', 'validating']).where('leaseExpiresAt', '<=', now).orderBy('leaseExpiresAt').limit(limit).get();
    return result.docs.map(doc => doc.data());
  }
}
