export function closingDb(seed = {}) {
  let rows = new Map(Object.entries(structuredClone(seed))), chain = Promise.resolve();
  const value = (obj, path) => path.split('.').reduce((v, k) => v?.[k], obj);
  const patch = (map, path, data) => {
    const row = structuredClone(map.get(path) || {});
    for (const [key, v] of Object.entries(structuredClone(data))) { const parts = key.split('.'); let at = row; for (const p of parts.slice(0, -1)) at = at[p] ||= {}; at[parts.at(-1)] = v; }
    map.set(path, row);
  };
  const snap = (map, path) => ({ id: path.split('/').at(-1), ref: doc(path), exists: map.has(path), data: () => structuredClone(map.get(path)) });
  const doc = path => ({ id: path.split('/').at(-1), path, get: async () => snap(rows, path), update: async data => patch(rows, path, data), set: async (data, opts) => opts?.merge ? patch(rows, path, data) : rows.set(path, structuredClone(data)), delete: async () => rows.delete(path) });
  const collection = (name, constraints = [], max = Infinity, order) => {
    const q = { doc: id => doc(`${name}/${id}`), where: (field, op, target) => collection(name, [...constraints, [field, op, target]], max, order), orderBy: field => collection(name, constraints, max, field), limit: limit => collection(name, constraints, limit, order), get: async () => {
      let result = [...rows].filter(([path]) => path.startsWith(`${name}/`) && path.split('/').length === 2).filter(([,row]) => constraints.every(([f,op,target]) => op === '<=' ? value(row,f) <= target : value(row,f) === target));
      if (order) result.sort((a,b) => value(a[1],order)-value(b[1],order));
      return { docs: result.slice(0,max).map(([path])=>snap(rows,path)) };
    } }; return q;
  };
  const db = { collection, runTransaction: callback => {
    const next = chain.then(async () => { const draft = new Map(structuredClone([...rows]));
      const result = await callback({ get: async ref => snap(draft, ref.path), update: (ref,data) => patch(draft,ref.path,data), set: (ref,data,opts) => opts?.merge ? patch(draft,ref.path,data) : draft.set(ref.path,structuredClone(data)), delete: ref=>draft.delete(ref.path) }); rows=draft;return result;
    }); chain=next.catch(()=>{});return next;
  } };
  return { db, read: path=>rows.get(path), rows:()=>rows };
}
