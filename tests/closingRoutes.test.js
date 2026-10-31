import test from 'node:test';
import assert from 'node:assert/strict';
import { registerClosingRoutes } from '../services/closingRoutes.js';
import { createClosingConfigStore } from '../services/closingConfig.js';
import { closingDb } from './helpers/closingDb.js';
function setup() {
 const h=closingDb({'leads/a':{nombre:'Ana'}}), routes=new Map();
 const app=Object.fromEntries(['get','post'].map(method=>[method,(path,...handlers)=>routes.set(`${method} ${path}`,handlers)]));
 registerClosingRoutes(app,{db:h.db,bucket:{},store:createClosingConfigStore(h.db),agent:{pause:async()=>{},resume:async()=>{}},readUser:async req=>req.user});
 return {call:async(method,path,user,body={},params={})=>{
  const handlers=routes.get(`${method} ${path}`),req={user,body,params},res={code:200,status(code){this.code=code;return this;},json(body){this.body=body;return this;}};
  let pending;await handlers[0](req,res,()=>{pending=handlers[1](req,res);});await pending;return res;
 }};
}
test('configuración y controles requieren administrador autenticado',async()=>{
 const h=setup();assert.equal((await h.call('get','/api/crm/closing/config',null)).code,401);
 assert.equal((await h.call('get','/api/crm/closing/config',{role:'salesAgent'})).code,403);
 const admin={role:'admin',uid:'owner'};
 const initial=await h.call('get','/api/crm/closing/config',admin);assert.equal(initial.body.config.enabled,false);
 assert.equal((await h.call('post','/api/crm/closing/config',admin,{enabled:true})).code,400);
 const saved=await h.call('post','/api/crm/closing/config',admin,{enabled:false});assert.equal(saved.code,200);assert.equal(saved.body.config.ownerUid,'owner');
 assert.equal((await h.call('post','/api/crm/closing/contacts/:leadId',admin,{action:'pause'},{leadId:'a/b'})).code,400);
});
