import {createHash,createHmac,randomBytes,timingSafeEqual} from 'node:crypto';
const hash=v=>createHash('sha256').update(v).digest();
const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i;
export function createMasmenuPaidAccessHandler({db,getSocket,env=process.env,now=Date.now}){
 return async function paidAccess(req,res){
  res.set('Cache-Control','private, no-store');
  const key=String(env.MASMENU_CONFIRMATION_API_KEY||'').trim();
  if(env.MASMENU_PAID_ACCESS_ENABLED!=='true'||key.length<32)return res.status(503).json({ok:false,code:'ACCESS_NOT_CONFIGURED'});
  const token=String(req.get('authorization')||'').match(/^Bearer\s+(\S+)$/i)?.[1]||'';
  if(!token||!timingSafeEqual(hash(token),hash(key)))return res.status(401).json({ok:false,code:'UNAUTHORIZED'});
  const p=req.body;let login;
  try{
   login=new URL(p?.loginUrl);const app=new URL(env.MASMENU_PUBLIC_APP_URL);
   if(!p||!uuid.test(p.purchaseId)||!uuid.test(p.menuId)||!/^\+52\d{10}$/.test(p.phone)||typeof p.restaurantName!=='string'||!p.restaurantName.trim()||p.restaurantName.length>150||!/^([a-z0-9]+-)*[a-z0-9]+$/.test(p.slug)||p.slug.length>96||!['test','live'].includes(p.environment)||
    (p.pin!==null&&(typeof p.pin!=='string'||!/^\d{4}$/.test(p.pin)))||app.protocol!=='https:'||login.origin!==app.origin||login.protocol!=='https:'||login.username||login.password||login.pathname!=='/acceso'||login.searchParams.get('next')!==`/administrar/${p.slug}`||[...login.searchParams.keys()].some(k=>k!=='next')||
    (p.pin!==null?!/^#activar=[A-Za-z0-9_-]{43}$/.test(login.hash):Boolean(login.hash)))throw new Error('INVALID');
  }catch{return res.status(400).json({ok:false,code:'INVALID_ACCESS_MESSAGE'});}
  const mac=v=>createHmac('sha256',key).update(v).digest('hex');
  const fingerprint=mac(JSON.stringify([p.menuId,p.phone,p.pin,p.loginUrl,p.environment,p.restaurantName]));
  const ref=db.collection('masmenuPaidAccessDeliveries').doc(p.purchaseId);
  const limitRef=db.collection('masmenuPaidAccessLimits').doc(mac(p.phone));
  let reserved=false;
  try{
   const outcome=await db.runTransaction(async tx=>{
    const previous=await tx.get(ref);
    if(previous.exists){const record=previous.data();if(record.fingerprint!==fingerprint)return 'conflict';return record.status==='sent'?'sent':'uncertain';}
    if(!getSocket())return 'disconnected';
    const limit=await tx.get(limitRef);const recent=((limit.exists?limit.data().attempts:[])||[]).filter(at=>now()-at<86400000);
    if(recent.length>=10||recent.some(at=>now()-at<60000))return 'limited';
    tx.set(ref,{fingerprint,status:'sending',attemptedAt:now(),expiresAt:new Date(now()+30*86400000)});
    tx.set(limitRef,{attempts:[...recent,now()],expiresAt:new Date(now()+86400000)});return 'reserved';
   });
   if(outcome==='sent')return res.json({ok:true,status:'already_sent'});
   if(outcome!=='reserved')return res.status(outcome==='limited'?429:409).json({ok:false,code:'ACCESS_'+outcome.toUpperCase()});
   reserved=true;const sock=getSocket();if(!sock)throw new Error('DISCONNECTED');
   const name=p.restaurantName.replace(/[\r\n*`_]/g,' ').replace(/\s+/g,' ').trim();
   const text=[`🎉 ¡Hola ${name}!`,'',p.environment==='test'?'🧪 Tu activación de *+Menú Pro de prueba* fue confirmada. No se cobró dinero real ni se habilitó administración de producción.':'✅ Tu acceso *+Menú Pro* ha sido activado exitosamente.','',
    '🔐 *Tus credenciales de acceso:*',`📱 Teléfono: ${p.phone.slice(1)}`,p.pin?`🔑 PIN: *${p.pin}*`:'🔑 PIN: *Usa tu PIN actual*','',
    '🌐 Accede a tu panel aquí:',p.loginUrl,'',
    '📝 *¿Qué puedes hacer en tu panel Pro?*','• Agregar y editar productos','• Actualizar precios y fotografías','• Organizar tus categorías','• Marcar productos disponibles o agotados','• Ver los cambios en tu menú público','',
    '💡 *Importante:*','- Guarda tu PIN y no compartas este mensaje',...(p.pin?['- Abre el enlace personal para activar tu primer acceso; vence en 7 días']:['- Esta compra conserva el acceso y PIN de tu cuenta']),'- Soporte: ayuda@negociosweb.mx','','¡Bienvenido a +Menú! 🚀'].join('\n');
   const sent=await sock.sendMessage(`${p.phone.slice(1)}@s.whatsapp.net`,{text,linkPreview:false},{timeoutMs:8000,messageId:`MASMENUAUTH_${randomBytes(16).toString('hex').toUpperCase()}`});
   if(!sent?.key?.id)throw new Error('NO_RECEIPT');
   await ref.set({status:'sent',sentAt:now()},{merge:true});return res.json({ok:true,status:'sent'});
  }catch{
   if(reserved)await ref.set({status:'unknown'},{merge:true}).catch(()=>{});
   return res.status(503).json({ok:false,code:'ACCESS_DELIVERY_UNCERTAIN'});
  }
 };
}
