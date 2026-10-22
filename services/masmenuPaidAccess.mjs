import {createHash,createHmac,timingSafeEqual} from 'node:crypto';
import {resolveMasmenuRecipient,sendMasmenuCredentials} from './masmenuWhatsApp.mjs';
const hash=v=>createHash('sha256').update(v).digest();
const uuid=/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i;
export function createMasmenuPaidAccessHandler({db,getSocket,env=process.env,now=Date.now,receiptTimeoutMs=4000,logger=console}){
 return async function paidAccess(req,res){
  res.set('Cache-Control','private, no-store');
  const key=String(env.MASMENU_CONFIRMATION_API_KEY||'').trim();
  if(env.MASMENU_PAID_ACCESS_ENABLED!=='true'||key.length<32)return res.status(503).json({ok:false,code:'ACCESS_NOT_CONFIGURED'});
  const token=String(req.get('authorization')||'').match(/^Bearer\s+(\S+)$/i)?.[1]||'';
  if(!token||!timingSafeEqual(hash(token),hash(key)))return res.status(401).json({ok:false,code:'UNAUTHORIZED'});
  const p=req.body;let login;
  try{
   login=new URL(p?.loginUrl);const app=new URL(env.MASMENU_PUBLIC_APP_URL);
   if(!p||!uuid.test(p.purchaseId)||!uuid.test(p.menuId)||(p.resendRequestId!==undefined&&!uuid.test(p.resendRequestId))||!/^\+52\d{10}$/.test(p.phone)||typeof p.restaurantName!=='string'||!p.restaurantName.trim()||p.restaurantName.length>150||!/^([a-z0-9]+-)*[a-z0-9]+$/.test(p.slug)||p.slug.length>96||!['test','live'].includes(p.environment)||
    (p.pin!==null&&(typeof p.pin!=='string'||!/^\d{4}$/.test(p.pin)))||app.protocol!=='https:'||login.origin!==app.origin||login.protocol!=='https:'||login.username||login.password||login.pathname!=='/acceso'||login.searchParams.get('next')!==`/administrar/${p.slug}`||[...login.searchParams.keys()].some(k=>k!=='next')||
    (Boolean(login.hash)&&!/^#activar=[A-Za-z0-9_-]{43}$/.test(login.hash)))throw new Error('INVALID');
  }catch{return res.status(400).json({ok:false,code:'INVALID_ACCESS_MESSAGE'});}
  const mac=v=>createHmac('sha256',key).update(v).digest('hex');
  const fingerprint=mac(JSON.stringify([p.menuId,p.phone,p.pin,p.loginUrl,p.environment,p.restaurantName]));
  const ref=db.collection('masmenuPaidAccessDeliveries').doc(p.resendRequestId?`${p.purchaseId}_${p.resendRequestId}`:p.purchaseId);
  const limitRef=db.collection('masmenuPaidAccessLimits').doc(mac(p.phone));
  let reserved=false,sendStarted=false;
  try{
   const outcome=await db.runTransaction(async tx=>{
    const previous=await tx.get(ref);
    if(previous.exists){const record=previous.data();if(record.fingerprint!==fingerprint)return {state:'conflict'};if(record.status!=='lookup_failed')return {state:record.status==='sent'?'sent':'uncertain',confirmed:record.deliveryConfirmed===true};}
    if(!getSocket())return 'disconnected';
    const limit=await tx.get(limitRef);const recent=((limit.exists?limit.data().attempts:[])||[]).filter(at=>now()-at<86400000);
    if(recent.length>=10||recent.some(at=>now()-at<60000))return 'limited';
    tx.set(ref,{fingerprint,status:'sending',attemptedAt:now(),expiresAt:new Date(now()+30*86400000)});
    tx.set(limitRef,{attempts:[...recent,now()],expiresAt:new Date(now()+86400000)});return 'reserved';
   });
   if(outcome?.state==='sent')return res.json({ok:true,status:'already_sent',deliveryConfirmed:outcome.confirmed});
   const state=typeof outcome==='string'?outcome:outcome.state;
   if(state!=='reserved')return res.status(state==='limited'?429:409).json({ok:false,code:'ACCESS_'+state.toUpperCase()});
   reserved=true;const sock=getSocket();if(!sock)throw new Error('DISCONNECTED');
   const jid=await resolveMasmenuRecipient(sock,p.phone);
   const name=p.restaurantName.replace(/[\r\n*`_]/g,' ').replace(/\s+/g,' ').trim();
   const text=[`🎉 ¡Hola ${name}!`,'',p.environment==='test'?'🧪 Tu acceso *+Menú Pro* está activo en modo de prueba. No se cobró dinero real; ya puedes usar tu administrador.':'✅ Tu acceso *+Menú Pro* ha sido activado exitosamente.','',
    '🔐 *Tus credenciales de acceso:*',`📱 Teléfono: ${p.phone.slice(1)}`,p.pin?`🔑 PIN: *${p.pin}*`:'🔑 PIN: *Usa tu PIN actual*','',
    '🌐 Accede a tu panel aquí:',p.loginUrl,'',
    '📝 *¿Qué puedes hacer en tu panel Pro?*','• Agregar y editar productos','• Actualizar precios y fotografías','• Organizar tus categorías','• Crear y editar combos','• Cambiar tu logo y los datos de tu restaurante','• Marcar productos disponibles o agotados','• Ver los cambios en tu menú público','',
    '💡 *Importante:*','- Guarda tu PIN y no compartas este mensaje','- Entra con tu teléfono y PIN, también desde otro dispositivo',...(!p.pin?['- Esta compra conserva el acceso y PIN de tu cuenta']:[]),'- Soporte: ayuda@negociosweb.mx','','¡Bienvenido a +Menú! 🚀'].join('\n');
   sendStarted=true;
   const sent=await sendMasmenuCredentials(sock,jid,text,{receiptTimeoutMs});
   await ref.set({status:'sent',sentAt:now(),messageId:sent.messageId,deliveryConfirmed:sent.deliveryConfirmed,recipientFormat:jid.startsWith('521')?'mx521':'mx52',transportVersion:2},{merge:true});
   logger.info('masmenu_paid_access',{purchaseId:p.purchaseId,outcome:sent.deliveryConfirmed?'delivered':'accepted',recipientFormat:jid.startsWith('521')?'mx521':'mx52'});
   return res.json({ok:true,status:'sent',deliveryConfirmed:sent.deliveryConfirmed});
  }catch(error){
   if(reserved)await ref.set({status:sendStarted?'unknown':'lookup_failed'},{merge:true}).catch(()=>{});
   const safeCode=['RECIPIENT_NOT_REGISTERED','RECIPIENT_LOOKUP_UNAVAILABLE','WHATSAPP_TIMEOUT','DELIVERY_REJECTED','NO_RECEIPT','DISCONNECTED'].includes(error?.message)?error.message:'DELIVERY_UNCERTAIN';
   logger.warn('masmenu_paid_access',{purchaseId:p.purchaseId,outcome:safeCode});
   if(error?.message==='RECIPIENT_NOT_REGISTERED')return res.status(422).json({ok:false,code:'ACCESS_RECIPIENT_NOT_REGISTERED'});
   return res.status(503).json({ok:false,code:'ACCESS_DELIVERY_UNCERTAIN'});
  }
 };
}
