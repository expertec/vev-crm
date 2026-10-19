import {randomBytes} from 'node:crypto';

async function bounded(promise, milliseconds) {
 let timer;
 try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new Error('WHATSAPP_TIMEOUT')),milliseconds);})]);}
 finally{clearTimeout(timer);}
}

// E.164 stays +52 in +Menu. Ask the existing WhatsApp session which MX PN is
// registered, including the historical 521 form; never guess a destination.
export async function resolveMasmenuRecipient(socket,phone) {
 if(typeof socket?.onWhatsApp!=='function')throw new Error('RECIPIENT_LOOKUP_UNAVAILABLE');
 const national=phone.slice(3),candidates=[`521${national}`,`52${national}`];
 const results=await bounded(socket.onWhatsApp(...candidates),5000);
 for(const candidate of candidates){
  const found=results?.find(item=>item.exists===true&&item.jid===`${candidate}@s.whatsapp.net`);
  if(found)return found.jid;
 }
 throw new Error('RECIPIENT_NOT_REGISTERED');
}

export async function sendMasmenuCredentials(socket,jid,text,{receiptTimeoutMs=4000}={}) {
 const id=`MASMENUAUTH_${randomBytes(16).toString('hex').toUpperCase()}`;
 let finish,deliveryError=false;
 const receipt=new Promise(resolve=>{finish=resolve;});
 const listener=updates=>{
  for(const item of updates??[]){
   if(item.key?.id!==id||item.key?.fromMe===false)continue;
   const status=Number(item.update?.status);
   if(status>=3&&status<=5)finish(true); // Baileys DELIVERY_ACK / READ / PLAYED.
   else if(status===0){deliveryError=true;finish(false);}
  }
 };
 const events=socket.ev;
 events?.on('messages.update',listener);
 try{
  const sent=await bounded(socket.sendMessage(jid,{text,linkPreview:false},{timeoutMs:8000,messageId:id}),8000);
  if(!sent?.key?.id)throw new Error('NO_RECEIPT');
  let timer;
  let confirmed=false;
  try{confirmed=await Promise.race([receipt,new Promise(resolve=>{timer=setTimeout(()=>resolve(false),receiptTimeoutMs);})]);}
  finally{clearTimeout(timer);}
  if(deliveryError)throw new Error('DELIVERY_REJECTED');
  return {messageId:sent.key.id,deliveryConfirmed:confirmed===true};
 }finally{
  if(events?.off)events.off('messages.update',listener);
  else events?.removeListener?.('messages.update',listener);
 }
}
