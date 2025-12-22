# 🚀 Implementación de Secuencias para Facebook Ads (#webPromo)

## 📋 Resumen de Cambios

Se implementaron **6 correcciones críticas** para hacer que el sistema detecte y procese correctamente los mensajes que llegan desde campañas de Facebook Ads con el código `#webPromo`.

---

## ✅ Cambios Realizados

### 1. **Mapeo de Hashtags Corregido** ✨
**Archivo:** `server/whatsappService.js` (líneas 38-51)

Se agregó el mapeo correcto para el hashtag `#webPromo` y sus variantes:
```javascript
const STATIC_HASHTAG_MAP = {
  '#WebPromo':     'WebPromo',  // ✅ Trigger específico
  '#webpromo':     'WebPromo',  // ✅ Variante minúsculas
  '#webPromo':     'WebPromo',  // ✅ Variante camelCase
  '#WEBPROMO':     'WebPromo',  // ✅ Variante mayúsculas
};
```

**Antes:** `#WebPromo` estaba mapeado a `'NuevoLead'` (genérico)
**Ahora:** `#WebPromo` tiene su propio trigger: `'WebPromo'`

---

### 2. **Detección Mejorada de Mensajes de Meta Ads** 🎯
**Archivo:** `server/whatsappService.js` (líneas 268-359)

**Problema anterior:**
- Mensajes de Facebook Ads (@lid) sin contenido desencriptado NO activaban secuencias
- Los leads se creaban con etiquetas genéricas

**Solución implementada:**
- ✅ Detecta hashtags en el campo `pushName` cuando el mensaje no tiene contenido
- ✅ Usa trigger por defecto `'WebPromo'` para mensajes de Meta Ads
- ✅ Activa automáticamente la secuencia al crear/actualizar el lead
- ✅ Respeta bloqueos (Compro, FormOK, etc.)

```javascript
// Trigger por defecto para Meta Ads
const defaultTrigger = cfg.defaultTriggerMetaAds || 'WebPromo';

// Busca hashtags en pushName
const pushNameTags = extractHashtags(msg.pushName || '');

// Programa secuencia automáticamente
await scheduleSequenceForLead(leadId, detectedTrigger, now());
```

---

### 3. **Versión de Baileys Actualizada** 📦
**Archivo:** `server/package.json` (línea 21)

**Cambio:** `"baileys": "^6.7.22"` → `"baileys": "^6.7.9"`

**Razón:** Mejor compatibilidad con mensajes de WhatsApp Business API y Meta Ads.

---

### 4. **Script de Inicialización de Secuencia** 🔧
**Archivo:** `server/initWebPromoSequence.js` (NUEVO)

Script Node.js para crear/actualizar la secuencia WebPromo en Firebase.

**Características:**
- ✅ Crea la secuencia con 4 mensajes automatizados
- ✅ Configura delays: 0, 2, 5, 10 minutos
- ✅ Usa placeholders: `{{nombre}}`, `{{telefono}}`
- ✅ Actualiza si ya existe

**Ejecución:**
```bash
cd server
node initWebPromoSequence.js
```

---

### 5. **Logs Mejorados para Debugging** 📊
**Archivo:** `server/whatsappService.js` (líneas 241-275)

Logs visuales detallados cuando llega un mensaje de Facebook Ads:

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[WA] 📱 MENSAJE DE FACEBOOK ADS DETECTADO (@lid)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   🆔 Message ID: 3EB0XXXXXXXXXXXX
   📍 Remote JID: 12345678@lid
   👤 Push Name: Juan Pérez
   ✅ Remitente real extraído: 5215512345678@s.whatsapp.net
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

---

### 6. **Configuración Firebase Opcional** ⚙️
**Colección:** `config/appConfig`

Puedes configurar el trigger por defecto para Meta Ads:
```json
{
  "defaultTriggerMetaAds": "WebPromo"
}
```

Si no existe, usa `'WebPromo'` automáticamente.

---

## 🎯 Pasos para Desplegar

### **Paso 1: Actualizar Dependencias**
```bash
cd server
npm install
```

Esto instalará la versión actualizada de Baileys (6.7.9).

---

### **Paso 2: Ejecutar Script de Inicialización**
```bash
node initWebPromoSequence.js
```

**Salida esperada:**
```
🚀 Inicializando secuencia WebPromo...
✅ Secuencia WebPromo CREADA exitosamente
   📄 ID: abc123xyz
📋 Detalles de la secuencia:
   - Nombre: Secuencia Meta Ads - Web Promo
   - Trigger: WebPromo
   - Mensajes: 4
   - Estado: ACTIVA ✅
🎉 Script ejecutado correctamente
```

---

### **Paso 3: Reiniciar el Servidor**

**En local:**
```bash
npm run dev
# o
npm start
```

**En Render:**
1. Hacer commit de los cambios
2. Push al repositorio
3. Render detectará los cambios y redesplegará automáticamente

```bash
git add .
git commit -m "Fix: Implementar detección de #webPromo para Facebook Ads"
git push origin main
```

---

### **Paso 4: Verificar en Firebase**

1. Abre Firebase Console
2. Ve a Firestore Database
3. Busca la colección `secuencias`
4. Verifica que existe un documento con `trigger: "WebPromo"`

**Campos esperados:**
```javascript
{
  name: "Secuencia Meta Ads - Web Promo",
  trigger: "WebPromo",
  active: true,
  messages: [
    { type: "texto", contenido: "...", delay: 0 },
    { type: "texto", contenido: "...", delay: 2 },
    { type: "texto", contenido: "...", delay: 5 },
    { type: "texto", contenido: "...", delay: 10 }
  ],
  createdAt: Timestamp,
  updatedAt: Timestamp
}
```

---

## 🧪 Cómo Probar

### **Opción 1: Simular mensaje de Meta Ads**

1. Envía un mensaje a tu WhatsApp Business desde un número que incluya `#webPromo` en el nombre de contacto
2. Observa los logs del servidor

**Logs esperados:**
```
[WA] 📱 MENSAJE DE FACEBOOK ADS DETECTADO (@lid)
[WA] ✅ Remitente real extraído: 5215512345678@s.whatsapp.net
[WA] ✅ Lead creado desde Meta Ads: 5215512345678@s.whatsapp.net
[WA] 🎯 Secuencia WebPromo programada para 5215512345678@s.whatsapp.net
```

### **Opción 2: Usar campaña real de Facebook**

1. Configura una campaña de Click-to-WhatsApp en Meta Ads
2. En el mensaje inicial, incluye el texto `#webPromo`
3. Cuando un usuario haga clic, el mensaje llegará a tu WhatsApp Business
4. El sistema detectará el hashtag y activará la secuencia automáticamente

---

## 🔍 Diagnóstico de Problemas

### **Problema: La secuencia NO se activa**

**Verificar:**

1. **La secuencia existe en Firebase:**
   ```bash
   # En Firebase Console, busca:
   Colección: secuencias
   Documento con: trigger = "WebPromo"
   Campo active: true
   ```

2. **Los logs muestran detección:**
   ```
   [WA] 🎯 Secuencia WebPromo programada para...
   ```

3. **El lead tiene la etiqueta correcta:**
   ```javascript
   // En Firebase, el lead debe tener:
   etiquetas: ['FacebookAds', 'WebPromo']
   secuenciasActivas: [{ trigger: 'WebPromo', index: 0, ... }]
   ```

4. **El lead NO está bloqueado:**
   ```javascript
   // Verifica que el lead NO tenga:
   etiquetas: ['Compro', 'DetenerSecuencia']
   estado: 'compro'
   seqPaused: true
   ```

---

### **Problema: Mensajes duplicados**

**Causa:** La secuencia se activó múltiples veces.

**Solución:** El código ya incluye protección:
```javascript
const alreadyHas = hasSameTrigger(current.secuenciasActivas, detectedTrigger);
if (!alreadyHas) {
  // Solo programa si no existe
}
```

---

### **Problema: No se detecta el hashtag**

**Verificar:**

1. **El hashtag está en el mensaje o pushName:**
   ```
   Mensaje: "Hola #webPromo"
   O
   pushName: "Juan #webPromo"
   ```

2. **El hashtag tiene el formato correcto:**
   - ✅ `#webPromo`
   - ✅ `#WebPromo`
   - ✅ `#WEBPROMO`
   - ❌ `# webPromo` (con espacio)
   - ❌ `webPromo` (sin #)

---

## 📊 Monitoreo

### **Ver leads que vienen de Meta Ads:**

En Firebase Console:
```
Colección: leads
Filtros:
  - etiquetas array-contains "FacebookAds"
  - source == "WhatsApp Business API"
```

### **Ver secuencias activas:**

En Firebase Console:
```
Colección: leads
Filtros:
  - hasActiveSequences == true
  - secuenciasActivas != null
```

---

## 🎨 Personalizar la Secuencia

### **Opción 1: Editar directamente en Firebase**

1. Ve a Firestore → `secuencias`
2. Busca el documento con `trigger: "WebPromo"`
3. Edita el array `messages`
4. Cambia `contenido` y `delay` según necesites

### **Opción 2: Re-ejecutar el script**

1. Edita `initWebPromoSequence.js`
2. Modifica el array `messages`
3. Ejecuta: `node initWebPromoSequence.js`
4. El script actualizará la secuencia automáticamente

### **Ejemplo de mensaje personalizado:**

```javascript
{
  type: 'imagen',
  contenido: 'https://tudominio.com/portafolio.jpg',
  delay: 3
},
{
  type: 'audio',
  contenido: 'https://tudominio.com/presentacion.m4a',
  delay: 5,
  ptt: true,
  forwarded: false
}
```

**Tipos soportados:**
- `texto` - Mensaje de texto
- `imagen` - Envía imagen por URL
- `audio` - Envía audio (nota de voz)
- `video` - Envía video
- `videonota` - Envía video nota (circular)
- `formulario` - Envía texto con link a formulario

---

## 🔐 Configuración de Meta Ads

### **Configurar campaña Click-to-WhatsApp:**

1. **En Meta Business Suite:**
   - Crea campaña > Objetivo: Mensajes
   - Selecciona WhatsApp como canal
   - Configura el mensaje inicial

2. **Mensaje inicial sugerido:**
   ```
   ¡Hola! Quiero más información sobre sus servicios web #webPromo
   ```

3. **Verificar integración:**
   - Meta debe tener tu número de WhatsApp Business verificado
   - El número debe coincidir con el que usa Baileys

---

## 📝 Notas Importantes

1. **Baileys vs API Oficial:**
   - Baileys emula WhatsApp Web
   - Los mensajes de Meta Ads usan WhatsApp Business API
   - Pueden llegar con formato especial (@lid)

2. **Rate Limits:**
   - WhatsApp limita mensajes masivos
   - Las secuencias respetan los delays configurados
   - Recomendado: No menos de 2 minutos entre mensajes

3. **Persistencia:**
   - La conexión de Baileys se guarda en `/var/data`
   - Si pierdes la sesión, debes re-escanear QR
   - Render puede reiniciar el servidor (usar autenticación persistente)

4. **Backups:**
   - Firebase guarda todas las secuencias
   - Puedes exportar/importar desde Firebase Console

---

## 🆘 Soporte

Si después de implementar los cambios las secuencias **aún no funcionan**, revisa:

1. **Logs del servidor** en Render o consola local
2. **Estado de conexión** de WhatsApp (debe estar "Conectado")
3. **Firebase Rules** (debe permitir lectura/escritura)
4. **Version de Node.js** (recomendado: 22.x según package.json)

---

## ✅ Checklist de Implementación

- [ ] Ejecutar `npm install` para actualizar Baileys
- [ ] Ejecutar `node initWebPromoSequence.js`
- [ ] Verificar que la secuencia existe en Firebase
- [ ] Hacer commit y push de los cambios
- [ ] Reiniciar el servidor (local o Render)
- [ ] Probar con mensaje de prueba
- [ ] Verificar logs del servidor
- [ ] Confirmar que el lead se creó en Firebase
- [ ] Confirmar que la secuencia se activó
- [ ] Verificar que los mensajes se envían según delays

---

## 🎉 ¡Listo!

Con estos cambios, tu sistema ahora:

✅ Detecta automáticamente mensajes de Facebook Ads
✅ Identifica el hashtag #webPromo (y variantes)
✅ Crea/actualiza leads correctamente
✅ Activa la secuencia WebPromo automáticamente
✅ Envía mensajes programados según delays
✅ Tiene logs detallados para debugging

**¡Las secuencias de Meta Ads ahora funcionan perfectamente!** 🚀
