// pinUtils.js - Utilidades para el sistema de PIN de clientes

/**
 * Genera un PIN aleatorio de 4 dígitos
 * @returns {string} PIN de 4 dígitos (ej: "1234")
 */
export function generarPIN() {
  return Math.floor(1000 + Math.random() * 9000).toString();
}

/**
 * Valida que un PIN tenga el formato correcto
 * @param {string} pin - PIN a validar
 * @returns {boolean} true si el PIN es válido
 */
export function validarPIN(pin) {
  const pinStr = String(pin || '').trim();
  return /^\d{4}$/.test(pinStr);
}

/**
 * Genera el mensaje de WhatsApp con las credenciales de acceso
 * @param {Object} params - Parámetros del mensaje
 * @param {string} params.companyName - Nombre del negocio
 * @param {string} params.pin - PIN generado
 * @param {string} params.phone - Teléfono del cliente
 * @param {string} params.plan - Plan contratado
 * @param {string} params.loginUrl - URL del panel de cliente
 * @returns {string} Mensaje formateado para WhatsApp
 */
export function generarMensajeCredenciales({ companyName, pin, phone, plan, loginUrl }) {
  const planNames = {
    basic: 'Básico',
    pro: 'Pro',
    premium: 'Premium'
  };

  const planName = planNames[plan] || plan;
  const phoneFormatted = phone.replace(/\D/g, '');

  return `🎉 ¡Hola ${companyName}!

✅ Tu plan *${planName}* ha sido activado exitosamente.

🔐 *Tus credenciales de acceso:*
📱 Teléfono: ${phoneFormatted}
🔑 PIN: *${pin}*

🌐 Accede a tu panel aquí:
${loginUrl}

📝 *¿Qué puedes hacer en tu panel?*
• Cambiar colores y diseño de tu sitio
• Editar textos e imágenes
• Administrar productos (si tienes tienda)
• Ver y gestionar reservas (si tienes sistema de citas)
• Personalizar tu menú y secciones

💡 *Importante:*
- Guarda tu PIN en un lugar seguro
- No lo compartas con nadie
- Si tienes dudas, contáctanos

¡Bienvenido a Negocios Web! 🚀`;
}

/**
 * Normaliza un número de teléfono para formato E164
 * @param {string} phone - Teléfono a normalizar
 * @returns {string} Teléfono en formato sin símbolos
 */
export function normalizarTelefono(phone) {
  return String(phone || '').replace(/\D/g, '');
}