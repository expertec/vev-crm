// mercadopagoConfig.js - Configuración de Mercado Pago Checkout Pro
import { MercadoPagoConfig, Preference, Payment } from 'mercadopago';
import dotenv from 'dotenv';

dotenv.config();

// Validar que el access token esté configurado
if (!process.env.MP_ACCESS_TOKEN) {
  console.warn('⚠️ MP_ACCESS_TOKEN no está configurada - Mercado Pago no funcionará');
}

// Inicializar cliente de Mercado Pago
const client = new MercadoPagoConfig({
  accessToken: process.env.MP_ACCESS_TOKEN || '',
});

// Exportar clientes de Preference y Payment
export const preferenceClient = new Preference(client);
export const paymentClient = new Payment(client);

// Planes disponibles
export const PLANES = {
  basico: {
    id: 'basico',
    nombre: 'Plan Básico',
    precio: 397,
    currency: 'MXN',
    descripcion: 'Página web profesional con todas las funciones básicas',
    features: [
      'Página web profesional',
      'Dominio personalizado',
      'Certificado SSL',
      'Soporte por WhatsApp',
      'Actualizaciones mensuales'
    ]
  },
  pro: {
    id: 'pro',
    nombre: 'Plan Pro',
    precio: 997,
    currency: 'MXN',
    descripcion: 'Página web premium con funciones avanzadas y prioridad',
    features: [
      'Todo lo del Plan Básico',
      'Diseño premium personalizado',
      'SEO avanzado',
      'Integraciones especiales',
      'Soporte prioritario 24/7',
      'Analíticas avanzadas'
    ]
  }
};

// URLs de configuración
export const MP_CONFIG = {
  frontendUrl: process.env.FRONTEND_URL || 'https://negociosweb.mx',
  webhookUrl: process.env.MP_WEBHOOK_URL || null,
};

console.log('✅ Mercado Pago configurado correctamente');
