// stripeConfig.js - Configuración de Stripe
import Stripe from 'stripe';
import dotenv from 'dotenv';

dotenv.config();

// Validar que las variables de entorno estén configuradas
if (!process.env.STRIPE_SECRET_KEY) {
  console.error('❌ STRIPE_SECRET_KEY no está configurada en .env');
  process.exit(1);
}

if (!process.env.STRIPE_PRICE_ID) {
  console.error('❌ STRIPE_PRICE_ID no está configurada en .env');
  process.exit(1);
}

if (!process.env.STRIPE_WEBHOOK_SECRET) {
  console.warn('⚠️ STRIPE_WEBHOOK_SECRET no está configurada - los webhooks no funcionarán');
}

// Inicializar Stripe
export const stripe = new Stripe(process.env.STRIPE_SECRET_KEY);

// IDs de productos/precios en Stripe
export const STRIPE_CONFIG = {
  priceId: process.env.STRIPE_PRICE_ID, // ID del precio mensual de $99 MXN
  webhookSecret: process.env.STRIPE_WEBHOOK_SECRET,
  currency: 'mxn'
};

// Estados de suscripción mapeados
export const SUBSCRIPTION_STATUS = {
  ACTIVE: 'active',
  PAST_DUE: 'past_due',
  CANCELED: 'canceled',
  INCOMPLETE: 'incomplete',
  TRIALING: 'trialing',
  UNPAID: 'unpaid'
};

console.log('✅ Stripe configurado correctamente');