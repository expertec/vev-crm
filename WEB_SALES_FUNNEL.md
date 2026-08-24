# Web Sales Funnel

## Auditoria de arquitectura reutilizada

- Leads y mensajes: `server/whatsappService.js` crea/actualiza leads, guarda mensajes y detecta Meta Ads/CTWA.
- Sales Brain: `server/services/salesBrain/*` mantiene analisis, score, `conversationObjective`, `nextBestAction`, `salesContext`, `commercialProgress` y eventos `salesBrainEvents`.
- Routing y cola: `server/services/salesQueue/*` calcula señales, prioridad, `routing`, `queue` y Modo Ventas.
- Secuencias: `server/queue.js` programa `secuenciasActivas`, procesa pasos, pausa/cancela y evita duplicados por trigger.
- Scheduler de muestras: `server/scheduler.js` genera schemas para `Negocios` en `Sin procesar` y envia la muestra por WhatsApp.
- Formulario de muestra: `frontend-next-v2/src/app/muestra/[phone]/page.jsx` y endpoints `/api/web/sample-access/:phone`, `/api/web/sample-submit`.
- Envio de formulario/muestra desde CRM: `/api/crm/lead-business/send-sample-link`.
- Tracking existente de apertura: `/api/track/link-open`.
- Etapas/estados/tags: se preservan campos legacy como `etapa`, `estado`, `etiquetas`, `sampleFlow`, `sampleSubmittedAt`, `webLinkSentAt`, `linkOpenedAt`.
- Pagos/ventas: ventas se registran en `salesQueue.registerAgentOutcome` y rutas de suscripcion/pagos existentes; el funnel web no reemplaza esas rutas.

## Nueva estructura

La nueva capa vive en `server/services/webSales`.

`webSales` es un estado interno por lead para el producto Web. No reemplaza Kanban, Sales Brain ni secuencias.

```js
webSales: {
  stage: "sample_form_sent",
  updatedAt: timestamp,
  nextConversionGoal: "get_sample_form_completed",
  productInterest: "web",
  experiment: { id: "web_funnel_v2", variant: "A" },
  sample: {
    offeredAt,
    formSentAt,
    formOpenedAt,
    formStartedAt,
    formLastStep,
    formCompletedAt,
    generatedAt,
    sentAt,
    firstOpenedAt,
    lastOpenedAt,
    openCount,
    feedbackReceivedAt
  },
  closing: {
    startedAt,
    paymentOfferedAt,
    paymentLinkSentAt,
    paymentLinkOpenedAt,
    depositPaidAt,
    paidAt
  }
}
```

Tambien se deriva `nextConversionGoal` en el lead para lectura rapida.

## Eventos

Eventos auditables por lead:

```text
leads/{leadId}/webSalesEvents/{eventId}
```

Campos:

```js
{
  type,
  timestamp,
  source,
  metadata,
  ignored,
  ignoredReason,
  stageBefore,
  stageAfter,
  nextConversionGoal,
  createdAt
}
```

Eventos integrados:

- `sample_form_sent`: `/api/crm/lead-business/send-sample-link`
- `sample_form_opened`: `/api/web/sample-access/:phone`
- `sample_form_started`: `/api/web/funnel-event` desde el wizard
- `sample_form_completed`: `/api/web/sample-submit` y `/api/web/after-form`
- `sample_generated`: `generateSiteSchemas`
- `sample_sent`: `enviarSitioWebPorWhatsApp` y `/api/web/sample-sent`
- `sample_opened`: `/api/track/link-open`

## Idempotencia

- Eventos unicos usan ID deterministico por tipo/idempotencyKey.
- `sample_opened` es repetible e incrementa `openCount`.
- `firstOpenedAt` no se sobrescribe.
- `lastOpenedAt` si se actualiza.
- Eventos de apertura con user-agent de bots/previews/headless se guardan como `ignored` y no incrementan contador.
- Cambios de estado y contador se hacen en transaccion Firestore.

## Integracion con Sales Brain y cola

- Nuevas intenciones/señales web se agregaron al catalogo de Sales Brain.
- Score suma bonuses unicos/capados por hitos del funnel.
- Routing reconoce `sample_engagement`.
- Modo Ventas muestra etapa Web, objetivo actual y razon resumida dentro del panel Sales Brain.

## Indices Firestore

No se agregaron consultas nuevas que requieran indices compuestos. La implementacion usa document reads por lead y subcoleccion de eventos por documento.
