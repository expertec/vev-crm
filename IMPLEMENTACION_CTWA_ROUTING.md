# Enrutamiento de Click-to-WhatsApp Ads a Secuencias

Esta implementacion permite que un mensaje entrante de Click-to-WhatsApp active
una secuencia distinta segun el anuncio, conjunto o campana de Meta.

## Que guarda el CRM

Cuando Baileys entrega metadata de anuncio, el lead recibe estos campos:

- `source: "meta_ads"`
- `campaign`: ID/nombre de campana detectado o `whatsapp_click_to_chat`
- `metaAttribution`: objeto con datos CTWA detectados
- `metaAdId`
- `metaSourceId`
- `metaAdSetId`
- `metaCampaignId`
- `metaCampaignName`
- `metaCtwaClid`
- `lastMetaSequenceTrigger`
- `lastMetaRouteId`
- `lastMetaRouteSource`

El mensaje entrante tambien guarda `metaAttribution` dentro de
`leads/{leadId}/messages/{messageId}` cuando aplica.

## Donde configurar reglas

Puedes usar cualquiera de estas dos opciones.

### Opcion A: coleccion recomendada

Crear documentos en la coleccion top-level:

`metaAdSequenceRoutes`

Ejemplo de documento:

```json
{
  "active": true,
  "name": "Anuncio tienda online",
  "adId": "120200000000001",
  "trigger": "LeadTiendaOnline",
  "priority": 10
}
```

Campos soportados:

- `trigger` o `sequenceTrigger`: trigger exacto de la secuencia.
- `adId`, `adIds`, `metaAdId`, `sourceId`, `sourceIds`: match por anuncio/source_id.
- `adSetId`, `adSetIds`, `adsetId`, `metaAdSetId`: match por conjunto.
- `campaignId`, `campaignIds`, `metaCampaignId`: match por campana.
- `campaignName`, `campaignNames`: match exacto por nombre de campana normalizado.
- `ctwaClid`, `ctwaClids`: match por click id.
- `sourceUrlIncludes`: match si la URL del anuncio contiene el texto.
- `headlineIncludes`: match si el titular del anuncio contiene el texto.
- `bodyIncludes` o `textIncludes`: match si el cuerpo del anuncio contiene el texto.
- `priority`: desempate numerico. Mayor gana.
- `active: false`: desactiva la regla.

Prioridad de match:

1. Anuncio/source_id
2. Conjunto
3. Campana
4. ctwa_clid
5. URL
6. Headline
7. Body/texto

### Opcion B: config/appConfig

Tambien puedes guardar un arreglo en:

`config/appConfig.metaAdSequenceRoutes`

```json
[
  {
    "name": "Campana paginas web",
    "campaignId": "120200000000010",
    "trigger": "LeadPaginaWeb"
  },
  {
    "name": "Anuncio tienda online",
    "adId": "120200000000001",
    "trigger": "LeadTiendaOnline"
  }
]
```

La coleccion `metaAdSequenceRoutes` y `config/appConfig.metaAdSequenceRoutes`
pueden coexistir. Si hay varias reglas compatibles, gana la mas especifica.

## Fallback

Si un mensaje viene de Meta Ads pero no existe regla especifica, el CRM usa:

`config/appConfig.defaultTriggerMetaAds`

Si ese campo no existe, usa:

`LeadWhatsapp`

## Hashtags en el mensaje inicial

Si el mensaje entrante de Click-to-WhatsApp trae un hashtag explicito, el CRM
lo respeta antes de caer al fallback de Meta. Para la campana de redes sociales
puedes usar:

`#RedesSociales`

Ese hashtag activa el trigger:

`PlanRedes`

Tambien se aceptan `#PlanRedes990`, `#PlanRedes` y `#Redes`.

Una regla especifica en `metaAdSequenceRoutes` sigue siendo la opcion mas
precisa cuando quieres enrutar por anuncio, conjunto o campana. El hashtag es
util para distinguir el mensaje inicial cuando Meta no entrega metadata
suficiente o cuando no hay una regla creada todavia.

## Flujo recomendado para configurar un anuncio

1. Crea la secuencia en el CRM con un trigger claro, por ejemplo
   `LeadTiendaOnline`.
2. Publica o identifica el anuncio Click-to-WhatsApp en Meta.
3. Manda un mensaje de prueba desde ese anuncio.
4. Revisa el lead creado en Firestore y copia `metaAdId` o `metaSourceId`.
5. Crea una regla en `metaAdSequenceRoutes` con ese ID y el trigger deseado.
6. Repite la prueba. El log debe mostrar `route=<id-del-documento>` y el lead
   debe quedar con `lastMetaSequenceTrigger`.
