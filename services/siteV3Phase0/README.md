# Negocios Web v3 — base local de Fase 0

Estado: biblioteca aislada, **sin montar en Next/Express ni habilitar para tráfico**.
No inicia Firebase, carga variables de entorno, invoca proveedores ni escribe en
`Negocios`. `SiteV3Service` está deshabilitado por defecto. Las únicas escrituras
de las pruebas ocurren en `MemoryStore` con fixtures ficticias.

## Archivos y fronteras

| Módulo | Responsabilidad |
| --- | --- |
| `contracts.mjs`, `schema/*.json`, `types.d.ts` | Cinco DTOs cerrados, límites y versiones. JSON Schemas draft 2020-12 exportables. Tipos derivados de la misma fuente. |
| `validation.mjs` | Referencias, secciones/variantes, capacidades, imágenes, eventos, WhatsApp y namespace de carrito. |
| `access.mjs` | Verificador de sesión inyectado y permiso por negocio consultado en almacenamiento privado. Firebase exige `verifyIdToken(token, true)`. |
| `facts.mjs` | Propuestas de hechos separadas de su confirmación privada. Compilador determinista y gate de publicación por recompilación exacta. |
| `store.mjs` | Adapter Firestore inyectado y doble transaccional serializable. No importa el inicializador Firebase del Core. |
| `service.mjs` | Edición condicional, jobs durables, leases/fencing, heartbeat, recuperación acotada, revisión inmutable, aprobación y publicación atómica. |
| `http.mjs` | Handlers compatibles con Express, probados directamente, **sin registrar rutas**. |
| `compatibility.mjs` | Decisión preparatoria del dispatcher, sin conectarla al renderer actual. |
| `fixtures/`, `test/` | Ejemplos ficticios y pruebas con red bloqueada. |

## Confirmación y publicación

1. Un administrador debe provisionar membresía privada en
   `SiteV3Access/{negocioId}/members/{uid}`. El navegador no crea permisos.
2. `createProject` crea solo un proyecto v3, sin modificar clientes existentes.
3. `saveBrief` coteja `expectedInputRevision`, invalida confirmaciones anteriores
   e incrementa la revisión de entrada. El contacto declarado aún no habilita CTA.
4. El propietario confirma explícitamente IDs de hechos con `confirmFacts`.
   La evidencia registra actor, negocio, fecha y fuente en servidor. Un editor o
   un campo `verified` del navegador no puede producir esta confirmación.
5. `requestGeneration` persiste un único job por hash de entrada, hechos, grants
   y versiones. La respuesta duplicada conserva el mismo ID.
6. Un worker con permiso del negocio toma lease y fencing token. El worker que
   perdió lease, token o revisión de entrada no puede completar el resultado.
7. `completeJob` acepta únicamente el resultado del compilador factual exacto.
   Crea snapshot con `create`, sin sobrescribir una revisión anterior.
8. `approve` vincula actor, hash y revisión. `publish` coteja también el puntero
   de publicación esperado y escribe snapshot público + puntero en transacción.
   Una edición conserva la publicación anterior hasta otra aprobación/publicación.

La confirmación prueba la declaración del propietario autorizado; no certifica
la veracidad material de esa declaración. Fuente `legacy_unverified` no se
promueve automáticamente. Testimonios, proyectos, políticas y copy IA no tienen
compilador publicable en Fase 0. Los campos estructurales existen para evolucionar
el contrato; el gate de publicación sigue limitado al subconjunto determinista.

## Recuperación

`recoverExpiredJobs` consulta jobs persistidos, vuelve a comprobar acceso por
negocio y toma un nuevo token creciente. Incluye jobs queued y leases vencidos.
Revisión anterior queda `superseded`; al agotar intentos queda `failed`.
Debe invocarse desde un consumidor durable al iniciar y periódicamente; **ese
runner aún no está conectado al cron existente**. El índice propuesto está en
la documentación de Fase 0. No se promete recuperación automática en producción.

## WhatsApp y capacidades

Sin confirmación o con formato inválido, `buildWhatsAppLink` devuelve `null` para
acciones deshabilitadas. No hay fallback al lead ni `#`. La normalización inicial
admite conservadoramente MX/US/CA; los demás países permanecen inválidos hasta
validar un adapter más completo. No se verifica la existencia de cuenta WhatsApp.
No hay apertura del navegador ni envío en esta biblioteca.

Las fixtures JSON no incluyen número. El número sintético US con bloque 555 de
los tests solo construye cadenas dentro del proceso; nunca se abre ni se envía.
El piloto comercial sigue necesitando un destino de prueba autorizado.

`grants` es configuración exclusivamente de servidor. El compilador no concede
checkout de hotel, agenda confirmada, pagos ni formularios por elegir una familia.
Las acciones no WhatsApp permanecen deshabilitadas: su payload discriminado
requiere congelar los adapters correspondientes. Generación de imágenes y
activación de pagos fallan cerradas aunque haya permiso; no tienen ejecutor.

## Contratos

`node packages/site-v3/export-contracts.mjs` exporta schemas/tipos desde la raíz NW;
`--check` comprueba deriva. El validador interno implementa exclusivamente las
keywords usadas por estos schemas; no es un motor JSON Schema general.
`validateContract` es estructural; `validatePlan`, `validateSite`,
`validateImageRequest`, `validateCommercialEvent` y `assertPublishable` añaden los
controles semánticos y de autoridad. **Validar solo JSON no autoriza publicar.**

Se concretan los campos antes no tipados: address lleva texto+sourceFactId;
hours lleva days/opens/closes/sourceFactId; socialLinks lleva platform/url/sourceFactId.
Los tipos de valor factual están acotados; evidencia compleja requerirá una
extensión versionada. No se aceptan URLs libres, HTML, base64 o permisos del modelo.
El compiler inicial emite únicamente muestras `sample_noindex`, sin medios:
la fotografía real del piloto y su registro de assets pertenecen a Fase 1.

`rendererVersion: 3.0.0` es el identificador **reservado** por el contrato; todavía
no hay un bundle de renderer v3. `publish` falla con `RENDERER_NOT_AVAILABLE` si
la versión no aparece en `rendererVersions`, que por defecto está vacío. Solo
los tests inyectan un manifiesto ficticio para verificar el commit atómico.
No se debe registrar un bundle hasta implementar y validar el renderer de Fase 1.

## Ejecución local

Desde NW:

```sh
node --import ./packages/site-v3/test/offline.mjs --test packages/site-v3/test/contracts.test.mjs packages/site-v3/test/service.test.mjs packages/site-v3/test/http.test.mjs
node packages/site-v3/export-contracts.mjs --check
node scripts/site-v3/check-legacy-integrity.mjs
python3 scripts/site-v3/sync-core.py
```

El último comando comprueba paridad de la copia local del Core. Solo `--apply`
escribe, con permisos de filesystem cuando corresponda. Rechaza otra rama y
archivos ajenos modificados. NW mantiene la fuente canónica; Core recibe una
copia idéntica para evitar imports entre repositorios o dependencias de paths
de escritorio en producción. Cambiar ambos manualmente no está soportado.

En Core, tras copiar:

```sh
node --import ./services/siteV3Phase0/test/offline.mjs --test services/siteV3Phase0/test/contracts.test.mjs services/siteV3Phase0/test/service.test.mjs services/siteV3Phase0/test/http.test.mjs
```

No se ha probado Firestore real/emulador ni SDK Auth real. Los tests de adapters
verifican el protocolo con dobles; faltan emulador, reglas, membresías reales,
índices y composición autenticada antes de montar cualquier endpoint.
