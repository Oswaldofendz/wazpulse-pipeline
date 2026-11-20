# Imágenes y respaldos de WaCapital

Actualizado: 20 de noviembre de 2026. Política activa: coste adicional de imágenes de 0 €.

## Comportamiento

Los gráficos con cotizaciones conservan sus datos reales. Los retratos oficiales conservan sus créditos. Las noticias cuyo titular afirma una subida o caída de un activo reciben una ilustración original del activo y la dirección, sin inventar precios, porcentajes, ejes ni una curva histórica.

Para otras noticias, Cloudflare sigue siendo el generador principal. La cadena contempla cinco integraciones de IA: Cloudflare, Pollinations, Together, Stability y AI Horde. Los tres servicios autenticados intermedios están desactivados en la política gratuita y no hay claves instaladas para ellos. AI Horde solo recibe los asuntos compatibles con sus condiciones. Una ilustración editorial local permite completar el trabajo cuando no queda una IA disponible.

Cada intento remoto queda registrado antes del envío. Una respuesta perdida no repite ese mismo proveedor. Los fallos, la falta de saldo y los límites diarios permiten continuar; la cola gratuita espera hasta 120 segundos antes de utilizar la ilustración editorial. Los límites por proveedor cuentan también los intentos fallidos. No se siguen URL de descarga arbitrarias devueltas por terceros.

El límite interno `DAILY_VISUAL_LIMIT` controla los intentos de WaCapital; no representa el saldo total de Neurons de la cuenta de Cloudflare. Se conserva el límite existente. La ilustración local usa el renderizador existente, sin una llamada de generación de imágenes.

## Coste y credenciales

- `IMAGE_COST_POLICY=free-only`: política por defecto y activa. Bloquea los proveedores que requieren saldo aunque alguien añada sus claves o active accidentalmente otro interruptor.
- `EDITORIAL_FALLBACK_ENABLED=true`: permite terminar con arte editorial original.
- `IMAGE_BACKUP_WAIT_SECONDS=120`: espera de Horde, limitada a 60–600 segundos.
- `IMAGE_REMOTE_BACKUPS_ENABLED=false`: los servicios autenticados permanecen desactivados.

Una futura activación de pago requiere autorización nueva para cambiar la política, habilitar los respaldos y guardar las claves como secretos: `POLLINATIONS_API_KEY`, `TOGETHER_API_KEY`, `STABILITY_API_KEY`. Cada límite diario es independiente y se acota a un máximo de 20. No se deben guardar valores de claves en este repositorio.

## Contexto y revisión

Todos los proveedores reciben el mismo titular original, extracto, protagonistas y acción. La ilustración reconoce expresiones como “Bitcoin selloff”, “Bitcoin’s falling price” y “la caída del precio de Bitcoin”. No interpreta “Nvidia drops acquisition plans” como una caída de su cotización. La escena de una negociación usa diálogo diplomático, sin atribuir un acuerdo que la fuente no afirma.

El arte vectorial se etiqueta como ilustración editorial, no como fotografía de un suceso, imagen con IA ni gráfico de cotizaciones. Las noticias siguen pasando por aprobación antes de compartir. Una entrega incierta de Telegram nunca se reenvía por esta recuperación.

## Operación

`GET /admin/image-providers` muestra integraciones, configuración, política de coste y presupuestos, sin revelar claves.

`POST /admin/image-recovery/<story_id>` reanuda únicamente formatos ya solicitados, sin archivos generados o entregados, sin aprobación final y sin entregas inciertas. Conserva los borradores aún no solicitados para generación. Requiere el secreto administrativo y toma el mismo bloqueo que la cola visual.

Fuentes técnicas consultadas:

- [AI Horde: condiciones](https://aihorde.net/terms/).
- [Cloudflare: precios y asignación gratuita](https://developers.cloudflare.com/workers-ai/platform/pricing/).
- [Pollinations: API y autenticación](https://gen.pollinations.ai/docs).
- [Together: generación y salida Base64](https://docs.together.ai/docs/inference/images/overview).
- [Stability: autenticación, generación y respuestas](https://docs.stability.ai/api-reference).
