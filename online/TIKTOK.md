# Borradores de TikTok desde Telegram

El carrusel aprobado muestra «Compartir en TikTok». Su página conserva la
descripción y las cinco láminas en orden. Con la integración activada, el
propietario pulsa «Enviar borrador a TikTok»: el servidor usa exclusivamente
`MEDIA_UPLOAD`, y el usuario termina la edición y publicación en la app,
desde la notificación de la Bandeja de entrada. La API no devuelve un enlace
documentado al editor de ese borrador. Abrir el sitio de TikTok por sí solo
no permite adjuntar fotografías ni garantiza abrir la app.

La opción de compartir con el menú del móvil sigue disponible. Que TikTok
aparezca y reciba los archivos depende del teléfono y de su versión; esa
opción no equivale a la carga oficial como borrador.

## Elegibilidad de producción: comprobación del 8 de noviembre de 2026

Las App Review Guidelines generales, además de las reglas específicas de
Direct Post, indican que las aplicaciones no pueden ser de uso privado o
personal y exigen un sitio web desarrollado y accesible al público. Esto
afecta al proceso de revisión de una aplicación que pide `video.upload`.
El bot actual es una herramienta interna de WaCapital. Completar el icono,
la descripción o las páginas legales no demuestra que cumpla ese requisito.
No se debe presentar el servicio como una plataforma pública o multicuenta
para conseguir aprobación: esa no es su implementación actual.

El siguiente paso es comprobar el flujo en Sandbox con una cuenta propia
añadida como Target user. Sandbox permite probar sin revisión previa, con
restricciones, y no ofrece Content Posting API para vídeos públicos. La
documentación no confirma aquí que un carrusel enviado con MEDIA_UPLOAD
desde Sandbox pueda terminar como post público en la app. Se necesita una
prueba real del permiso, la carga y la notificación; la prueba local con red
simulada no confirma esa capacidad ni una aprobación de producción.

Hasta entonces se conserva el botón de compartir del móvil y la carga API
permanece desactivada. No se crean credenciales de producción, ni se
publican condiciones o datos de un responsable inventados.

Referencias:

- https://developers.tiktok.com/docs/en/app-review-guidelines
- https://developers.tiktok.com/docs/en/add-a-sandbox

## Activación técnica pendiente

Se necesita una aplicación real en TikTok for Developers, Login Kit para
web y Content Posting API con el permiso de carga `video.upload` autorizado.
El propietario debe autorizar también `user.info.basic`, para identificar
la cuenta conectada en la página de envío. No se solicita `video.publish`.
La elegibilidad descrita arriba debe resolverse antes de solicitar una
revisión de producción. Esta integración no utiliza Direct Post.

Datos para registrar en la aplicación:

- Callback: `https://wacapital-online.wacapital-online.workers.dev/tiktok/callback`
- Prefijo de medios: `https://wacapital-online.wacapital-online.workers.dev/tiktok/media/`
- Permisos: `user.info.basic,video.upload`
- Los carruseles necesitan TikTok 31.8 o superior, según su documentación.

Pasos del operador:

1. Aplicar `tiktok-schema.sql` a D1 antes de desplegar esta versión. Es aditivo.
2. Guardar `TIKTOK_CLIENT_KEY` y `TIKTOK_CLIENT_SECRET` como secretos del Worker
   desde Cloudflare o los prompts privados de `wrangler secret put --config
   wrangler.local.jsonc`. No poner valores en Git, mensajes o capturas.
3. Verificar el prefijo de medios desde el portal de TikTok. Si proporciona
   un archivo de texto, configurar su nombre exacto como
   `TIKTOK_VERIFICATION_FILENAME` y su texto exacto como
   `TIKTOK_VERIFICATION_CONTENT`. El Worker sirve solo ese `.txt` bajo el
   prefijo de medios; no ofrece un explorador de archivos.
4. Solo tras confirmar la verificación y los permisos de la aplicación,
   configurar `TIKTOK_MEDIA_VERIFIED=true` y `TIKTOK_UPLOAD_ENABLED=true` en
   la configuración privada. Desplegar y enviar `/conectartiktok` al bot.
5. Abrir el enlace privado de 15 minutos, autorizar la cuenta correcta y
   volver al carrusel aprobado en Telegram. Si el acceso falla en el
   navegador de Telegram, abrir el enlace en Safari en iPhone o Chrome en Android.
6. Hacer la primera prueba con un carrusel aprobado real. Verificar cuenta,
   cinco láminas, orden y descripción en la notificación de TikTok. El botón
   no publica. La aprobación final y los ajustes del post ocurren en TikTok.

## Comportamiento y límites

- Solo se aceptan enlaces privados de material aprobado y entregado. Cambiar
  archivos, texto o aprobación invalida el acceso anterior.
- Se convierten los PNG aprobados a JPEG de 1080 × 1920, sin otra generación
  de IA. Las copias se sirven con enlaces firmados por dos horas y se borran
  al siguiente mantenimiento; se conserva el identificador del envío.
- El contenido ilustrado con IA se marca con `is_aigc=true`; gráficos y
  montajes de retratos oficiales no se marcan como imágenes generadas.
- Cinco intentos en 24 horas como máximo. TikTok además puede rechazar envíos
  cuando haya cinco cargas pendientes. No hay cambio a un plan de pago.
- Se registra la intención antes de contactar con TikTok. Doble pulsación,
  recarga o respuesta perdida no vuelven a inicializar el envío. Un estado
  incierto exige comprobar la Bandeja de entrada antes de otra carga manual.
- Los tokens están cifrados con AES-GCM en D1. OAuth usa estado de un solo
  uso y una cookie de navegador segura; el inicio requiere un enlace emitido
  únicamente al propietario del bot. Nunca se conecta una cuenta desde el
  enlace de un carrusel compartido. La rotación de refresh tokens se guarda
  y una respuesta perdida de renovación exige reconectar.
- La página consulta el estado sin enviar de nuevo. La comprobación
  automática tiene un máximo de doce consultas; después se usa el botón.
- No hay programación de posts públicos, ni cambios en la cuenta, ni acceso
  a sus mensajes. Hasta completar la activación, la carga API está apagada.

Referencias oficiales, consultadas el 7 de noviembre de 2026:

- https://developers.tiktok.com/docs/en/content-posting-api-reference-photo-post
- https://developers.tiktok.com/docs/en/content-posting-api-get-started-upload-content
- https://developers.tiktok.com/docs/en/content-posting-api-media-transfer-guide
- https://developers.tiktok.com/docs/en/content-posting-api-reference-get-video-status
- https://developers.tiktok.com/docs/en/login-kit-web
- https://developers.tiktok.com/docs/en/oauth-get-a-user-access-token
- https://developers.tiktok.com/docs/en/oauth-user-access-token-management
