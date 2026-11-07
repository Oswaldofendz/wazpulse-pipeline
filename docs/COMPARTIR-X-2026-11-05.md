# WaCapital · Texto y portada al compartir desde el móvil

Prioridad iPhone/Safari, con alternativa Android/Chrome. El enlace de Web Intent
de X solo precarga texto; no se promete que adjunte archivos. El nuevo botón
**Compartir texto e imagen** abre una página del mismo Worker, precarga el PNG
exacto aprobado y su texto, y entrega ambos al menú nativo con Web Share.
El propietario elige X, revisa su borrador y pulsa publicar allí.

La integración no utiliza la API de pago de X, nuevas generaciones, cuentas,
proveedores de IA ni tareas con el PC encendido. No añade mensajes de Telegram:
la aprobación final actualiza el mensaje existente con el botón. `/publicar ID`
actualiza ese mensaje para trabajos nuevos aprobados; el flujo histórico conserva
su recuperación explícita de portada y texto y también ofrece el nuevo botón.

## Compatibilidad

- El archivo se descarga **antes** de habilitar compartir. El toque llama a
  `navigator.share` sin esperar red o portapapeles y conserva la activación iOS.
- Se comparten solo `files` y `text`, sin URL del material privado ni URL fuente.
- No se puede obligar al menú del sistema a elegir X ni verificar que X conserve
  los dos campos. La página avisa de revisar el borrador.
- Si Telegram abre un navegador integrado incompatible, se indica abrir Safari
  en iPhone o Chrome en Android. La página ofrece copiar texto, compartir solo
  imagen, guardar PNG y abrir X con texto. Las fotos siguen siendo originales.
- Cancelar el menú no publica ni dispara reintentos. Una promesa resuelta de
  compartir tampoco se registra como publicación confirmada.

## Acceso

La página pública es genérica y no contiene noticias ni identificadores.
El material requiere una capacidad HMAC-SHA256 válida por siete días, vinculada
a la noticia, tipo de aprobación, `file_id` de portada y texto aprobado.
Se reutiliza el secreto administrativo internamente, nunca se envía al navegador.
La capacidad está en el fragmento, no en rutas ni parámetros de consulta; el
navegador la envía solo en el cuerpo POST a los endpoints del mismo origen.

Los endpoints revalidan aprobación y entrega confirmada en cada lectura. Rechazan
materiales en revisión, descartados, envíos inciertos, cambios del texto o imagen,
firmas falsas, caducidad y origen ajeno. Descargan únicamente el archivo aprobado
desde Telegram, con límite de 1,9 MB y validación PNG. No revelan token, URL
privada de Telegram ni identificadores de archivo. Un enlace reenviado permite
acceder a su material durante su vigencia: se indica que es privado.

Respuestas sin caché, sin referer, sin indexación, sin marcos, sin analítica,
sin scripts externos y con CSP restrictiva. No cambia el esquema D1.

## Verificación

90 pruebas automáticas correctas, incluidas siete nuevas de permisos, firmas,
caducidad, vínculo con archivo/texto, lecturas y recuperación sin reenvío.
Prueba de navegador a 390 px: sin desbordamiento, entrega de un File PNG con
texto exacto y sin URL privada, alternativa de solo imagen, cancelación y modo
sin Web Share. El destino nativo se sustituyó en esa prueba; no acredita el
resultado de X en un iPhone o Android real. Esa comprobación debe hacerse con
el propietario antes de declarar compatibilidad completa.

La comprobación online obtuvo página 200, texto aprobado 200, PNG aprobado 200
(970.295 bytes) y firma manipulada 401, sin mensajes nuevos ni generación. Una
primera lectura agotó la espera de `getFile` de Telegram; se amplió solo esa
lectura de 8 a 25 segundos y la verificación posterior completó correctamente.
El diagnóstico guarda únicamente etapa, categoría y estado HTTP en administración
privada, sin tokens, enlaces, identificadores ni contenido. Si falla la descarga,
la página conserva el texto y ofrece la alternativa manual.

Despliegue final: `9c63a426-e72f-4278-8e10-31e2801114ab` (servicio 0.3.1).
También se comprobó en navegador el fallo simulado de descarga: se conserva el
texto aprobado, se oculta el enlace de imagen no disponible y se muestran las
opciones manuales. No se enviaron pruebas al bot ni se publicaron posts en X.

Referencias oficiales consultadas el 05/11/2026:

- https://www.w3.org/TR/web-share/
- https://webkit.org/blog/13862/the-user-activation-api/
- https://docs.x.com/x-api/getting-started/pricing
