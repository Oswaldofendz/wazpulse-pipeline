# WaCapital online · primera versión

Recoge noticias del agregador público de WaStake, prepara materiales editoriales
con su IA actual y los envía a revisión del propietario por Telegram.
Después de aprobar, entrega un archivo de texto con X, Instagram, carrusel,
guion TikTok/Shorts y dirección visual. La publicación en redes es manual.

La ampliación visual genera una ilustración con Workers AI, una portada PNG
para X y cinco láminas de carrusel, entregadas en Telegram para una segunda
aprobación. Consulta [el funcionamiento, límites y recuperación](../docs/IMAGENES-2026-10-31.md).
La plantilla mantiene MEDIA_ENABLED=false; la configuración privada de la
cuenta real determina si está activada.

Estado al 31 de octubre de 2026: desplegado en Cloudflare Free, D1 conectado,
recolección y análisis activados; Telegram conectado. Se confirmó una primera
noticia analizada y enviada al propietario, su aprobación y la entrega del
archivo. La prueba completa se documenta en
[el registro del despliegue](../docs/DESPLIEGUE-2026-10-31.md).

## Alojamiento y costes

Destino: Cloudflare Workers **Free** + D1 **Free**. El archivo histórico de
Supabase se conserva sin modificarlo ni trasladar sus credenciales.
La nueva cola D1 está asociada únicamente al Worker y no tiene una API pública
para leer sus noticias, textos o identificadores personales.

Se usa el backend actual de WaStake en Render para noticias y redacción.
No se añade una suscripción ni un proveedor de IA de pago. El código no
puede consultar ni garantizar la modalidad de facturación de las cuentas
externas: activar IA requiere conservar Groq/Gemini en las cuotas gratuitas.
Una clave disponible no prueba por sí sola que una cuenta sea gratuita.

Límites iniciales:

- Recolección cada diez minutos, en minutos 3, 13, 23, 33, 43 y 53 UTC.
- La ampliación visual avanza un paso por minuto fuera de esos minutos.
- Hasta ocho noticias incorporadas por ciclo; antigüedad máxima de 24 h.
- Análisis solo de noticias de las últimas seis horas.
- Relevancia inicial mínima 65/100, y editorial mínima 4/5.
- Una llamada editorial por ciclo y seis al día UTC por defecto.
- Límite configurable de llamadas entre 0 y 16; también cuenta los fallos.
- Como máximo un envío automático de revisión por ciclo.
- La cola se detiene para incorporar noticias al llegar a 5.000 registros.
- La aprobación formatea el texto revisado y, si MEDIA_ENABLED=true,
  encola la ilustración: presupuesto separado de tres intentos/día UTC.

El límite mide llamadas a /api/analysis/news-angle, no las llamadas internas
que ese backend pueda hacer a Groq/Gemini durante un reintento o fallback.
No sustituye los límites del proveedor.

Cloudflare Free tiene 10 ms de CPU por invocación y 100.000 solicitudes/día;
D1 tiene sus propios límites de lectura/escritura y espacio. La espera de red
no cuenta como CPU, pero sí el procesamiento de JSON y el código local.
El paquete es pequeño; el cumplimiento real de CPU se medirá en Cloudflare.
Si se supera una cuota o falla una fuente, la aplicación debe quedar pendiente,
sin cambiar de plan. El flujo editorial se verificó en producción; la ampliación
visual tiene su propio registro de pruebas y entrega.

La consulta periódica recoge noticias reales; no es un ping artificial para
evitar que Render duerma. Si Render está apagado o arrancando, el ciclo puede
fallar y se vuelve a intentar en el siguiente horario. No hay garantía de
entrega instantánea ni cobertura de todos los acontecimientos.

## Controles implementados

- PIPELINE_ENABLED y AI_ENABLED están en false de forma inicial.
- Fuente fija de noticias: /api/market/impact-alerts del backend de WaStake.
- Se aceptan únicamente enlaces HTTPS de las nueve fuentes actuales
  (Fed, BCE, CNBC, CoinDesk, CoinTelegraph y BBC); no se sigue ningún
  enlace del artículo ni se realiza scraping desde este Worker.
- Fechas futuras, antiguas, fuentes marcadas como obsoletas y opiniones
  identificadas por el agregador quedan excluidas.
- No se aceptan puntuaciones de IA ausentes, fraccionarias o fuera de 1–5.
- Deduplicación por enlace canónico y clave de evento, además de un bloqueo
  de ciclo y un presupuesto diario persistente.
- Telegram requiere secreto del webhook, chat y propietario coincidentes.
  Los grupos requieren TELEGRAM_APPROVER_ID explícito.
- Una aprobación debe corresponder al mensaje original y estado pendiente.
- La bandeja persistente permite recuperar aprobaciones tras reinicios.
- Los envíos con resultado incierto se retienen para intervención del usuario;
  no se reenvían a ciegas.
- No hay llamadas a publicadores de redes, generación de imágenes de pago,
  borrado automático de archivos o cambios al esquema antiguo de Supabase.

Los textos se basan en titular y extracto RSS. La valoración de relevancia
es una heurística editorial, no una auditoría factual, una probabilidad de
mercado o una señal de inversión. El revisor debe abrir la fuente original.

## Preparación local

Requiere Node 22.13 o posterior. Ejecutar dentro de online:

    npm ci
    npm test
    npx wrangler deploy --dry-run --outdir .wrangler/build

Las pruebas usan SQLite real local y red sustituida. No contactan al bot.
La construcción con --dry-run no publica ni crea recursos en Cloudflare.

## Activación en la cuenta del propietario

1. Iniciar sesión en una cuenta de Cloudflare que conserve el plan Free.
   Si la cuenta debe crearse, el propietario completa sus términos y acceso.

2. Desde online:

       npx wrangler login
       npx wrangler d1 create wacapital-editorial

   Copiar el identificador REAL devuelto por el segundo comando, y ejecutar:

       node scripts/prepare-config.mjs ID_REAL_DE_D1
       npx wrangler d1 execute wacapital-editorial --remote --file schema.sql --config wrangler.local.jsonc

   Estos comandos crean únicamente una cola nueva. No usan Supabase.
   No ejecutar SQL de la reconstrucción histórica sobre la base antigua.

3. Introducir los secretos en Cloudflare, uno por uno. Los comandos solicitan
   el valor en privado; no pegarlos en el chat ni guardarlos en Git:

       npx wrangler secret put TELEGRAM_BOT_TOKEN --config wrangler.local.jsonc
       npx wrangler secret put TELEGRAM_CHAT_ID --config wrangler.local.jsonc
       npx wrangler secret put TELEGRAM_APPROVER_ID --config wrangler.local.jsonc
       npx wrangler secret put TELEGRAM_WEBHOOK_SECRET --config wrangler.local.jsonc
       npx wrangler secret put ADMIN_TOKEN --config wrangler.local.jsonc

   El token y el usuario/chat de Telegram deben ser los reales del propietario.
   TELEGRAM_WEBHOOK_SECRET debe ser un secreto nuevo aleatorio, entre 16 y
   256 caracteres alfanuméricos, guion o guion bajo. ADMIN_TOKEN es otro
   secreto independiente. No reutilizar claves de Supabase ni proveedores IA.

4. Desplegar inicialmente en pausa:

       npx wrangler deploy --config wrangler.local.jsonc

   Abrir la URL REAL que devuelva Cloudflare, seguida de /health.
   Debe mostrar service=wacapital-online, enabled=false y databaseBound=true.
   El identificador de cuenta, base y URL se incorporan solo cuando existan.

5. Activar PIPELINE_ENABLED=true en la configuración local y volver a desplegar.
   Mantener AI_ENABLED=false para comprobar la recolección sin gastar cuota IA.
   Consultar /admin/status con Authorization: Bearer ADMIN_TOKEN mediante
   una herramienta privada; no incluir ese token en una URL.

6. Conectar el bot. El proceso viejo de polling debe permanecer detenido.
   El propietario configura temporalmente TELEGRAM_BOT_TOKEN y
   TELEGRAM_WEBHOOK_SECRET en su terminal privada, con los mismos valores
   que guardó en Cloudflare. Ejecutar:

       node scripts/connect-telegram.mjs https://URL_REAL_DEL_WORKER.workers.dev

   El script valida el Worker y registra el webhook sin borrar los mensajes
   pendientes. Este paso se verificó en la instalación del 31/10/2026.

7. Enviar /estado al bot desde el usuario autorizado. Comprobar la respuesta.
   Después activar AI_ENABLED=true, confirmar las cuotas gratuitas de las
   cuentas de IA que ya usa WaStake y probar UNA noticia nueva.

8. Aprobar esa noticia en Telegram y comprobar el archivo recibido.
   Verificar que un usuario ajeno y un botón repetido no generan otra entrega.
   Medir CPU/errores en Cloudflare antes de dejar activo el horario.

## Comandos del bot

- /estado: estado del flujo y llamadas editoriales del día.
- /pendientes: hasta cinco noticias pendientes recientes.
- /paquetes: últimos cinco paquetes aprobados y estado de su entrega.
- /revisar ID: reenvío solicitado por el propietario; invalida botones del
  mensaje anterior cuando se guarda el nuevo.
- /paquete ID: vuelve a enviar un paquete aprobado, bajo solicitud explícita.

Los ID aparecen en /pendientes y en el nombre del paquete. Los archivos
contienen borradores y guiones, no vídeos renderizados ni imágenes.

## Verificación realizada

- Las pruebas de esta versión usan SQLite real y red controlada; el recuento
  y las comprobaciones finales están en el registro del despliegue.
- Construcción y despliegue con Wrangler 4.156.0 confirmados.
- Instalación: auditoría npm con cero vulnerabilidades informadas.
- Prueba de lectura pública: HTTP 200, nueve fuentes, veinte noticias
  recibidas, diecisiete aceptadas por los filtros en el momento de consulta.
- La autorización de Cloudflare se completó con el propietario, y después
  se verificó el plan Free ($0) en su panel.
- El propietario autorizó explícitamente trasladar solo el token y destino
  de Telegram desde Railway a los secretos cifrados de Cloudflare. No se
  imprimieron sus valores ni se copiaron otras credenciales.
- Se corrigió una diferencia real del entorno Workers: no admite
  redirect:error. Se usa redirect:manual y se rechazan respuestas 3xx.
- Se generan dos secretos internos aleatorios. Su copia local se guarda
  cifrada con DPAPI de Windows, fuera de Git. Los ayudantes administrativos
  requieren PowerShell 7 (pwsh) y nunca imprimen esos secretos.

## Operación desde el PC autorizado

Los secretos internos ya configurados permiten usar:

    node scripts/admin.mjs status
    node scripts/admin.mjs telegram-info

El comando tick ejecuta un ciclo real: con IA activa puede consumir una llamada
del presupuesto diario y enviar una noticia a Telegram. No usarlo como sondeo.

El registro del webhook puede gestionarse en el propio Worker con
telegram-connect. Requiere administración autenticada, propietario configurado
y WORKER_ORIGIN igual al dominio que recibe la petición. No revela el token
del bot y conserva las actualizaciones pendientes.

Los botones antiguos de Railway no corresponden a la nueva cola D1. El
historial de Supabase se conserva, y las nuevas aprobaciones se hacen en los
mensajes que entregue este servicio.

## Siguiente iteración

Comprobar fuentes por activo, reglas de agrupación de eventos repetidos y
derechos de materiales. Añadir producción de carruseles/vídeo después de
validar esta primera entrega, sin convertir imágenes IA en una dependencia
de pago. Reutilizar el prototipo WaNews requiere su propia auditoría.

## Referencias

- https://developers.cloudflare.com/workers/platform/limits/
- https://developers.cloudflare.com/workers/platform/pricing/
- https://developers.cloudflare.com/d1/platform/limits/
- https://developers.cloudflare.com/workers/configuration/cron-triggers/
- https://developers.cloudflare.com/d1/worker-api/prepared-statements/
- https://core.telegram.org/bots/api#setwebhook
