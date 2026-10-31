# WaCapital · Registro de despliegue

31 de octubre de 2026 · Europe/Lisbon.

## Estado verificado

- Cloudflare Workers: plan **Free, $0**, comprobado en la cuenta del propietario.
- Servicio: https://wacapital-online.wacapital-online.workers.dev
- Salud pública: https://wacapital-online.wacapital-online.workers.dev/health
- Base nueva: wacapital-editorial (D1), cinco tablas; esquema aplicado.
- Se conserva el archivo histórico de Supabase sin modificar sus tablas.
- Recolección e IA activadas. Máximo seis llamadas editoriales/día UTC.
- Horario: minutos 3,13,23,33,43,53 de cada hora UTC.
- Bot verificado mediante getMe: @WazPulse_bot.
- Webhook conectado a /telegram/webhook; actualizaciones pendientes conservadas.
- Primera recolección confirmada: ocho noticias guardadas.
- Primera prueba con IA a las 22:38 UTC: un editorial y una notificación,
  ambos confirmados por el servicio. Telegram aceptó el envío.
- La aprobación y recepción del archivo por el propietario están pendientes
  de confirmación al redactar esta primera versión del registro.

## Acceso y secretos

El propietario autorizó la cuenta mediante el flujo oficial de dispositivos
de Wrangler con permisos de cuenta, Workers y D1. También autorizó copiar
TELEGRAM_BOT_TOKEN y TELEGRAM_CHAT_ID de Railway a Cloudflare y conectar el
webhook para una prueba en su Telegram.

Solo se trasladaron esos dos valores, almacenados como secretos cifrados.
No se incorporaron a archivos, commits ni mensajes. Al terminar la transferencia
se ocultaron los campos de Railway y se limpiaron las copias en memoria.

ADMIN_TOKEN y TELEGRAM_WEBHOOK_SECRET son secretos internos nuevos,
generados de forma aleatoria. La copia administrativa local usa DPAPI de
Windows y queda ignorada por Git, al igual que wrangler.local.jsonc.

No se introdujo información de pago ni se cambió el plan gratuito.

## Correcciones detectadas en la prueba real

La primera recolección falló antes de contactar al origen porque Workers no
admite redirect:error, aunque Node sí. Se sustituyó por redirect:manual y se
mantiene el rechazo de respuestas 3xx. Una prueba de regresión comprueba que
no se sigue otro destino.

Se añadieron diagnósticos limitados a fase/categoría/estado HTTP, sin URLs
de Telegram ni excepciones que pudieran incluir claves.

El cifrado local usa pwsh (PowerShell 7). Windows PowerShell 5 heredaba
rutas de módulos incompatibles; se comprobó la solución con una muestra sin
credenciales antes de usar los secretos internos.

## Validación

- 18 pruebas online: SQLite local real y red sustituida.
- 12 pruebas de recuperación Python, de la fase anterior.
- Esquema SQLite aplicado y consultado en D1 real.
- Compilación y despliegue confirmados por Cloudflare.
- Prueba real de recolección, IA y envío a Telegram confirmada.
- Pendiente: botón humano de aprobación, paquete recibido y comprobación
  posterior del horario automático.

## Referencias

- https://developers.cloudflare.com/workers/runtime-apis/request/
- https://developers.cloudflare.com/workers/platform/limits/
- https://core.telegram.org/bots/api#setwebhook
