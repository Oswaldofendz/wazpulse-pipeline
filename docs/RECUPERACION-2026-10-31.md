# WaCapital: proyecto localizado y primera recuperación

Fecha de revisión: 31 de octubre de 2026.

Decisión confirmada por Oswaldo en esta revisión: funcionamiento online con
el PC apagado, aceptando límites gratuitos. La ejecución local no es el destino.

Actualización: se implementó la primera adaptación en [online](../online/README.md)
con una cola privada D1 independiente del archivo de Supabase. Pasan 16 pruebas
de la versión online, además de las 12 de recuperación Python. La construcción
oficial de Cloudflare pasa; el despliegue y la prueba real de Telegram siguen
pendientes de acceso/configuración. No hay una sesión de Cloudflare iniciada
y el control del navegador estaba desactivado en el momento de continuar.

## Identidad y ubicaciones verificadas

WaCapital es la marca editorial. WallNews/WazPulse son nombres anteriores;
el repositorio conserva el nombre wazpulse-pipeline y las tablas pulse_*.
WaStake es la web de análisis que proporciona parte del contexto y la IA.

- GitHub: https://github.com/Oswaldofendz/wazpulse-pipeline
- Rama remota main, base recuperada 4312d30 (15 agosto 2026).
- Copia C:\Dev\WaGroup\wazpulse-pipeline: 5b6d446 (29 mayo), limpia.
- Copia OneDrive\Escritorio\Dexter\WaGroup\wazpulse-pipeline: c89599c (11 mayo).
- Documentos: HANDOFF_WACAPITAL.md, HANDOFF_OPUS.md, HANDOFF_TO_SONNET.md
  en C:\Users\oswal\OneDrive\Escritorio\Dexter\WaGroup.
- WaNews\files: otro prototipo llamado WallNow v2, con RSS, Gemini,
  voz, clips Pexels y ensamblado de vídeo. Es material reutilizable, no el
  bot editorial desplegado.
- WaFinance\WaFinance\marketpulse: una copia antigua de la aplicación WaStake.

Los documentos son históricos, con algunas contradicciones de fechas,
ramas y estado. Se contrastaron con el código y los paneles actuales.
No se ejecutaron sus instrucciones de borrado, SQL, publicación o pago.

## Arquitectura recuperada

RSS → pulse_candidates → IA de WaStake → pulse_posts → Telegram →
aprobación humana → materiales para redes / publicadores opcionales.

El código Python programa ciclos cada cinco minutos y consulta respuestas de
Telegram cada diez segundos entre ciclos. El procesamiento pesado puede
retrasar ese sondeo.

La IA editorial se solicita a POST /api/analysis/news-angle. El contexto
procede de GET /api/wapulse/snapshot. La ruta de snapshot respondió HTTP 200
en el backend actual https://wastake.onrender.com durante esta revisión.
Las rutas editoriales y visuales siguen presentes en el código de WaStake.
No se realizó una llamada editorial de pago o una generación real para probarlas.

Supabase almacena datos y archivos; no es el generador de imágenes.
Pillow compone las tarjetas y carruseles. El código histórico también usa
Pollinations e Imagen; su disponibilidad gratuita actual no queda garantizada.

## Estado real de servicios y contenido

Railway: proyecto balanced-patience, servicio wazpulse-pipeline apagado,
trial vencido. El último despliegue visible falló durante la construcción;
los anteriores figuran retirados. No se reactivó ni contrató un plan.

Supabase: proyecto WaFinance, región París, plan Free. Conserva
pulse_candidates, pulse_posts, pulse_sources_config, pulse_state y pulse_metrics.
El editor mostró **2.855 registros en pulse_posts**. No significa que todos
estén aprobados o publicados. Se comprobó la existencia de titulares históricos.

El panel marcaba Unhealthy y un aviso de fin de periodo de gracia, aunque
permitía consultar tablas. Mostraba aproximadamente 64/500 MB de base de datos
y 0,03/1 GB de archivos en el ciclo actual. No se debe concluir que el problema
actual sea exceso de almacenamiento: falta comprobar el componente no saludable.
Tampoco se ha verificado íntegramente el esquema de aprobación, las políticas
de acceso o la disponibilidad de todos los archivos.

Funciones existentes:

- Recolección RSS, deduplicación, filtros editoriales y selección por relevancia.
- Textos para X e Instagram; copy TikTok y Facebook reutiliza otros textos.
- Prompts de imágenes para X, Instagram, YouTube y TikTok.
- Bot de aprobación y generación de briefs.
- Tarjetas y carruseles.
- Publicador X a través de Make; la documentación menciona un intermediario
  de pago. No se debe activar bajo el requisito gratuito.
- Publicador TikTok, históricamente limitado a sandbox/SELF_ONLY y dominio
  Railway verificado. Migrar el dominio exige volver a revisar esa integración.
- No hay publicadores completos de Instagram y YouTube en este repositorio.
  Un prompt de miniatura no equivale a un vídeo terminado.

## Primera tanda de correcciones locales

Rama recovery/free-editorial, copia aislada en wacapital-recovery.
No modifica la copia original, WaStake en producción, Railway ni Supabase.

1. Importar config en editorial_generator: se usaba sin importarlo al generar
   posts y completar tarjetas, provocando NameError.
2. Cambiar la URL predeterminada del backend al Render actual. Una variable
   antigua WASTAKE_API_URL puede sobreescribirla: hay que corregirla al activar.
3. Rechazar relevancias ausentes, booleanas, no numéricas o fuera de 1–5.
   Antes una relevancia cero podía eludir el filtro.
4. Seleccionar candidatos detectados en las últimas 24 horas para evitar
   reprocesar la cola histórica. Esto no sustituye validar la fecha original
   de publicación: queda pendiente reforzar feeds sin fecha o con fechas futuras.
5. Validar el chat y el usuario que pulsa el botón. Los grupos necesitan
   TELEGRAM_APPROVER_ID explícito; en privado se acepta únicamente al dueño.
6. Vincular cada callback al mensaje original y hacer la aprobación/rechazo
   condicional al estado pending_approval, evitando reaprobaciones.
7. Publicación social apagada por defecto, incluso con credenciales antiguas
   presentes: ENABLE_SOCIAL_PUBLISHING=false.
8. Borrado automático de almacenamiento apagado por defecto:
   ENABLE_STORAGE_CLEANUP=false. Una recuperación no debe eliminar archivos.
9. Imágenes siguen apagadas por defecto, como en la versión remota:
   ENABLE_CARD_IMAGES=false.

Validación: 12 pruebas offline pasan, usando sustitutos de red/base de datos.
Cubren aprobación ajena, grupos, mensajes incorrectos, doble aprobación,
configuración ausente, relevancia inválida, bloqueo de publicadores y sintaxis.
No certifican la integración en vivo ni la compatibilidad de todas las dependencias.

El PC tiene Python 3.14; el proyecto declara 3.12 y dependencias antiguas.
El diagnóstico local identifica dependencias faltantes sin leer .env:

    python scripts/recovery_check.py
    python -m unittest discover -s tests -v

No ejecutar src.main contra la base real durante el diagnóstico: envía noticias,
procesa aprobaciones y escribe datos cuando está configurado.

## Continuación recomendada sin suscripciones

### Fase 1 — Validar recuperación

1. Comprobar la salud y definición de las tablas en Supabase, en lectura.
   Verificar approved_by_user, approved_at, telegram_message_id y políticas RLS.
   No ejecutar el antiguo init_schema.sql a ciegas: es una reconstrucción
   incompleta y propone desactivar protecciones.
2. Preparar entorno Python 3.12 aislado y comprobar dependencias fijadas.
3. Revisar con muestras las fuentes activas, fechas, enlaces y filtros;
   no presentar una puntuación de la IA como verificación factual.
4. Probar una noticia y una aprobación autorizada, con publicación apagada.
   Solo después habilitar el envío periódico al Telegram del propietario.

### Fase 2 — Entrega útil

Tras aprobar, entregar un paquete descargable: texto para X, caption/carrusel
Instagram, guion vertical para TikTok/Shorts y miniatura, fuentes, fecha y
distinción entre hechos e interpretación. Comenzar con publicación manual.
Producir imágenes con plantillas locales y recursos con licencia verificada;
la generación de imágenes con IA será opcional, nunca una dependencia pagada.
El prototipo WaNews puede aportar voz y vídeo tras auditar licencias y código.

### Fase 3 — Alojamiento

La recuperación Python sirve como base y banco de pruebas local.
Para cumplir el funcionamiento online elegido, recomiendo una adaptación ligera
a Cloudflare Workers Free: horarios para recolectar, webhook Telegram y
consultas pequeñas a Supabase/IA. No es un traslado directo de Pillow/Python:
hay que separar la generación gráfica y medir CPU, tamaño de feeds y cuotas.

Render Free no es un reemplazo fiable de un worker permanente: los servicios
web duermen tras 15 minutos sin tráfico y los background workers no tienen
plan gratuito. No usar pings artificiales para sostener un proceso.

No se ha activado ningún servicio nuevo ni se promete gratuidad ilimitada.
Los límites gratuitos deben detener trabajo, no provocar contratación
automática. Si no queda cuota de IA, se conserva la noticia pendiente.

## Riesgos pendientes prioritarios

- Idempotencia al insertar posts y enviar Telegram si la red falla entre envío
  y actualización: falta restricción única/verificación de reintentos.
- Identidad y permisos del bot, errores que podrían registrar URLs con token.
- Estado real de Supabase y cobertura de su esquema; no consultar pulse_state
  indiscriminadamente porque puede contener información de autenticación.
- Callbacks largos de prompts/carruseles y límites de longitud de Telegram.
- Diferencia entre sentimiento del titular, relevancia editorial y semáforo
  técnico: hoy pueden mezclarse; la redacción debe explicitar su base.
- Trazabilidad, derechos de imágenes/texto, fecha y fuente en cada publicación.
- Actualización de proveedores y costes: no heredar la suposición de que
  Pollinations, Imagen, Make o APIs sociales siguen siendo gratuitos.

## Fuentes oficiales consultadas

- https://render.com/docs/free
- https://render.com/docs/migrate-from-heroku
- https://supabase.com/pricing
- https://developers.cloudflare.com/workers/platform/pricing/
- https://developers.cloudflare.com/workers/platform/limits/
- https://ai.google.dev/gemini-api/docs/pricing
- https://core.telegram.org/bots/api

Cloudflare Free declara 100.000 solicitudes/día y 10 ms CPU por ejecución.
Supabase Free declara 500 MB de base y 1 GB de archivos, con otras cuotas.
Son condiciones actuales de terceros, no garantías permanentes.
