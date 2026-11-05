# WaCapital · Revisión móvil y generación por red

## Comportamiento

La primera notificación contiene el titular, la fuente y una fecha corta. Ofrece
Twitter/X, TikTok, Instagram y descartar. Elegir una red aprueba la redacción y
abre un único borrador de ese formato, sin generar ilustraciones todavía.

El segundo botón prepara solo los archivos elegidos:

- X: un PNG y texto completo con conteo ponderado. Sin carrusel ni paquete general.
- Instagram: cinco PNG 1080 × 1350, enviados en un grupo silencioso.
- TikTok: cinco PNG 1080 × 1920, enviados en un grupo silencioso. No es un vídeo.

El mismo mensaje de borrador se actualiza para indicar cola, revisión final y
aprobación. La aprobación final no vuelve a enviar los archivos. Abrir X solo
precarga texto; el propietario adjunta el PNG. No hay publicación automática.
Texto completo, fuente y guion opcional se descargan únicamente al pedirlos.

Los botones antiguos de aprobación editorial abren la selección de red en el
mismo mensaje. Los trabajos y aprobaciones visuales anteriores se conservan.
Los comandos históricos de recuperación siguen siendo solicitudes explícitas.

## Integridad y límites

La selección y sus callbacks se vinculan al propietario, noticia, red y mensaje.
Una ilustración nueva se guarda por noticia y se reutiliza si se eligen varias
redes. Los trabajos nuevos no reutilizan los fondos antiguos posiblemente ajenos
al sujeto. Se conserva el presupuesto Cloudflare compartido de tres intentos/día
UTC, incluidos fallos y previas. Se mantienen seis llamadas editoriales/día.

Las cargas se reclaman antes de enviarlas. Una respuesta incierta bloquea el
reenvío automático y conserva los buffers. Un álbum confirmado registra todos
sus identificadores en un batch de D1. Las ediciones del mensaje de revisión
pueden repetirse sin volver a subir imágenes. La retención de fondos nuevos es
de treinta días desde la primera observación de todos los formatos entregados;
los trabajos activos, incompletos o inciertos impiden la retirada.

La migración `online/platform-schema.sql` solo añade tablas. Debe aplicarse antes
del despliegue. No cambia WaStake, Render, Supabase ni `sources/`.

## Correspondencia visual

La dirección política se reconoce separadamente del contexto financiero. El
caso Trump/Vance con crítica o enfrentamiento usa una composición VS con retratos
oficiales de dominio público. Las pruebas reales del generador inventaron rasgos
de Vance y, en un intento, aún añadieron una moneda pese a prohibirla: no se dieron
por válidas. La composición final conserva las identidades, acredita a Daniel
Torok / Wikimedia Commons y se etiqueta MONTAJE EDITORIAL. No gasta IA ni depende
de las cuotas. Véase `online/assets/PORTRAITS.md` para licencias y procedencia.
No es una fotografía del enfrentamiento ni una representación de una pelea real.

Bitcoin solo se prescribe si figura en el titular original, no por etiquetas de
mercado ni menciones secundarias del extracto. Las reglas de prompt no acreditan
por sí solas la calidad o correspondencia del resultado: se necesita revisión.

## Proveedores alternativos: investigación y límites

1. Cloudflare Workers AI sigue primero. La cuota propia no se amplía. Un fallo
   confirmado por cuota también permite considerar el respaldo; un error de
   contenido, una salida inválida o una respuesta incierta no lo activan.
2. AI Horde es comunitario y gratuito. Integración anónima con un máximo de dos
   solicitudes/día UTC, una imagen por solicitud, modelo
   `Flux.1-Schnell fp8 (Compact)`, 512 × 512 y cuatro pasos. Cola persistida,
   sondeo por minuto, espera máxima 45 minutos y sin repetir un envío incierto.
   Las condiciones publicadas restringen usos vinculados a cripto/blockchain;
   por prudencia se excluyen titulares/extractos cripto. Las generaciones
   anónimas y sus prompts se comparten con LAION: solo se envían datos públicos
   de la noticia y la dirección visual, nunca identificadores de Telegram ni
   secretos. La resolución y espera son inferiores al servicio principal.
3. No se habilita un tercer proveedor sin una vía gratuita verificada. La
   búsqueda devolvió una versión antigua de Hugging Face con $0,10/mes; al abrir
   su página vigente, los usuarios Free aparecen **sin créditos incluidos**.
   Por tanto se descarta esa cifra antigua y no se conecta su API. Pollinations
   requiere clave y Pollen; sus recompensas iniciales no acreditan una cuota
   recurrente suficiente. Gemini Imagen/Image no ofrece la API gratuita en la
   página de precios consultada. Puter traslada el coste al usuario.

Las noticias cripto quedan en espera del siguiente cupo UTC si Cloudflare se
agota. Un trabajo sin cupo se aplaza para permitir que otros avancen. No se
compran créditos, abren cuentas, cambian planes ni prometen imágenes ilimitadas.

Fuentes oficiales consultadas el 05/11/2026:

- https://developers.cloudflare.com/workers-ai/platform/pricing/
- https://aihorde.net/
- https://aihorde.net/api/swagger.json
- https://aihorde.net/api/v2/documents/terms?format=markdown
- https://raw.githubusercontent.com/Haidra-Org/AI-Horde-image-model-reference/main/stable_diffusion.json
- https://huggingface.co/docs/inference-providers/pricing
- https://gen.pollinations.ai/docs
- https://raw.githubusercontent.com/pollinations/pollinations/main/README.md
- https://ai.google.dev/gemini-api/docs/pricing?hl=en
- https://docs.puter.com/user-pays-model/

## Verificación

83 pruebas correctas a esta revisión: aislamiento por red, callbacks y dueño,
repeticiones, álbum, portada única, entrega incierta, recuperación de edición,
retención, cuotas, orden de proveedores y exclusión cripto de Horde. Las pruebas
usan SQLite real y servicios sustituidos; no acreditan entrega real de un álbum
en Telegram. Construcción Wrangler correcta. La prueba real de Horde completó
una imagen (modelo Flux.1-Schnell fp8 (Compact), 29.256 bytes) en aproximadamente
444 segundos entre solicitud y consulta final; ese intervalo incluye la espera
entre consultas y no es una medición exacta del tiempo de inferencia. Se detectó
la mala identidad de Vance y no se entregó esa prueba como material válido.

Despliegue final: `2a0d8c7f-8a3d-402b-bc53-ba89bd31d485`, 05/11/2026.
Migración aditiva remota completada. Administración autenticada responde 200;
se mantienen seis llamadas editoriales y tres intentos Cloudflare/día. Las dos
previas IA consumieron los dos intentos restantes de hoy (3/3), sin ampliar cuota.
La vista final con fotos oficiales se renderizó online al mismo 3/3, se descargó
y se inspeccionó: ambos rostros reales, sin moneda, texto legible y créditos.
No se cambiaron aprobaciones anteriores ni se enviaron pruebas a Telegram.
