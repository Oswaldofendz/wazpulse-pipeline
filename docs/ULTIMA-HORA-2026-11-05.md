# WaCapital · vía rápida de noticias y mercados

Actualización del 5 de noviembre de 2026. WaStake conserva su papel de agregador
y redactor. WaCapital incorpora observación directa en Cloudflare y no espera
a que Render termine la recopilación para avisar de un acontecimiento urgente.
No se modifica el código ni las fuentes sincronizadas del proyecto WaStake.

## El cuello de botella anterior

La recopilación normal corría cada diez minutos, analizaba una noticia por
ciclo y compartía un límite de seis llamadas editoriales al día UTC. Al iniciar
esta ampliación, las seis llamadas del 5 de noviembre ya estaban consumidas.
No había vigilancia independiente de movimientos de precios. Reducir solo el
intervalo de recopilación no resolvía esos tres problemas.

## Funcionamiento nuevo

| Entrada | Frecuencia | Acción inicial |
| --- | --- | --- |
| S&P 500, Nasdaq, Dow, BTC y ETH | Un minuto durante su sesión; cripto 24/7 | Aviso breve con datos, fecha y botones por red al superar un umbral |
| Euro Stoxx 50 y Nikkei | Un minuto durante su sesión local | Misma validación de frescura; se descartan datos demorados |
| Oro y WTI futuros | Dos minutos, con pausa de mantenimiento | Solo se aceptan datos válidos y recientes |
| VIX | Un minuto durante sesión estadounidense | Se interpreta una subida como riesgo |
| Fed, BCE, CNBC, CoinDesk, BBC World y BBC Mundo | Cada fuente, dos minutos; tres fuentes por minuto | Titular original y enlace inmediato; revisión española después |
| Redacción de noticias | Cada dos minutos, salvo mantenimiento o prioridad de un aviso | Una llamada por ciclo, con el mismo máximo diario de seis |
| Recopilación de WaStake | Diez minutos | Sigue complementando las entradas directas |
| Preparación de imágenes | Alterna con redacción, cuando no hay un aviso urgente | Se conservan el formato elegido, la aprobación y los límites anteriores |

Las ventanas de sesión respetan las zonas horarias y el cambio de hora. No
incluyen un calendario oficial completo de festivos: la validación de fecha
de la cotización impide avisos con precios congelados cuando un mercado cierra.

Las fuentes de noticias se consultan mediante sus RSS públicos. Se admiten
decisiones monetarias, publicaciones de inflación/empleo/PIB, emergencias
financieras, movimientos fuertes de mercado, incidentes de seguridad,
interrupciones geopolíticas relevantes y resultados empresariales importantes.
Los filtros descartan opinión y previsiones explícitas; no garantizan comprender
cada titular. La aprobación editorial sigue siendo humana.

El primer contacto con cada RSS establece una base y no envía sus noticias
anteriores. Después solo se consideran entradas nuevas de hasta veinte minutos.
Cada fuente entrega como máximo una novedad por revisión; las restantes se
conservan para la siguiente. Un tema repetido se silencia veinte minutos.

## Telegram y publicación

- Una alerta numérica no necesita IA editorial. Incluye cifras observadas y
  permite elegir X, Instagram o TikTok mediante los botones existentes.
- Una alerta de fuente llega primero con su titular original y enlace. Cuando
  hay cupo de redacción se edita **ese mismo mensaje** con la revisión española
  y los botones. Si no hay cupo se indica claramente que está pendiente.
- Aprobar para X permite preparar una sola portada. Los gráficos usan el
  corte de datos almacenado en esa noticia, muestran fuente y hora UTC y no
  llaman a ningún generador de imágenes. Las bajadas usan la composición roja.
- Instagram y TikTok conservan sus cinco láminas y su entrega agrupada.
- Ningún aviso aprueba ni publica por el propietario. Se mantienen los enlaces
  para compartir el texto y la portada aprobados y las alternativas manuales.
- Una respuesta perdida de Telegram se registra como incierta. No se vuelve
  a enviar automáticamente un mensaje o archivo cuya entrega es ambigua.
- `/rapido` muestra actividad, último ciclo, frescura, fuentes y avisos usados.

## Umbrales y calidad de los datos

| Activo | Movimiento en unos 5 min | En unos 15 min | Nivel frente a cierre/referencia |
| --- | --- | --- | --- |
| S&P 500, Dow y Euro Stoxx | 0,8% | 1,5% | 3% |
| Nasdaq | 1% | 1,8% | 4% |
| Nikkei | 1% | 1,8% | 4% |
| BTC | 2% | 3% | 5% |
| ETH | 2% | 3% | 7% |
| Oro | 1% | 1,5% | 3% |
| WTI | 2% | 3% | 5% |
| VIX | 12% | 18% | 25% |

Se requieren dos observaciones recientes consecutivas. Las ventanas admiten
hasta dos minutos adicionales y el texto informa los minutos realmente
transcurridos. La detección de un nivel diario ya alcanzado se identifica como
tal: no afirma que la caída haya comenzado en ese minuto ni inventa su causa.

Yahoo Finance es la fuente inicial de cotizaciones. Se rechazan símbolos
incorrectos, valores no positivos, fechas futuras, precios de más de tres
minutos y demoras declaradas superiores a dos minutos. BTC y ETH pueden usar
Coinbase si falla esa entrada, con ticker reciente y referencia móvil de 24 h
actualizada cada cinco minutos. Se marca esa base distinta en texto y gráfico;
al cambiar de proveedor o base se reinicia el historial para evitar saltos falsos.

Antes de avisar sobre cripto se contrasta el precio con otra fuente: Coinbase
para la entrada de Yahoo y Kraken para la de Coinbase. Se exigen frescura,
una diferencia máxima del 1,5% y controles de caché para Kraken. No se promedia
ni se fuerza un aviso si el contraste falla.

Una familia de mercado y dirección tiene una pausa de 45 minutos. Puede
escalar después de diez minutos si el mismo activo empeora o intensifica
su variación diaria al menos dos puntos porcentuales. También se conserva
un nivel por símbolo para no repetir el mismo umbral diario persistentemente.
Una tarjeta que ya describe una caída conjunta de bolsa y cripto conserva
además los precios de los activos cubiertos: no vuelve a avisar por el mismo
movimiento en la otra familia. Sí admite un choque adicional que supere el
umbral rápido o una escalada diaria de dos puntos después de diez minutos.

## Coste y límites

- Continúa Cloudflare Free; no se contratan noticias, API de X ni IA de pago.
- Doce intentos de aviso rápido al día UTC, compartidos. Los avisos de precio
  utilizan como máximo ocho de esos intentos, dejando cuatro para noticias.
  Es un límite deliberado contra ráfagas, no una promesa de alertas ilimitadas.
- Seis llamadas al redactor al día UTC, contando fallos. Las noticias ordinarias
  pueden consumir hasta cuatro; las de prioridad 95 o superior pueden usar
  las dos restantes. No se reinicia un presupuesto que ya esté agotado.
- Máximo de 12.000 intentos HTTP externos por día para el monitor, contando
  errores, respaldos y contrastes. Las reservas simultáneas se agrupan en una
  actualización atómica; no se omiten intentos de las cuentas.
- Se respeta `Retry-After` en 429 y se espacia progresivamente el resto de los
  fallos. Los RSS usan `ETag`/`Last-Modified` cuando la fuente los ofrece.
- Las consultas se agrupan y se alterna redacción e imágenes para respetar
  el máximo de 50 llamadas D1 por ejecución de Workers Free. La documentación
  oficial confirma que una llamada `batch()` cuenta como una sola frente
  a ese máximo. Las escrituras individuales siguen contando para la cuota diaria.
- Se conservan hasta dos horas/125 muestras de precios por activo. Las URLs
  vistas vencen a los siete días y la limpieza elimina lotes limitados.
- Los límites de imágenes anteriores permanecen; los gráficos numéricos
  no gastan ese cupo. Los límites de filas, almacenamiento, navegador y
  las protecciones de entrega anteriores siguen aplicándose.

## Verificación

Las 116 pruebas pasan. Cubren choque de precios sin Render/IA, hora de mercado, cambio
de hora, fuentes caídas, datos viejos, 429, límites duros, concurrencia,
deduplicación, reservas editoriales, actualización del mismo mensaje,
ráfagas de titulares y respeto de aprobación.

Medición de llamadas D1 en una ejecución de prueba:

- Mercado y tres anuncios de fuentes distintos: **45/50**.
- Respaldo cripto, contraste y tres anuncios simultáneos: **47/50**.
- Monitor, incorporación de ocho artículos y redacción: **25/50**.
- Monitor y entrega de una portada X aprobada: **24/50**.

Se renderizaron once formatos localmente sin desbordamiento: una portada X,
cinco láminas Instagram y cinco TikTok. Cero llamadas a IA de imágenes.
Se usaron datos guardados solo para esa verificación visual, no como una
noticia nueva para enviar al propietario.

La observación en línea del 5 de noviembre confirmó las seis fuentes RSS,
cotizaciones recientes de los tres índices estadounidenses y BTC/ETH, y
coincidencia entre Coinbase y Kraken. Oro, WTI y VIX devolvían datos demorados
o inválidos y quedaron excluidos de las alertas. Europa y Japón estaban
cerrados: no se afirma cobertura en vivo comprobada para esos mercados.

Activación verificada: los primeros avisos automáticos reales llegaron a
Telegram a las **17:17:15 y 17:18:14 UTC**, con confirmación de entrega. Se
produjeron cinco y cuatro segundos después de la hora de sus respectivas
cotizaciones, no del comienzo de la caída. No hubo llamadas editoriales ni
de imagen adicionales: los contadores siguieron en seis y tres. Tras observar
esa pareja se añadió y probó la supresión de avisos sobre otros activos ya
cubiertos por la misma caída conjunta. No se enviaron titulares ficticios,
no se aprobaron materiales y no se publicó en redes durante la validación.

## Alcance de la rapidez

Objetivo operativo: revisar precios cada minuto y titulares cada dos minutos,
sin esperar a la IA para el primer aviso. El tiempo desde el acontecimiento
real incluye la demora del proveedor y su publicación en el RSS. Las entradas
públicas no ofrecen una garantía de latencia ni disponibilidad; 100% de
cobertura y entrega instantánea permanente no se puede garantizar con este
presupuesto. Una fuente lenta no se convierte en tiempo real por consultarla
más veces. La ruta de precios permite detectar movimientos antes de que un
medio publique una explicación.

## Operación y recuperación

Aplicar `online/fast-schema.sql` después de los esquemas existentes. La
migración es aditiva. `FAST_ENABLED` activa la observación;
`FAST_ALERTS_ENABLED` activa los avisos. Las plantillas públicas mantienen
ambos apagados; la configuración privada controla el servicio real.

`node scripts/admin.mjs fast-status` muestra último ciclo, antigüedad,
presupuestos, estado HTTP y los cinco últimos eventos con entrega y latencia.
`fast-probe` comprueba Coinbase/Kraken consumiendo únicamente el presupuesto
HTTP; no avisa. `fast-tick` ejecuta un ciclo real y puede avisar si está
activado: no debe usarse para inventar una noticia de prueba.

No se imprimen claves y las rutas administrativas siguen autenticadas. Para
volver al comportamiento anterior basta desactivar `FAST_ENABLED` en la
configuración privada y desplegar; no hace falta borrar noticias ni presupuestos.

Referencias de límites: [Cloudflare D1](https://developers.cloudflare.com/d1/platform/limits/)
y [Coinbase Exchange](https://docs.cdp.coinbase.com/exchange/rest-api/rate-limits).

Fuentes directas: [Fed](https://www.federalreserve.gov/feeds/press_monetary.xml),
[BCE](https://www.ecb.europa.eu/rss/press.html),
[CNBC](https://www.cnbc.com/id/100003114/device/rss/rss.html),
[CoinDesk](https://www.coindesk.com/arc/outboundfeeds/rss?outputType=xml),
[BBC World](https://feeds.bbci.co.uk/news/world/rss.xml) y
[BBC Mundo](https://feeds.bbci.co.uk/mundo/rss.xml).
