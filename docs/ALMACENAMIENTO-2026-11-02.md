# WaCapital · Retención automática y protección de espacio

Se comprobó online el 02/11/2026 que la limpieza horaria está activa, con última
ejecución correcta a las 19:01 UTC. En la consulta inicial se midieron 9.154.560
bytes de base de datos y la generación no estaba pausada por espacio.

- Cachés de vistas previas: eliminación tras siete días.
- Fondo original de IA: retirada tras treinta días desde la entrega confirmada
  de los seis PNG, solo en trabajos terminados, aprobados o descartados.
- Registros antiguos sin reloj: plazo nuevo desde su primera observación por
  el limpiador, nunca desde una fecha antigua de publicación.
- Se conservan textos, fuentes, aprobaciones, manifiestos e identificadores de
  los originales enviados a Telegram. Las entregas pendientes, dudosas o activas
  conservan sus imágenes y buffers.
- La comprobación comparte el bloqueo del generador, procesa lotes acotados de
  diez y registra cuántos elementos retiró. No elimina el histórico de Supabase.
- Antes de generar se reserva un margen de 2,4 MB frente a un freno propio de
  100 MB. Si no puede medirse el tamaño, se pausa el trabajo nuevo. El espacio
  físico de SQLite puede no disminuir inmediatamente al liberar filas, por lo
  que este control es conservador; no se promete reanudación automática por purga.

Administración privada: `storage-status`, `storage-plan` (solo lectura) y
`storage-cleanup` mediante `online/scripts/admin.mjs`. Diez pruebas cubren
retención, tiempos inválidos, entregas incompletas y pausa de generación.
