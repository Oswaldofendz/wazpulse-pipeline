# WaCapital · Variedad editorial de imágenes

Las portadas anteriores imponían una caja fuerte y servidores a cualquier texto
que mencionara un hackeo, además de luz roja/azul y una única composición. Una
reapertura tras un incidente recibía así la misma escena que el robo inicial.

El estilo `impact-v3` distingue recuperación de servicio, interrupción o bug,
salida de fondos, seguridad, adquisiciones, política monetaria, resultados y
contexto de mercado. El titular tiene prioridad sobre los antecedentes del
extracto. Una reapertura negada o propuesta no se presenta como completada.
Las escenas son metáforas editoriales etiquetadas como IA, no fotografías de
personas o acontecimientos reales. No se añaden símbolos de Bitcoin a altcoins
o empresas que no lo mencionen.

Cada familia dispone de tres escenas; se alternan cuatro paletas y tres
composiciones: fotografía protagonista, titular sobre fotografía enmarcada y
composición asimétrica. Los dos últimos manifiestos con dirección visual sirven
para evitar repetir escena, paleta y composición; esto no garantiza la ausencia
de similitudes semánticas producidas por el modelo. No requiere otra llamada a
IA ni tablas nuevas. Las ilustraciones v1/v2 anteriores conservan sus archivos,
manifiestos, dimensiones y aprobaciones.

La dirección elegida se guarda en el manifiesto antes del renderizado. Reintentar
una entrega conserva la misma imagen y composición. Las vistas previas tienen
su propia clave de estilo y prompt; no alteran los paquetes entregados.

Se mantienen tres intentos de ilustración por día UTC, incluidos fallos y
vistas previas, y una ilustración reutilizada en una portada y cinco láminas.
No se cambia de plan ni se añade un proveedor de pago.

Validación local: 52 pruebas automáticas y 18 renderizados reales en Chrome
(tres composiciones por seis piezas), comprobación de desbordamiento y separación
del pie. Se comprobó que un texto extremo falla de forma explícita sin recortarlo
silenciosamente. Para maquetación se reutilizó un fondo anterior sin gastar IA;
esas muestras no acreditan la variedad del generador. La muestra online nueva se
verificó por separado después del despliegue: una ilustración nueva y su portada
privada, sin cambiar la aprobación del paquete anterior. Se usó el último intento
disponible del 02/11; no se amplió el presupuesto para generar más ejemplos.

Versión final desplegada: `60348160-1c3e-4b25-8101-a015fe8a419d`.
Comprobación a las 20:05 UTC: salud HTTP 200, administración sin autenticar 401,
generación activada, límite diario 3/3 y base de datos de 10.018.816 bytes, sin
pausa por almacenamiento. La muestra permitió añadir el caso «opens crypto
withdrawals» a las pruebas de reapertura. Esa corrección posterior se verificó
con pruebas; no se gastó otra ilustración para comprobarla visualmente.
