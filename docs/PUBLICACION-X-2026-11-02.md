# WaCapital · Titular, texto completo y portada de X

La captura del propietario mostraba una frase cortada en «los…». El código
anterior reservaba espacio para la fuente y recortaba el texto a una posición
de Unicode, incluso a mitad de una idea. El botón era un Web Intent de texto
y enlace: no subía ninguna imagen a X. La imagen visible no acredita el envío
de la portada y es compatible con una tarjeta del enlace externo.

La nueva política conserva el texto aprobado completo si cabe en 280 caracteres
ponderados, contando emoji mediante twitter-text. Si no cabe, selecciona el
titular completo y unidades de texto aprobadas que quepan; no añade puntos
suspensivos ni corta palabras para llenar el espacio. Si no hay una unidad
admisible, conserva el original para editar y no ofrece botón de X. Esta
heurística no sustituye la revisión de redacción o de los hechos.

El borrador X ya no contiene enlaces de la fuente. La atribución se conserva
en el paquete privado de Telegram, en los textos extensos y en la portada.
Para la noticia de la captura, el texto completo original cabe en 277/280
caracteres ponderados al retirar ese enlace; no se necesitó otra llamada de IA.

Al aprobar las imágenes, el bot reenvía el PNG de posición 0 del mismo story_id
por su file_id confirmado. Su título y referencia acompañan al mensaje del
borrador. No busca «la última imagen», ni sustituye el archivo por una tarjeta
del medio. Si falta la portada confirmada, no ofrece este conjunto.

El botón se llama «Abrir X · adjuntar la portada anterior». El propietario debe
guardar ese PNG, adjuntarlo con el selector de fotos de X y confirmar el post.
No se integra una API de pago ni se sube la imagen de forma automática. El
comando privado `/publicar ID` recupera texto actualizado y portada de un trabajo
visualmente aprobado, incluidos paquetes anteriores, sin regenerar IA ni
reescribir sus manifiestos o aprobaciones. En caso de respuesta de envío dudosa,
el propietario puede pedirlo de nuevo explícitamente; no se reintenta a ciegas.

Los nuevos subtítulos se contrastan con el titular mediante términos normalizados.
Si repiten sustancialmente la misma idea, se busca una frase complementaria en
el ángulo o razonamiento ya aprobados. Si tampoco existe, se omite el subtítulo.
Los originales ya entregados mantienen su versión; las correcciones de tipografía
de vistas previas reutilizan el fondo en caché sin gastar otra ilustración.

Validación: 59 pruebas aprobadas; seis renderizados del titular/subtítulo real
en las tres composiciones, sin desbordamiento. Versión desplegada:
`6930e6a9-3127-48d8-8c1b-1c7d0694d1d0`. El envío privado del conjunto actualizado
para la referencia `e43243d6` devolvió `prepared: true`: Telegram confirmó tanto
el documento original como el mensaje nuevo. No se publicó nada en X ni se
gastó otra ilustración. Los botones de mensajes antiguos siguen apuntando a
sus textos originales; se debe usar el mensaje nuevo o `/publicar ID`.
