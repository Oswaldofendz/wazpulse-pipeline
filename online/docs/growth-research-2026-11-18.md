# WaCapital: investigación y aplicación editorial

Revisión: 18 de noviembre de 2026.

## Qué explican las plataformas

**TikTok.** Su documentación describe recomendaciones personalizadas a partir de interacciones, información del contenido y contexto de la cuenta. Ver completo o saltar, compartir, comentar y el tiempo visto figuran entre las señales; para muchos usuarios, las interacciones pesan más que otras categorías. Las ponderaciones pueden variar. No publica una fórmula que convierta un diseño en viral. La documentación de vídeo no prueba una duración óptima universal para carruseles de fotos. [Fuente oficial](https://support.tiktok.com/en/using-tiktok/exploring-videos/how-tiktok-recommends-content).

**Instagram.** Meta describe varias fases para seleccionar y ordenar candidatos de Explore según su relevancia para cada persona. Su explicación general incluye la probabilidad de compartir entre varias predicciones; no ofrece un multiplicador fijo universal. Los documentos técnicos consultados son de 2023 y explican el funcionamiento general, no todos los detalles del sistema actual. [Explore](https://engineering.fb.com/2023/08/09/ml-applications/scaling-instagram-explore-recommendations-system/), [explicación de Meta](https://about.fb.com/news/2023/06/how-ai-ranks-content-on-facebook-and-instagram/).

**X.** La página de recomendaciones enumera intereses, red de contactos, temas, publicaciones con las que se interactúa y contenido popular en esa red. Explica que ninguna señal tiene siempre un peso superior fijo. Las tendencias reflejan conversaciones emergentes. Esto respalda claridad, relevancia temática y conversación útil; no demuestra que llenar el texto de etiquetas mejore el alcance. [Fuente oficial](https://help.x.com/en/rules-and-policies/recommendations).

## Decisiones de WaCapital derivadas de esa investigación

Son hipótesis editoriales para probar con las cuentas, no factores internos de las plataformas:

1. Portada con protagonista, hecho y una cifra principal cuando exista. Dos enfoques identificables: dato protagonista o contraste. La asignación permanece estable en los reintentos.
2. Una función distinta por lámina: movimiento, recorrido y comparación. Cuando falta un comparable, se muestran extremos observados o la referencia, según los datos disponibles.
3. Imágenes horizontales únicas para X; 4:5 para Instagram; 9:16 con zona central protegida para TikTok. Texto grande, contraste alto y comprobación de desbordamiento antes de entregar.
4. Gráficos originales hechos con cotizaciones guardadas. Ejes completos, horas, unidades y referencias explícitas. No se comparan activos con referencias diferentes ni cotizaciones antiguas. Una caída no permite inventar su causa.
5. Guiones en español con fragmentos de fuente numerados, nombres y cifras conservados. Pregunta específica al final; hasta dos etiquetas relacionadas. Revisión humana antes de publicar.
6. Una redacción válida se reutiliza entre Instagram y TikTok; los gráficos de mercado no consumen generación de imágenes ni llamadas de redacción. Los límites diarios se mantienen.

## Medición para mejorar con resultados reales

El auditor permite clasificar el gancho y registrar compartidos, guardados, exposición y alcance único, además de las métricas de consumo que ofrezca cada plataforma. Las tasas por visualizaciones y por alcance permanecen separadas. Los campos no disponibles se dejan vacíos.

Durante las próximas dos semanas: registrar cada publicación a las 24 horas y a los siete días; comparar dentro de la misma red, formato y ventana; alternar ganchos entre noticias comparables, sin duplicar el mismo post como supuesto experimento. Revisar mediana de exposición, compartidos por 1.000 y guardados por 1.000. Tres publicaciones por grupo son solo un mínimo práctico inicial, sin significación estadística garantizada. El auditor informa señales observadas y no declara causalidad ni predice viralidad.

## Corrección de la redacción

El identificador que mezclaba las variantes fp8 y fast de Llama 3.1 no aparece en el catálogo consultado. Se sustituyó por modelos documentados: Llama 3.3 70B FP8 Fast para guiones estructurados y Llama 3.2 3B para titulares. Se comprueba la fuente tras la generación y se conserva un estado de error reducido para diagnóstico. [Catálogo](https://developers.cloudflare.com/workers-ai/models/), [JSON estructurado](https://developers.cloudflare.com/workers-ai/features/json-mode/).
