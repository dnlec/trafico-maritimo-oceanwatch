# Bitácora de la segunda entrega de OceanWatch

**Equipo 8 · Sistemas Intensivos de Datos · 2026-20.** La base de esta entrega es `166ac7f` de la rama `main`. La primera entrega usó siete días de junio de 2023. El flujo institucional admite la carga de esos días y la llegada incremental de archivos posteriores.

## Avances y decisiones

| Fecha de noviembre de 2026 | Avance documentado | Evidencia |
| --- | --- | --- |
| 15 | El equipo separó configuración, descarga, Bronze, Silver y Gold. | Commit `735be35` |
| 16 | El equipo movió la deduplicación a Silver y compartió la configuración y la lectura del World Port Index. | Commits `9e2c071` y `e4e6037` |
| 17 | El equipo documentó sus reglas de calidad e incorporó los grants iniciales. | Commits `a0e022a`, `e838ed5` y base `166ac7f` |
| 21 | Se contrastaron PDF, Word, versión del repo, retroalimentación de la primera entrega y sesiones prácticas. Se conservaron Bronze, su checkpoint y los tres productos de negocio. Se incorporaron validación hidrográfica, balance por microbatch, recuperación y manejo de llegadas tardías. | Notebooks 00 a 04, `reference/fuentes.md` y prueba de recuperación |
| 21 | Se incorporaron el esquema y contrato de oro, el modelo de anomalías registrado, el tablero institucional y su consumidor en Free Edition. Se integró ML como quinta tarea del job. | Notebook 05, `free/06_consumo_free.ipynb`, `dashboards/`, contrato |
| 21 | Se procesó una muestra de tres ZIP oficiales, se evaluaron dos cortes temporales y se reprodujeron los resultados desde un directorio vacío. Se verificaron las consultas de la consola. | `qa/reproducir_muestra.py`, `qa/salidas_muestra/`, guía y vista HTML |

**Decisión de calidad.** La observación de la primera entrega pedía justificar las columnas elegidas para identificar duplicados. El MMSI identifica al emisor y `BaseDateTime` identifica el instante de la señal. Agregar posición o velocidad a la llave dejaría pasar transmisiones contradictorias del mismo instante como observaciones independientes. Esto alteraría el conteo de tráfico y la distancia por buque. Se evalúan las copias antes de deduplicar. Una copia inválida va a cuarentena aunque otra del mismo instante sea válida. En un lote se usa un desempate estable y entre lotes prevalece la primera copia válida confirmada.

**Decisión espacial.** La regla original comprobaba los rangos, pero dejaba pendiente el requisito de agua. La primera prueba con cartografía general marcó muchas observaciones costeras y fluviales. Se añadió la línea de costa TIGER/Line de 2023 y la red navegable de USACE antes de fijar la versión de la máscara. En la muestra final, 220.263 mensajes recibieron el motivo `posicion_en_tierra`, a veces junto con otro motivo. Corresponde al 26,8 % de la muestra. Las coordenadas rechazadas siguen disponibles para revisar orillas o cauces donde la cartografía de referencia sea insuficiente. El campo `motivo` enumera las infracciones de una fila. Sumar los conteos por causa individual puede contar una fila más de una vez.

**Material del curso aplicado.** El inventario del Drive incluyó 88 archivos entre plan del curso, entregas, módulos virtuales, sesiones presenciales, quizzes y parciales. Se usaron las sesiones por contenido y modalidad, teniendo en cuenta el intercambio de las semanas 8 y 9. La práctica de la semana 9 orientó el uso de Structured Streaming y checkpoints. La semana 10 aportó los patrones de procesamiento incremental. La práctica 12 orientó dependencias y reintentos del job. La práctica 14 guio la comparación con una regla base, el seguimiento del experimento y el registro del modelo. Del Word del proyecto se tomó la referencia de Marine Cadastre y se contrastó el esquema de 17 columnas con los ZIP del equipo. [Documentación del conjunto AIS](https://hub.marinecadastre.gov/pages/vesseltraffic).

## Revisión frente al enunciado

| Punto y peso | Implementación | Comprobación efectuada y evidencia del workspace para la entrega |
| --- | --- | --- |
| 1. Ingesta en streaming · 15 % | 01 publica cada CSV completo. 02 usa Auto Loader y su checkpoint. 03 resuelve duplicados por emisor e instante. | En Spark/Delta se forzó una caída después de confirmar el lote de Silver y se reinició el mismo checkpoint. En Databricks, `prueba_bronze=si` permite mostrar el archivo repetido y su reenvío con nombre nuevo. |
| 2. Capas y calidad · 15 % | Bronze solo admite append. Silver aplica las reglas, conserva rechazos y balance. 04 construye trayectorias, presencia portuaria y tráfico. | Se conciliaron los tres días. El total de posiciones en Silver coincide con las sumas de los productos de trayectoria y tráfico. Se validó la unicidad de las cuatro llaves de oro. |
| 3. Orquestación · 20 % | `configurar_job` configura cinco tareas en el job del equipo, parámetro de fecha, dos reintentos y calendario diario. | El modo `verificar` consulta Jobs y comprueba si los runs recientes incluyen un `PERIODIC` exitoso. Guardar ese run institucional, sus tareas y el JSON del control. |
| 4. Consola · 15 % | Dashboard nativo AI/BI con filtro de día, mapa H3, ranking de puertos, buques observados y alertas. | Se ejecutaron los cinco datasets sobre oro local. La vista HTML muestra la muestra del 9 de junio. El notebook 05 verifica las consultas en el warehouse elegido y devuelve el enlace tras publicar el tablero institucional. |
| 5. Gobierno · 10 % | Esquema de consumo, grants por grupo, diccionario de oro y seguimiento de dependencias. | El contrato documenta tipos, llaves, unidades, nulos y reglas. En el workspace conservar los grants efectivos y el linaje visible en Catalog Explorer para las tablas del día. |
| 6. ML con MLflow · 10 % | Isolation Forest con tres características, historial cronológico, referencia P99, registro y lectura del modelo por versión en la tarea 05. | El registro local generó dos versiones. Repetir el flujo sin novedades actualizó cero fechas. En la universidad guardar el enlace al experimento y la versión en Unity Catalog. |
| 7. Repositorio y bitácora · 5 % | README, contrato, bitácora, código, definiciones del tablero y pruebas reproducibles. | El ZIP se aplica sobre `166ac7f` e incluye el commit transportable. Para entregar al curso, la rama debe estar disponible en GitHub. |
| 8. Sustentación · 10 % | Guía de cuatro páginas con orden de demostración, resultados de ejemplo y puntos técnicos para el grupo. | La presentación oral corresponde a los tres integrantes en la última clase. |
| Bono · +5 % | Share Open Sharing con destinatario de token, catálogo externo de solo lectura, comparación de cuatro tablas y dashboard de Free. | Para acreditarlo, conservar el tablero publicado con el warehouse de Free y las cuatro firmas coincidentes entre dos hosts distintos en la misma fecha. |

## Resultados observados

Spark local procesó una muestra determinista por buque sobre los archivos nacionales completos del 7 al 9 de junio. Se seleccionaron los MMSI cuyo primer byte de MD5 era inferior a 8, conservando todos sus mensajes. Los tres ZIP contenían 26.623.294 filas en total. En la muestra, 507.084 posiciones pasaron a Silver, 313.965 filas llegaron a cuarentena y 15 fueron duplicados válidos. La suma es 821.064. Los totales de esta prueba describen la muestra. La ejecución del equipo procesará el universo de cada archivo.

El 9 de junio, 460 de 479 buques tenían trayectoria suficiente. El detector señaló 11, la regla base señaló 7 y los siete coincidieron con el detector. La tasa de alertas del modelo entre los evaluados fue 2,39 %. El 7 fue el arranque sin historia previa de la prueba. No hay etiquetas confirmadas que permitan calcular precisión o exhaustividad. Se conservan los identificadores de los runs locales y el artefacto MLflow de la versión 2 en `qa/modelo_muestra_v2/`, con su firma y ejemplo de entrada.

En un ensayo adicional de Spark y Delta, dos días partieron de una posición cada uno. Llegó una segunda posición al día anterior. Al procesar el día del job se reabrió esa partición y quedó en dos posiciones, mientras la otra quedó en una. Al repetir la tarea ambas conservaron sus conteos. El lector local de archivos de Structured Streaming también conservó 1, 1 y 2 filas al procesar un archivo inicial, repetir su checkpoint y reenviar su contenido con otro nombre. El ensayo nativo de Auto Loader está disponible en 02. Los valores medidos están en `qa/salidas_muestra/llegada_tardia.json`.

## Sustentación del equipo

1. Una persona abre el historial del job, recorre el DAG y demuestra qué sucede al repetir un archivo. Otra reproduce el ensayo de caída y explica cómo se reconcilia el microbatch.
2. La tercera elige una fecha y un puerto, muestra los buques observados y abre una alerta. El equipo identifica las dos velocidades del caso y su versión de modelo.
3. Intercambiar los roles para explicar el contrato y comprobar las huellas en Free. Cada integrante debe poder recorrer todo el flujo. Conservar los enlaces y capturas obtenidos por el propio equipo para la entrega en Bloque Neón.
