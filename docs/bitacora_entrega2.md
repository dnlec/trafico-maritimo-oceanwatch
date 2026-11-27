# Bitácora de la segunda entrega de OceanWatch

**Equipo 8 · Sistemas Intensivos de Datos · 2026-20.** La base de esta entrega es `166ac7f` de la rama `main`. La primera entrega usó siete días de junio de 2023. El flujo institucional admite la carga de esos días y la llegada incremental de archivos posteriores.

## Avances y decisiones

| Fecha de noviembre de 2026 | Avance documentado | Evidencia |
| --- | --- | --- |
| 15 | El equipo separó configuración, descarga, Bronze, Silver y Gold. | Commit `735be35` |
| 16 | El equipo movió la deduplicación a Silver y compartió la configuración y la lectura del World Port Index. | Commits `9e2c071` y `e4e6037` |
| 17 | El equipo documentó sus reglas de calidad e incorporó los grants iniciales. | Commits `a0e022a`, `e838ed5` y base `166ac7f` |
| 21 | Se contrastaron PDF, Word, versión del repo, retroalimentación de la primera entrega y sesiones prácticas. Se conservaron Bronze, su checkpoint y los tres productos de negocio. Se incorporaron el balance por batch y el manejo de llegadas tardías. No hay chequeo de tierra contra mar. | Notebooks 00 a 04 |
| 21 | Se incorporaron el contrato de oro, el modelo de anomalías registrado y el tablero institucional. Se integró ML como quinta tarea del job. | Notebook `05_ml`, `dashboards/`, contrato |
| 21 | Se procesó una muestra de tres ZIP oficiales y se evaluaron dos cortes temporales. Esos archivos locales no forman parte del repositorio. | Guía de ejecución y notebooks 00 a 05 |

**Decisión de calidad.** La observación de la primera entrega pedía justificar las columnas elegidas para identificar duplicados. El MMSI identifica al emisor y `BaseDateTime` identifica el instante de la señal. Agregar posición o velocidad a la llave dejaría pasar transmisiones contradictorias del mismo instante como observaciones independientes. Esto alteraría el conteo de tráfico y la distancia por buque. Se evalúan las copias antes de deduplicar. Una copia inválida va a cuarentena aunque otra del mismo instante sea válida. Dentro de un batch, `dropDuplicates` conserva una copia arbitraria por `(MMSI, BaseDateTime)`: no hay desempate garantizado si las copias válidas difieren en otras columnas. El `MERGE` solo inserta, así que entre batches y al repetir una ejecución prevalece la primera fila que llegó.

**Decisión espacial.** No hay chequeo de tierra contra mar. El dato NOAA incluye puertos, ríos y los Grandes Lagos, y un mapa de tierra grueso marcaría tráfico real como si estuviera en tierra. El campo `motivo` enumera las infracciones de una fila. Sumar los conteos por causa individual puede contar una fila más de una vez.

**Material del curso aplicado.** El inventario del Drive incluyó 88 archivos entre plan del curso, entregas, módulos virtuales, sesiones presenciales, quizzes y parciales. Se usaron las sesiones por contenido y modalidad, teniendo en cuenta el intercambio de las semanas 8 y 9. La práctica de la semana 9 orientó el uso de Structured Streaming y checkpoints. La semana 10 aportó los patrones de procesamiento incremental. La práctica 12 orientó dependencias y reintentos del job. La práctica 14 guio la comparación con una regla base, el seguimiento del experimento y el registro del modelo. Del Word del proyecto se tomó la referencia de Marine Cadastre y se contrastó el esquema de 17 columnas con los ZIP del equipo. [Documentación del conjunto AIS](https://hub.marinecadastre.gov/pages/vesseltraffic).

## Revisión frente al enunciado

| Punto y peso | Implementación | Comprobación efectuada y evidencia del workspace para la entrega |
| --- | --- | --- |
| 1. Ingesta en streaming · 15 % | 01 publica cada CSV completo. 02 usa Auto Loader y su checkpoint. 03 resuelve duplicados por emisor e instante. | El checkpoint de bronze lee cada archivo una vez. Silver escribe cuarentena, filas nuevas y métricas con un id de batch, así un reintento no duplica el append. |
| 2. Capas y calidad · 15 % | Bronze solo admite append. Silver aplica las reglas, conserva rechazos y balance. 04 construye trayectorias, presencia portuaria y tráfico. | Para una fecha, el conteo de silver se compara con la suma de `n_posiciones` en trayectorias y en tráfico. Las llaves de oro deben ser únicas. |
| 3. Orquestación · 20 % | El job se arma en la interfaz: cinco tareas, `fecha=AUTO`, dos reintentos y calendario diario a las 9:01 de Bogotá. | En **Runs**, conservar una ejecución `PERIODIC` exitosa con las cinco tareas. |
| 4. Consola · 15 % | Dashboard nativo AI/BI con filtro de día, mapa H3, ranking de puertos, buques observados y alertas. | Importar el JSON del tablero y publicarlo en el workspace. Conservar el enlace publicado con el día visible. |
| 5. Gobierno · 10 % | Esquema de consumo, grants por grupo, diccionario de oro y seguimiento de dependencias. | El contrato documenta tipos, llaves, unidades, nulos y reglas. En el workspace conservar los grants efectivos y el linaje visible en Catalog Explorer para las tablas del día. |
| 6. ML con MLflow · 10 % | Isolation Forest con tres características, historial cronológico, referencia P99, registro y lectura del modelo por versión en la tarea 05. | En la universidad guardar el enlace al experimento `/Shared/OceanWatch_eq8_velocidad` y la versión del modelo en Unity Catalog. |
| 7. Repositorio y bitácora · 5 % | README, contrato, bitácora, código, definiciones del tablero y pruebas reproducibles. | El ZIP se aplica sobre `166ac7f` e incluye el commit transportable. Para entregar al curso, la rama debe estar disponible en GitHub. |
| 8. Sustentación · 10 % | El recorrido de demostración está en esta bitácora y en el README. | La presentación oral corresponde a los tres integrantes en la última clase. |
## Resultados observados

Una llegada tardía a un día ya publicado vuelve a abrir esa partición al procesar el día del job. Al repetir la tarea, los conteos de las particiones se conservan. El checkpoint de bronze conserva un archivo ya leído y vuelve a leer el mismo contenido solo si llega con otro nombre. No hay etiquetas confirmadas de anomalías. La evidencia de la entrega es el experimento `/Shared/OceanWatch_eq8_velocidad` y el modelo en `default`.

## Sustentación del equipo

1. Una persona abre el historial del job, recorre el DAG y demuestra qué sucede al repetir un archivo. Otra reproduce el ensayo de caída y explica cómo se reconcilia el batch.
2. La tercera elige una fecha y un puerto, muestra los buques observados y abre una alerta. El equipo identifica las dos velocidades del caso y su versión de modelo.
3. Intercambiar los roles para explicar el contrato, el tablero y el modelo. Cada integrante debe poder recorrer todo el flujo. Conservar los enlaces y capturas obtenidos por el propio equipo para la entrega en Bloque Neón.
