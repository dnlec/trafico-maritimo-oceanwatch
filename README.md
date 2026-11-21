# OceanWatch AIS — Análisis de tráfico marítimo (Entrega 1)

Análisis de big data sobre tráfico marítimo a partir de datos **AIS** (Automatic Identification System)
de la NOAA, procesados con **PySpark en Databricks** (Unity Catalog + Delta Lake). Corresponde a la
Parte 1 del proyecto final.

**Entrega 2.** El trabajo continúa en el mismo repositorio. La segunda entrega procesa un archivo AIS por día en el workspace institucional y publica cuatro productos de oro. Para ejecutarla, ve a [Parte 2: pipeline](#parte-2-pipeline). Allí están el [tablero](#punto-4-consola-de-visualización), la [gobernanza](#punto-5-gobernanza), el [modelo de velocidad](#punto-6-anomalías-de-velocidad-con-mlflow) y el [bono en Free Edition](#bono-delta-sharing-en-free-edition).

## Datos

- **AIS (NOAA)**: 7 días de posiciones, del **2023-06-01 al 2023-06-07** — **60,533,559** registros de
**31,871** buques únicos. Descargados de `coast.noaa.gov` (AISDataHandler 2023).
- **World Port Index (WPI)**: catálogo de puertos (`UpdatedPub150.csv`) para cruzar las zonas de mayor
tráfico con puertos conocidos.

## Organización en Unity Catalog

| Nivel                  | Nombre                                                                                        |
| ---------------------- | --------------------------------------------------------------------------------------------- |
| Catálogo               | `proyecto_datos`                                                                              |
| Esquema                | `default`                                                                                     |
| Tabla (Delta, managed) | `ais_positions_delta` — particionada por `date`, optimizada con `ZORDER (h3_cell)`            |
| Volumen                | `data` — CSV crudos (`ais_data/`), copia Parquet (`ais_parquet/`) y WPI (`UpdatedPub150.csv`) |

La tabla lleva **comentarios** (catálogo, esquema, tabla y cada columna) y **metadatos**
(`TBLPROPERTIES`: fuente, período, capa, frecuencia, responsable, clasificación) como capa de gobernanza.

## Estructura del repositorio

```text
.
├── src/
│   ├── proyecto_ocean_watch.ipynb              # Parte 1 (Requisitos 1–5)
│   ├── _config.ipynb                           # Parte 2. Configuración compartida y fecha AIS
│   ├── 00_setup.ipynb                          # Volumen, tablas, WPI, máscara de agua y permisos
│   ├── 01_ingesta.ipynb                        # ZIP diario de NOAA → landing
│   ├── 02_bronze.ipynb                         # Auto Loader → Bronze
│   ├── 03_silver.ipynb                         # Calidad → Silver, cuarentena y métricas
│   ├── 04_gold.ipynb                           # Trayectorias, presencia en puertos y tráfico
│   └── 05_oceanwatch.ipynb                     # ML, gobierno, tablero, job, verificación y Delta Sharing
├── dashboards/                                 # Definiciones de los tableros AI/BI de la Entrega 2
│   ├── OceanWatch_Institucional.lvdash.json
│   └── OceanWatch_Free.lvdash.json
├── free/                                       # Bono. Notebook que se ejecuta en el workspace receptor
│   └── 06_consumo_free.ipynb
├── docs/                                       # Enunciados, diccionario, contrato de oro, guía y bitácora
│   ├── Proyecto_Final_2026-20_OceanWatch_AIS - Parte 1.pdf
│   ├── project-instructions-part-1.md
│   ├── data-dictionary.md
│   ├── data-dictionary.pdf
│   ├── Proyecto_Final_2026-20_OceanWatch_AIS - Parte 2.pdf
│   ├── Proyecto_Final_2026-20_OceanWatch_AIS_Parte_2_1.docx
│   ├── Guia_OceanWatch.pdf
│   ├── contrato_oro.md
│   └── bitacora_entrega2.md
├── exploration/                                # Resultados exportados de la exploración de la Entrega 1
│   ├── daily_summary.csv
│   ├── quality_diagnostics.csv
│   ├── size_metrics.csv
│   └── vessel_type_dist.csv
├── reference/                                  # Cartografía y procedencia de la regla de agua de Silver
│   ├── agua_ne_v1.csv.gz
│   ├── construir_mascara.py
│   ├── descargar_hidrografia.py
│   └── fuentes.md
├── qa/                                         # Pruebas locales y resultados de la muestra
│   ├── Vista_OceanWatch.html
│   ├── reproducir_muestra.py
│   ├── requirements-local.txt
│   ├── salidas_muestra/                        # Resultados tabulares y controles reproducibles
│   │   ├── anomalias.csv
│   │   ├── balance.csv
│   │   ├── cobertura_ml.csv
│   │   ├── conteos_fuente.json
│   │   ├── indicadores.csv
│   │   ├── llegada_tardia.json
│   │   ├── mapa.csv
│   │   ├── motivos.csv
│   │   ├── presencia.csv
│   │   ├── ranking.csv
│   │   ├── recuperacion_delta.json
│   │   └── runs_mlflow_local.csv
│   └── modelo_muestra_v2/                      # Modelo MLflow de la muestra local
│       ├── conda.yaml
│       ├── contrato_entrada.json
│       ├── input_example.json
│       ├── MLmodel
│       ├── model.pkl
│       ├── python_env.yaml
│       ├── registered_model_meta
│       ├── requirements.txt
│       └── serving_input_example.json
├── integracion/                                # Respaldo Git, guía del paquete de cambios y huellas
│   ├── Guia_del_ZIP_de_cambios.pdf
│   ├── OceanWatch_Equipo8.bundle
│   └── SHA256SUMS.txt
├── UpdatedPub150.csv                           # World Port Index
├── Guía de Ejecución en DataBricks.docx        # Guía paso a paso para ejecutar en Databricks
├── .gitignore
└── README.md
```

Los datos AIS crudos no se versionan en el repositorio (son ~6 GB); se descargan desde el Requisito 1
del notebook directamente al Volume de Databricks.

## Contenido del notebook

El notebook `src/proyecto_ocean_watch.ipynb` está organizado por requisito, con celdas de markdown
que explican y justifican cada paso:

- **Requisito 1 Ingesta.** Descarga con reintentos y verificación de integridad de los 7 ZIP, extracción  
al Volume y lectura como un único DataFrame con esquema explícito (~60.5 M filas).
- **Requisito 2 Exploración y perfilamiento.** En 7 pasos: columnas derivadas, volumen y actividad  
diaria, perfilamiento por tipo de buque y dimensiones, diagnóstico de calidad (reglas de validación),  
auditoría de nulos por columna, consistencia de atributos estáticos por MMSI y anomalías adicionales  
(posiciones imposibles en el tiempo, `VesselType` fuera de catálogo, `MMSI = 0`). Cierra con hallazgos
y conclusión orientados a la limpieza de la Entrega 2.
- **Requisito 3 Preguntas de negocio.** (a) buques distintos por día (exacto vs `approx_count_distinct`),
(b) tipos de buque con más tráfico y velocidad media, (c) top 10 de distancia recorrida (haversine con
ventana por MMSI), (d) concentración de tráfico en grilla **H3 res. 8** y cruce con el WPI, (e)
proporción de buques activos los 7 días y ubicación de los "visitantes de un solo día". Las decisiones
técnicas se justifican con el plan de ejecución real (`explain("formatted")`).
- **Requisito 4 Almacenamiento óptimo para un propósito.** Propósito de consulta (día + sector `h3_cell`)
y decisiones de formato/tabla/layout: **CSV vs Parquet vs Delta**, partición por `date`, `OPTIMIZE` +
`ZORDER (h3_cell)`. Evidencia medida: bytes (6.04 GB → 2.01 GB → 1.48 GB), archivos leídos por la
consulta (Parquet 6 vs Delta 1) y efecto de `OPTIMIZE` a esta escala, con tabla resumen y conclusión.
- **Requisito 5 Gobernanza.** Comentarios y metadatos aplicados sobre los activos en Unity Catalog  
(aplicados directamente en Databricks). El notebook incluye una celda de verificación (`DESCRIBE EXTENDED`)
que deja evidencia del comentario y las `TBLPROPERTIES` de la tabla.

## Cómo ejecutarlo

Abre `src/proyecto_ocean_watch.ipynb` y ejecuta las celdas con la extensión de Databricks.
La primera celda de código abre la sesión y apunta a `proyecto_datos.default`, donde ya están
la tabla `ais_positions_delta` y el volume `data`. La salida de cada celda queda guardada en el
archivo, así que se puede leer sin volver a ejecutar.

Los requisitos 2–4 dependen del DataFrame `df_ais` de la celda de lectura. La celda de descarga no
vuelve a bajar los ZIP si los 7 CSV siguen en el volume.

## Stack

PySpark - Delta Lake - Unity Catalog - Databricks - H3 (funciones geoespaciales nativas) - Photon.

## Parte 2: pipeline

Notebooks en `src/`, para el editor web de Databricks en el cluster shared
(acceso standard). Los archivos se escriben con Python sobre `/Volumes`.
`_config.ipynb` tiene el catálogo, las rutas, los nombres de tabla y el widget `fecha`.
Los demás lo incluyen con `%run ./_config`. No es una tarea del job.

| Notebook | Qué hace | Cuándo |
|---|---|---|
| `_config.ipynb` | Constantes y widget `fecha` | Lo incluye cada notebook |
| `00_setup.ipynb` | Volume, carpetas, tablas, World Port Index y `GRANT` a `mine-group8` | Una vez, a mano |
| `01_ingesta.ipynb` | Zip NOAA del día `fecha` → `landing/` | Tarea del job |
| `02_bronze.ipynb` | Auto Loader hace append del CSV crudo. El checkpoint lee cada archivo una vez | Tarea del job |
| `03_silver.ipynb` | Quita duplicados `(MMSI, BaseDateTime)` y separa válidas de cuarentena | Tarea del job |
| `04_gold.ipynb` | Trayectorias, presencia en puerto y tráfico celda × hora | Tarea del job |
| `05_oceanwatch.ipynb` | Calcula anomalías de velocidad con MLflow. Otros modos publican el tablero, aplican gobierno, configuran o verifican el job y comparten oro | Quinta tarea en `pipeline`. Los otros modos se eligen a mano |

`00_setup` copia `UpdatedPub150.csv` desde la raíz del repo (carpeta Git) a `ref/`
del volume y lo lee desde ahí. El número de puerto del CSV viene como `7950.0`.

Catálogo `dbr_acad_ingsistemas_mine`, esquema `default`, volume `oceanwatch_data`.
El día entra por el widget `fecha` (`YYYY-MM-DD`).

Bronze conserva cada fila, no deduplica. Silver comprueba cada mensaje antes de elegir una copia válida por `(MMSI, BaseDateTime)`. Así, si llegan dos copias del mismo buque e instante y una trae coordenadas inválidas, el equipo puede consultar la causa de ese rechazo en cuarentena. Silver ordena las copias válidas dentro de cada lote y usa `MERGE` sobre las fechas afectadas. Entre lotes conserva la primera copia válida confirmada. Cada lote deja su balance en `oceanwatch_metricas_silver_v2`.

En la Entrega 2, **`dbr_acad_ingsistemas_mine.default`** contiene el volumen, Bronze, Silver, cuarentena, referencias y métricas. El esquema **`dbr_acad_ingsistemas_mine.oceanwatch_oro_eq8`** contiene las cuatro tablas de consumo. `04_gold` produce las tres primeras. `05_oceanwatch`, en `modo=pipeline`, usa trayectorias para producir la cuarta. El número `04` en el nombre de un notebook indica su lugar en la secuencia. El **punto 4 del enunciado** es el tablero que se publica desde `05_oceanwatch`.

**De dónde salen los datos al ejecutar el job.** `01_ingesta` solicita a NOAA el ZIP real de la fecha indicada. Para el 9 de junio de 2023 busca `AIS_2023_06_09.zip`. Si ya existe una copia íntegra en `raw_zips/` del volumen, la reutiliza. El CSV se publica completo en `landing/` y `02_bronze` procesa los archivos nuevos según su checkpoint. Para incorporar otro día, se elige una fecha con archivo disponible en NOAA que aún no se haya procesado. Las fechas de junio de 2023 reproducen el archivo histórico. Los CSV y el HTML de `qa/` documentan una prueba local con una muestra. Las consultas del tablero leen las tablas de oro que genera el job. [Fuente AIS y documentación de Marine Cadastre](https://hub.marinecadastre.gov/pages/vesseltraffic).

### Job en la UI

Estas instrucciones usan el job compartido **Orquestracion OceanWatch** y el cómputo **clase14-equipo8**. Los pasos 1 a 4 dejan datos en oro. Con una fecha ya procesada por las cinco tareas, se puede continuar en el paso 5. El equipo también tiene una [guía de ejecución en Word](Gui%CC%81a%20de%20Ejecucio%CC%81n%20en%20DataBricks.docx).

1. **Importar el proyecto y conectar el cómputo.** En el workspace institucional **DBR-ACAD-INGSISTEMAS-MINE**, abre tu carpeta personal en **Workspace → Users** e importa el ZIP. Localiza la carpeta que contiene `src/`, `dashboards/` y `UpdatedPub150.csv`. En el menú de `src/` selecciona **Copy path**. Comprueba que `_config` y los notebooks `00` a `05` están juntos. En **Compute**, inicia `clase14-equipo8` si está detenido y espera a **Running**. Abre `src/00_setup` y selecciónalo en la barra de cómputo. Databricks puede mostrar los notebooks sin la extensión `.ipynb`.
2. **Ejecutar la preparación.** En `00_setup`, pulsa **Run all** con una identidad que pueda crear los objetos y aplicar los permisos. La celda del WPI debe imprimir `Puertos 3807` con el CSV incluido. La celda de la máscara comprueba la huella del archivo y las 64.800 teselas. Si falta una referencia, abre **Catalog → dbr_acad_ingsistemas_mine → default → Volumes → oceanwatch_data → ref → Upload**. Desde la copia descomprimida del proyecto, sube `UpdatedPub150.csv` de la raíz o `reference/agua_ne_v1.csv.gz`, según indique el error. Conserva los nombres y la extensión `.gz` de la máscara. Vuelve a ejecutar `00_setup`.
3. **Vincular el job a esta copia.** Abre `src/05_oceanwatch`, conéctalo a `clase14-equipo8` y ejecuta las tres primeras **celdas de código** en orden. La primera lee la fecha, la segunda ejecuta `%run ./_config` y la tercera crea el selector `modo`. Selecciona `modo=configurar_job`, deja una fecha explícita como `2023-06-08`, pega la ruta de `src/` en `ruta_src` y pulsa **Run all**. Este modo actualiza el job compartido que ya contiene las tareas `01` a `04`. En **Jobs & Pipelines → Orquestracion OceanWatch → Tasks**, comprueba las rutas, el cómputo y la secuencia `01_ingesta → 02_bronze → 03_silver → 04_gold → 05_oceanwatch`. La quinta tarea debe tener `modo=pipeline`. Si todas las tareas ya apuntan a la copia correcta, continúa con el paso 4.
4. **Procesar las fechas en orden.** En el menú junto a **Run now**, elige la opción que permite cambiar los parámetros de la ejecución. Selecciona las cinco tareas, escribe `2023-06-07` en `fecha` y lanza el job. Espera a que las cinco finalicen con éxito. Repite con `2023-06-08` y luego con `2023-06-09`. Así el modelo tiene días anteriores para evaluar el 9. Si se quiere incorporar al catálogo institucional todo el período de la primera entrega, comienza por el 1 de junio, avanza un día por ejecución hasta el 7 y continúa con los siguientes. La fecha corresponde al día de las señales en UTC.
5. **Publicar la consola institucional.** Usa la última fecha procesada, el JSON institucional y un SQL warehouse del equipo. [Punto 4. Consola de visualización](#punto-4-consola-de-visualización).
6. **Comprobar gobernanza y ML.** Revisa permisos y linaje, concilia los conteos del día y sigue una alerta hasta la versión registrada. [Punto 5. Gobernanza](#punto-5-gobernanza) · [Punto 6. Anomalías de velocidad con MLflow](#punto-6-anomalías-de-velocidad-con-mlflow).
7. **Demostrar el bono.** Comparte oro desde la universidad y abre la segunda consola en un workspace independiente. [Bono. Delta Sharing en Free Edition](#bono-delta-sharing-en-free-edition).

`00_setup` no va en el job. La programación configurada es diaria a las **06:00 de Bogotá** (`America/Bogota`), con dos reintentos por tarea y una ejecución simultánea. El parámetro `fecha=AUTO` avanza por el archivo histórico desde `2023-06-08` según los días transcurridos desde `ancla_replay`. Para reprocesar un día, usa una fecha explícita en **Run now**. Para acreditar el punto 3, abre **Runs** y conserva una ejecución exitosa iniciada por el calendario (`PERIODIC`) con sus cinco tareas. Las notificaciones de fallos se revisan en **Notifications** del job.

### Punto 4. Consola de visualización

El tablero institucional **OceanWatch · Operación marítima** consulta las cuatro tablas de oro. La definición de las visualizaciones y sus consultas SQL está en el [JSON institucional](dashboards/OceanWatch_Institucional.lvdash.json). La sección **Consola AI/BI** del [notebook 05](src/05_oceanwatch.ipynb) valida las consultas en el warehouse y publica el tablero.

1. En **SQL Warehouses**, abre un warehouse institucional que puedas usar. En **Connection details → HTTP path**, copia solo el identificador que aparece después de `/warehouses/`. Este es el valor de `warehouse_id`. El clúster `clase14-equipo8` ejecuta los notebooks y el warehouse ejecuta las consultas del tablero.
2. En **Workspace**, copia la ruta de `dashboards/OceanWatch_Institucional.lvdash.json`. En el notebook `05`, selecciona `modo=tablero`, escribe `fecha=2023-06-09`, completa `warehouse_id` y pega la ruta absoluta en `ruta_dashboard`. Pulsa **Run all**. Si el notebook no encuentra el JSON como archivo, súbelo al directorio `ref/` del volumen y usa `/Volumes/dbr_acad_ingsistemas_mine/default/oceanwatch_data/ref/OceanWatch_Institucional.lvdash.json`.
3. En la salida, busca `Consultas validadas en el warehouse` y los cinco nombres `indicadores`, `mapa`, `ranking`, `presencia` y `anomalias`. Abre el enlace `Tablero publicado`. En **Día AIS · UTC**, selecciona **9 de junio de 2023**. El filtro del tablero inicia en el 8 y se cambia independientemente del widget `fecha` del notebook.

| Vista | Qué comprobar para el día elegido |
|---|---|
| Indicadores | Posiciones válidas, buques distintos, buques evaluados por ML y alertas. Un buque puede emitir muchos mensajes en el día. |
| Mapa de intensidad | Muestra hasta las 3.000 celdas H3 de resolución 8 con más posiciones. La intensidad corresponde a la suma diaria de mensajes en cada celda. |
| Ranking de puertos | Ordena hasta 12 puertos por número de MMSI distintos. El número WPI identifica al puerto, incluso cuando dos puertos comparten nombre. |
| Buques en puerto | Para cada puerto y MMSI muestra la primera y la última señal UTC y cuántas se observaron en la vecindad del puerto. La presencia se define por la celda H3 de resolución 7 del puerto y su primer anillo. |
| Anomalías del día | Ordena las alertas de mayor a menor `score`. Permite comparar la velocidad AIS con la estimada por los tramos y consultar la versión del modelo. |

Para la sustentación, conserva el enlace **publicado** y una captura con el día visible. Para revisar el diseño antes de conectarse al workspace, el equipo puede abrir la [vista HTML de la muestra del 9 de junio](qa/Vista_OceanWatch.html).

### Punto 5. Gobernanza

Los consumidores institucionales consultan `dbr_acad_ingsistemas_mine.oceanwatch_oro_eq8`. El responsable operativo es `mine-group8`. El contrato documenta fuentes, columnas, tipos, unidades, valores nulos, controles de calidad, periodicidad y reglas para cambiar los productos. [Contrato de datos de la capa oro](docs/contrato_oro.md).

| Tabla de oro | Qué representa una fila | Llave |
|---|---|---|
| `oceanwatch_gold_trayectorias` | Trayectoria de un buque durante un día UTC | `fecha, MMSI` |
| `oceanwatch_gold_presencia_puertos` | Presencia observada de un buque junto a un puerto WPI durante el día | `fecha, numero_puerto, MMSI` |
| `oceanwatch_gold_trafico_celda_hora` | Tráfico en una celda H3 durante una hora UTC | `fecha, hora, h3_cell` |
| `oceanwatch_gold_anomalias` | Estado de evaluación de velocidad de un buque durante el día | `fecha, MMSI` |

1. **Aplicar y revisar permisos.** Con una identidad facultada para administrar estos objetos, abre `05`, selecciona `modo=gobernanza` y escribe en `grupo_lectura` el nombre real del grupo analítico acordado por el equipo, distinto de `mine-group8`. Pulsa **Run all**. El notebook concede a ese grupo `USE CATALOG`, `USE SCHEMA` y `SELECT` sobre las cuatro tablas de oro. Con el campo vacío, actualiza los comentarios y las propiedades y muestra los permisos existentes. Revisa los resultados de `SHOW GRANTS` y los permisos heredados del grupo lector. Para abrir la consola publicada con sus propias credenciales, el lector también necesita **CAN VIEW** en el tablero y **CAN USE** en el warehouse.
2. **Seguir el linaje observado.** Después de un job exitoso, abre **Catalog → dbr_acad_ingsistemas_mine → oceanwatch_oro_eq8**, elige una tabla y entra a **Lineage**. En presencia portuaria, sigue las entradas de Silver y del World Port Index. En anomalías, sigue la entrada de trayectorias. Guarda una captura donde se lean los nombres de origen y destino. El contrato también incluye el diagrama de dependencias del proyecto.
3. **Conciliar la jornada.** En `05` selecciona `modo=verificar`, indica la misma `fecha` y pulsa **Run all**. Debe aparecer `plata = posiciones en trayectorias = tráfico`. Se comprueban las llaves, el balance de Silver y que cada buque con trayectoria tenga una fila de evaluación. También se consulta `system.access.table_lineage` cuando la identidad tiene acceso. La salida deja el archivo `verificacion.json` en `/Volumes/dbr_acad_ingsistemas_mine/default/oceanwatch_data/evidencias/<fecha>/`. Allí quedan los conteos, el estado de la consulta de linaje y las ejecuciones recientes del job.

### Punto 6. Anomalías de velocidad con MLflow

La opción de ML elegida es la **detección de anomalías de velocidad**. `05_oceanwatch` lee las trayectorias diarias de `04_gold` y calcula tres variables por buque y día. Usa la velocidad media reportada por AIS (`sog_media`, en nudos), el logaritmo `log1p` de la velocidad estimada por tramos y el `log1p` de la diferencia absoluta entre ambas velocidades. La estimación por tramos divide la distancia en kilómetros entre `1,852` y las horas transcurridas entre la primera y la última señal. Requiere al menos dos posiciones y diez minutos de observación.

El modelo **Isolation Forest** aprende de buques-día elegibles de los **14 días anteriores** a la fecha evaluada. Requiere al menos 100 ejemplos y usa como máximo 30.000. El `score` es el negativo de la función de decisión del modelo. Un `score > 0` genera una alerta. La evaluación también cuenta cuántos casos superarían el percentil 99 histórico de `sog_media` y cuántos coinciden con las alertas del modelo.

1. **Ejecutar el modelo.** Al procesar un día con el job, la quinta tarea ya ejecuta `05` en `modo=pipeline`. Para repetir esa tarea manualmente sobre un día que `04_gold` ya cerró, selecciona `modo=pipeline`, escribe la fecha y pulsa **Run all**. Si ni los datos ni el historial pertinente cambiaron, una repetición puede informar `Fechas de ML actualizadas: 0`.
2. **Revisar la cobertura.** En `modo=verificar`, el resumen agrupa buques y alertas por `estado`, `modelo_version` y `run_id`. `evaluado` indica que hay puntuación. `traza_insuficiente` indica que faltan observaciones o duración para estimar la velocidad. `sin_historial` indica que la traza es utilizable, pero aún faltan ejemplos anteriores para entrenar. En estos dos últimos estados la alerta queda nula. Por eso el tablero muestra por separado los buques distintos y los evaluados.
3. **Seguir una alerta hasta el experimento.** En el tablero, anota el `MMSI`, la fecha y `modelo_version`. Busca esa fila en `oceanwatch_gold_anomalias` y copia su `run_id`. En **Experiments**, abre `OceanWatch_eq8_velocidad` del usuario que ejecutó la tarea. El run de entrenamiento guarda variables, parámetros y firma del modelo. El run de evaluación temporal registra cobertura, alertas del modelo y resultados de la regla de referencia. En **Catalog → dbr_acad_ingsistemas_mine → oceanwatch_oro_eq8 → oceanwatch_detector_velocidad**, abre la versión anotada. Conserva el enlace al run y a esa versión para la sustentación.

Como referencia para interpretar la salida, la **muestra local** del 9 de junio de 2023 contiene 171.116 posiciones válidas y 479 buques. Se evaluaron 460 y el modelo señaló 11. La ejecución institucional procesa los archivos diarios completos y produce sus propios conteos y versiones. [Indicadores de la muestra](qa/salidas_muestra/indicadores.csv) · [Cobertura por estado y versión](qa/salidas_muestra/cobertura_ml.csv).

### Bono. Delta Sharing en Free Edition

En el bono intervienen dos workspaces. El institucional **provee** las cuatro tablas de oro mediante **Delta Sharing con Open Sharing**. Un workspace independiente de **Databricks Free Edition** **lee** esas tablas y publica su propio tablero con su propio SQL warehouse. Para comprobar que ambos ven los mismos resultados, se compara una fecha con datos en los dos entornos.

1. **Universidad, publicar el share.** En `src/05_oceanwatch`, selecciona `modo=compartir` y la fecha usada en el tablero, por ejemplo `2023-06-09`. Ejecuta **Run all** con permisos para gestionar el share y el destinatario. Comprueba que `oceanwatch_eq8_oro` contiene las cuatro tablas con alias bajo `oro` y que el destinatario `oceanwatch_eq8_free` tiene `SELECT` sobre el share. Copia el JSON de control que imprime la última celda. Contiene el host del proveedor, la fecha, el conteo y una suma de hashes por tabla.
2. **Universidad, obtener el perfil de activación.** En **Catalog → OpenSharing / Delta Sharing → Shared by me → Recipients**, abre `oceanwatch_eq8_free`. Usa **Activation link** para obtener el perfil `.share` del destinatario. El perfil contiene la credencial de lectura y se entrega en privado al responsable del workspace Free. El JSON del paso anterior sirve para comparar resultados.
3. **Free Edition, vincular los datos.** Entra al workspace independiente. En **Catalog → OpenSharing → Shared with me → Install share**, importa el `.share` como proveedor `oceanwatch_equipo8_open`. Abre el share `oceanwatch_eq8_oro` y crea el catálogo `oceanwatch_externo`. En **Catalog → oceanwatch_externo → oro** deben aparecer las cuatro tablas. Esta vinculación permite consultar los productos que publica la universidad.
4. **Free Edition, ejecutar y publicar.** Importa el [notebook consumidor](free/06_consumo_free.ipynb) y localiza el [JSON del tablero de Free](dashboards/OceanWatch_Free.lvdash.json) en ese workspace. Abre `06_consumo_free` con el cómputo de Free y ejecuta la primera celda de código para crear los widgets. Coloca `fecha=2023-06-09`, pega el JSON de control en `firma_proveedor`, indica el `warehouse_id` **de Free**, pega la ruta absoluta de su JSON en `ruta_dashboard` y selecciona `publicar=si`. Con el proveedor ya instalado, `ruta_perfil` se deja vacío. Pulsa **Run all**.
5. **Acreditar la interoperabilidad.** La salida debe decir `Las cuatro tablas coinciden entre los dos workspaces.` y mostrar un enlace `Tablero publicado`. El notebook comprueba que los hosts sean distintos y que coincidan la fecha, los conteos y las sumas de hashes de las cuatro tablas. Abre el enlace emitido en Free, selecciona la misma fecha en **Día AIS · UTC** y compara sus indicadores con los del tablero institucional. Para la entrega, conserva ambos enlaces y la salida de la comparación.

### Documentación y evidencias de la Entrega 2

- **Qué pide el curso.** El PDF reúne los requisitos, sus porcentajes y las condiciones del bono. El Word complementario identifica la fuente AIS y los campos de referencia. [Enunciado de la Entrega 2](docs/Proyecto_Final_2026-20_OceanWatch_AIS%20-%20Parte%202.pdf) · [Documento complementario](docs/Proyecto_Final_2026-20_OceanWatch_AIS_Parte_2_1.docx).
- **Qué cambió y cómo sustentarlo.** La bitácora relaciona cada requisito con su implementación, documenta decisiones sobre calidad y llegadas tardías y propone un recorrido de sustentación. La guía PDF recoge la secuencia de demostración del equipo. Su paso «Aplicar el incremento al repositorio» ya está incorporado en este ZIP completo. El primer notebook que se ejecuta es `00_setup`. [Bitácora de la Entrega 2](docs/bitacora_entrega2.md) · [Guía OceanWatch](docs/Guia_OceanWatch.pdf).
- **Qué se puede revisar fuera de Databricks.** En `qa/` están el procedimiento de reproducción, el modelo MLflow exportado y los resultados por fecha de la muestra. Las evidencias del workspace que presenta el equipo se obtienen en sus propias ejecuciones del job, la publicación de los tableros, los permisos y el registro institucional del modelo. [Reproducción de la muestra](qa/reproducir_muestra.py) · [Salidas de la muestra](qa/salidas_muestra/).

El inventario [SHA256SUMS.txt](integracion/SHA256SUMS.txt) contiene las huellas del paquete previo a esta actualización del README.
