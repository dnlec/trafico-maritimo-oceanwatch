# OceanWatch AIS — Análisis de tráfico marítimo (Entrega 1)

Análisis de big data sobre tráfico marítimo a partir de datos **AIS** (Automatic Identification System)
de la NOAA, procesados con **PySpark en Databricks** (Unity Catalog + Delta Lake). Corresponde a la
Parte 1 del proyecto final.

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

```
.
├── src/
│   ├── proyecto_ocean_watch.ipynb   # Parte 1 (Requisitos 1–5)
│   ├── 00_setup.ipynb               # Parte 2: volume, tablas, WPI, permisos
│   ├── 01_ingesta.ipynb             # Parte 2: zip NOAA del día → landing
│   ├── 02_bronze.ipynb              # Parte 2: Auto Loader → bronze
│   ├── 03_silver.ipynb              # Parte 2: reglas de calidad → silver / cuarentena
│   └── 04_gold.ipynb                # Parte 2: trayectorias, puertos, tráfico
├── docs/
│   ├── Proyecto_Final_..._Parte 1.pdf   # Enunciado del proyecto
│   └── data-dictionary.pdf              # Diccionario de datos AIS
├── exploration/                     # Resultados exportados de la exploración (CSV)
│   ├── daily_summary.csv
│   ├── quality_diagnostics.csv
│   ├── size_metrics.csv
│   └── vessel_type_dist.csv
├── UpdatedPub150.csv                # World Port Index
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

Cinco notebooks en `src/`, pensados para el editor web de Databricks en el cluster shared
(acceso standard). Los archivos se escriben con Python sobre `/Volumes`. No hay bundle.

| Notebook | Qué hace | Cuándo |
|---|---|---|
| `00_setup.ipynb` | Volume, carpetas, tablas, World Port Index y `GRANT` a `mine-group8` | Una vez, a mano |
| `01_ingesta.ipynb` | Zip NOAA del día `fecha` → `landing/` | Tarea del job |
| `02_bronze.ipynb` | Auto Loader de `landing/` a bronze, con conteo antes/después | Tarea del job |
| `03_silver.ipynb` | Reglas de calidad: válidas a silver, el resto a cuarentena | Tarea del job |
| `04_gold.ipynb` | Trayectorias, presencia en puerto y tráfico celda × hora | Tarea del job |

Antes de `00_setup`, el CSV `UpdatedPub150.csv` tiene que estar en `ref/` del volume
(o seguir en el volume de la Parte 1, `proyecto_datos.default.data`). El setup lo copia.

Catálogo `dbr_acad_ingsistemas_mine`, esquema `default`, volume `oceanwatch_data`.
El día entra por el widget `fecha` (`YYYY-MM-DD`).

### Job en la UI

1. Create Job. Cluster: el shared del curso (standard, Unity Catalog).
2. Cuatro tareas en este orden, cada una un notebook de `src/`: `01_ingesta` → `02_bronze` → `03_silver` → `04_gold`. Cada flecha es "depends on" la anterior.
3. Job parameter `fecha`. En cada tarea, el parámetro del notebook `fecha` vale `{{job.parameters.fecha}}`.
4. Schedule diario (por ejemplo 06:00 America/Bogota), cuando el zip de NOAA de ese día ya existe. El valor de `fecha` hay que pasarlo en cada ejecución, o fijarlo en el schedule si el día es siempre ayer.
5. En cada tarea, Retries en 1 o 2. En Notifications del job, correo al fallar.
6. Max concurrent runs = 1. Los checkpoints de bronze y silver son únicos: dos ejecuciones a la vez los corrompen.

`00_setup` no va en el job. MLflow y el dashboard quedan para después.

