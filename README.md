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
├── notebook/
│   └── proyecto_ocean_watch.ipynb   # Notebook principal (Requisitos 1–5)
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

> Los datos AIS crudos no se versionan en el repositorio (son ~6 GB); se descargan desde el Requisito 1
> del notebook directamente al Volume de Databricks.



## Contenido del notebook

El notebook `notebook/proyecto_ocean_watch.ipynb` está organizado por requisito, con celdas de markdown
que explican y justifican cada paso:

- **Requisito 1 — Ingesta.** Descarga con reintentos y verificación de integridad de los 7 ZIP, extracción
al Volume y lectura como un único DataFrame con esquema explícito (~60.5 M filas).
- **Requisito 2 — Exploración y perfilamiento.** En 7 pasos: columnas derivadas, volumen y actividad
diaria, perfilamiento por tipo de buque y dimensiones, diagnóstico de calidad (reglas de validación),
auditoría de nulos por columna, consistencia de atributos estáticos por MMSI y anomalías adicionales
(posiciones imposibles en el tiempo, `VesselType` fuera de catálogo, `MMSI = 0`). Cierra con hallazgos
y conclusión orientados a la limpieza de la Entrega 2.
- **Requisito 3 — Preguntas de negocio.** (a) buques distintos por día (exacto vs `approx_count_distinct`),
(b) tipos de buque con más tráfico y velocidad media, (c) top 10 de distancia recorrida (haversine con
ventana por MMSI), (d) concentración de tráfico en grilla **H3 res. 8** y cruce con el WPI, (e)
proporción de buques activos los 7 días y ubicación de los "visitantes de un solo día". Las decisiones
técnicas se justifican con el plan de ejecución real (`explain("formatted")`).
- **Requisito 4 — Almacenamiento óptimo para un propósito.** Propósito de consulta (día + sector `h3_cell`)
y decisiones de formato/tabla/layout: **CSV vs Parquet vs Delta**, partición por `date`, `OPTIMIZE` +
`ZORDER (h3_cell)`. Evidencia medida: bytes (6.04 GB → 2.01 GB → 1.48 GB), archivos leídos por la
consulta (Parquet 6 vs Delta 1) y efecto de `OPTIMIZE` a esta escala, con tabla resumen y conclusión.
- **Requisito 5 — Gobernanza.** Comentarios y metadatos aplicados sobre los activos en Unity Catalog
(aplicados directamente en Databricks). El notebook incluye una celda de verificación (`DESCRIBE EXTENDED`)
  que deja evidencia del comentario y las `TBLPROPERTIES` de la tabla.



## Cómo ejecutarlo

1. En Databricks (Free Edition / Serverless sirve), importar `notebook/proyecto_ocean_watch.ipynb`.
2. Crear el catálogo `proyecto_datos`, el esquema `default` y el volumen `data` en Unity Catalog.
3. Colocar `UpdatedPub150.csv` en el volumen (`/Volumes/proyecto_datos/default/data/`).
4. Ejecutar los requisitos en orden: el Requisito 1 descarga y deja los CSV en el Volume; el resto lee
  desde ahí. Los requisitos 2–4 dependen del DataFrame `df_ais` cargado en el Requisito 1.



## Stack

PySpark - Delta Lake - Unity Catalog - Databricks - H3 (funciones geoespaciales nativas) - Photon.

