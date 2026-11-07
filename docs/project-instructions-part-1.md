# Proyecto Final  
## Soluciones Intensivas en Datos (2026-20)  
### Entrega 1  

**OceanWatch Analytics: el lakehouse del trafico maritimo**

Proyecto final · 30% de la nota del curso · Grupos de 3 · Dos entregas

---

## 1. Contexto

Ustedes son el equipo de datos de **OceanWatch Analytics**, el operador analitico que las autoridades portuarias y las navieras contratan para entender el trafico maritimo: que buques pasan, por donde, a que velocidad y cuanto tiempo permanecen en puerto.

Los buques mercantes transmiten su posicion de forma continua mediante el **AIS** (Automatic Identification System): identificador, posicion, velocidad, rumbo y caracteristicas del buque, mensaje a mensaje. La NOAA publica todo ese trafico para las aguas de Estados Unidos en archivos diarios.

En la **primera fase** del proyecto exploraran esos datos, responderan las preguntas que hace el negocio y los dejaran almacenados de forma optima para el proposito acordado.

---

## 2. Los datos

- **Fuente oficial:** AIS Data de Marine Cadastre (NOAA). Un archivo zip por dia, con un CSV de posiciones adentro.  
  Documentacion del dataset y del estandar AIS:  
  [https://hub.marinecadastre.gov/pages/vesseltraffic](https://hub.marinecadastre.gov/pages/vesseltraffic)

- **Corpus de la Entrega 1:** los dias **1 al 7 de junio de 2023** (7 archivos, ~2,3 GB comprimidos en total, del orden de 150 millones de filas). Enlaces directos:

  - [https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_01.zip](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_01.zip)
  - [https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_02.zip](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_02.zip)
  - [https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_03.zip](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_03.zip)
  - [https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_04.zip](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_04.zip)
  - [https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_05.zip](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_05.zip)
  - [https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_06.zip](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_06.zip)
  - [https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_07.zip](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_07.zip)

- **Columnas de referencia** (verifican las exactas de sus archivos):  
  `MMSI` (identificador del buque), `BaseDateTime`, `LAT`, `LON`, `SOG` (velocidad sobre el fondo, nudos), `COG` (rumbo), `Heading`, `VesselName`, `IMO`, `CallSign`, `VesselType` (codigo), `Status`, `Length`, `Width`, `Draft`, `Cargo`, `TransceiverClass`.

- **Recursos complementarios:**
  - **World Port Index** (puertos del mundo): disponible desde la misma pagina del dataset ([https://hub.marinecadastre.gov/pages/vesseltraffic](https://hub.marinecadastre.gov/pages/vesseltraffic)); tambien se acepta cualquier otra fuente de puertos, citada en el README.
  - Los codigos de `VesselType` corresponden a las categorias de tipo de buque del estandar AIS; su descripcion esta en la documentacion del dataset.

- Estos son **datos crudos de sensores**: traen problemas reales de la fuente. Detectarlos, cuantificarlos y documentarlos hace parte del trabajo.

- Para el analisis espacial pueden usar una **grilla regular**: celdas H3 (con la libreria `h3`) o simplemente redondear latitud/longitud a una rejilla fija. Cualquiera de las dos sirve si la justifican.

---

## 3. Entrega 1 — explorar, responder, almacenar bien

**40% del proyecto (12% de la nota del curso)** · Databricks Free Edition

**Alcance:** todo lo visto hasta la semana 7. No se requiere pipeline por capas, streaming ni ML en esta fase: **explorar, responder y almacenar de forma optima para un proposito**.

| # | Requisito | % |
|---|-----------|---|
| 1 | **Ingesta documentada:** descarguen los 7 dias desde la fuente oficial dentro del notebook (con reintentos y verificacion de integridad), descompriman en su Volume y lean los CSV con esquema explicito | 15 |
| 2 | **Exploracion y perfilamiento:** posiciones por dia, buques unicos, distribucion por tipo de buque y tamaño; identifiquen y cuantifiquen los problemas de calidad que encuentren (posiciones fuera de rango, velocidades imposibles, identificadores anomalos, duplicados, etc.). Ese diagnostico es la materia prima de la Entrega 2 | 20 |
| 3 | **Preguntas de negocio.** Respondan las 5 preguntas con Spark eficiente y justifiquen sus decisiones tecnicas con el plan de ejecucion: | 25 |
|   | **a.** ¿Cuantos buques distintos transmitieron cada dia? Comparen el conteo exacto con `approx_count_distinct` y argumenten cual usarian en produccion. |   |
|   | **b.** ¿Que tipos de buque generan mas trafico? Top 10 por numero de posiciones, con velocidad media por tipo (usen el catalogo de tipos). |   |
|   | **c.** ¿Que 10 buques recorrieron mas distancia durante la semana? (orden por buque y tiempo, distancia entre posiciones consecutivas con haversine). |   |
|   | **d.** ¿Donde se concentra el trafico? Top 10 celdas de una grilla espacial (H3 resolucion 8, o su propia rejilla lat/lon) por numero de posiciones, y cuales de esas celdas corresponden a puertos del World Port Index. |   |
|   | **e.** ¿Que proporcion de los buques de la semana transmitio los 7 dias? ¿Donde estan los "visitantes de un solo dia"? |   |
| 4 | **Almacenamiento optimo para un proposito:** declaren un proposito de consulta concreto (ejemplo: "la consulta diaria del operador, que filtra por fecha y zona maritima"), elijan formato de archivo y de tabla (Parquet vs Delta) y layout (particionamiento, `CLUSTER BY`), y demuestren la mejora con evidencia: bytes antes/despues, archivos leidos por cada consulta (plan o `INPUT_FILE_NAME`) y efecto de `OPTIMIZE` | 25 |
| 5 | **Gobernanza basica:** organicen su trabajo en Unity Catalog (catalogo y esquemas propios) y dejen comentarios y metadatos en sus tablas | 10 |
| 6 | **Repositorio y bitacora:** repositorio con los notebooks documentados, README y bitacora con el aporte de cada clase | 5 |

**Fechas:** el proyecto se publica el sabado 12 de septiembre. **Entrega 1:** viernes 25 de septiembre, 23:59 (enlace al repositorio del grupo). Esta entrega no lleva sustentacion: la unica sustentacion del proyecto es la final, en la ultima clase.

---

## 5. Recomendaciones

- Serverless tiene limites: `cache()`/`persist()` no estan disponibles en Free Edition (los veran de nuevo en el cluster real). Ante la duda, consulten que si funciona.
- Los zip no se leen directamente con Spark: primero descompriman en su Volume.
- Toda decision tecnica se justifica con **evidencia** (planes de ejecucion, bytes, archivos leidos), no con opiniones. "Lo hice porque es mas rapido" no es un argumento.
- Trabajen de forma incremental: cada clase aporta una pieza del proyecto y la bitacora es la evidencia de ese avance continuo.
- El diagnostico de calidad de la Entrega 1 es el insumo de las reglas de la Entrega 2: cuanto mejor lo documenten ahora, mas facil les sera despues.

---

## 6. Reglas del juego

- **Grupos de 3 estudiantes.** En la sustentacion final cada integrante debe poder explicar cualquier parte del proyecto (hay evaluacion individual).
- La entrega se hace con el **Bloque Neon**.
- La calidad de forma y contenido (documentacion, estandares, redaccion) hace parte de la calificacion.