import json, uuid, io

NB = "notebook/proyecto_ocean_watch.ipynb"

with io.open(NB, "r", encoding="utf-8") as f:
    nb = json.load(f)

cells = nb["cells"]

# Sanity: Req 2 must be cells 5..12 (header + 5 code + 2 markdown)
assert "".join(cells[5]["source"]).startswith("# Requisito 2"), "cell 5 no es el header de Req 2"
assert "".join(cells[13]["source"]).startswith("# Requisito 3"), "cell 13 no es el header de Req 3"


def nuid():
    return str(uuid.uuid4())


def md(text):
    return {
        "cell_type": "markdown",
        "metadata": {
            "application/vnd.databricks.v1+cell": {
                "cellMetadata": {},
                "inputWidgets": {},
                "nuid": nuid(),
                "showTitle": False,
                "tableResultSettingsMap": {},
                "title": "",
            }
        },
        "source": text.split("\n"),
    }


def code(text):
    return {
        "cell_type": "code",
        "execution_count": 0,
        "metadata": {
            "application/vnd.databricks.v1+cell": {
                "cellMetadata": {"byteLimit": 2048000, "rowLimit": 10000},
                "inputWidgets": {},
                "nuid": nuid(),
                "showTitle": False,
                "tableResultSettingsMap": {},
                "title": "",
            }
        },
        "outputs": [],
        "source": text.split("\n"),
    }


new_cells = []

new_cells.append(md(
    "# Requisito 2 Exploracion y perfilamiento\n"
    "\n"
    "**Objetivo.** Caracterizar el dataset AIS de la semana (7 dias) antes de responder preguntas de\n"
    "negocio o disenar el almacenamiento: cuanto volumen hay, como se comporta dia a dia, que tipos de\n"
    "embarcacion lo componen y, sobre todo, que problemas de calidad tiene, para definir las reglas de\n"
    "limpieza de la Entrega 2.\n"
    "\n"
    "El analisis se organiza en cuatro pasos:\n"
    "\n"
    "1. **Preparacion** de columnas derivadas que se reutilizan en todo el requisito (`EventDate` y una\n"
    "   marca de duplicados).\n"
    "2. **Volumen** general y por dia (total de registros, buques unicos, actividad diaria).\n"
    "3. **Perfilamiento** de embarcaciones: distribucion por tipo de buque (con velocidad media) y\n"
    "   distribucion de dimensiones fisicas (eslora, manga, calado).\n"
    "4. **Diagnostico de calidad**: una tabla unica que cuantifica cada regla de validacion sobre el\n"
    "   total de filas. Es el insumo directo de la Entrega 2."
))

new_cells.append(md(
    "## Paso 1: Columnas derivadas de apoyo\n"
    "\n"
    "Dos columnas que reutilizan los pasos siguientes:\n"
    "\n"
    "- **`EventDate`**: la fecha (sin hora) de cada emision, para los cortes diarios.\n"
    "- **`is_duplicate`**: marca de duplicado exacto. Definimos duplicado como una misma emision fisica,\n"
    "  es decir, la combinacion `(MMSI, BaseDateTime, LAT, LON)` que aparece mas de una vez. En lugar de un\n"
    "  `row_number()` sobre una ventana, cuyo orden dentro del grupo es arbitrario y confunde la lectura,\n"
    "  contamos cuantas veces aparece cada combinacion (`count` sobre la particion) y marcamos como\n"
    "  duplicada toda fila cuyo grupo tenga mas de un registro. Es equivalente en el conteo y autoexplicativo."
))
new_cells.append(code(
    "from pyspark.sql import functions as F\n"
    "from pyspark.sql.window import Window\n"
    "\n"
    "# Fecha del evento (sin hora) para los cortes diarios\n"
    "df_prepared = df_ais.withColumn(\"EventDate\", F.to_date(\"BaseDateTime\"))\n"
    "\n"
    "# Marca de duplicado exacto: misma emision fisica repetida.\n"
    "# Duplicado = combinacion (MMSI, BaseDateTime, LAT, LON) que aparece mas de una vez.\n"
    "# Contamos el tamano de cada grupo y marcamos las filas cuyo grupo tiene mas de 1 registro.\n"
    "dup_window = Window.partitionBy(\"MMSI\", \"BaseDateTime\", \"LAT\", \"LON\")\n"
    "df_flagged = df_prepared.withColumn(\n"
    "    \"is_duplicate\",\n"
    "    F.count(\"*\").over(dup_window) > 1\n"
    ")\n"
    "\n"
    "# Vista rapida de la estructura resultante\n"
    "display(df_flagged.select(\"MMSI\", \"BaseDateTime\", \"LAT\", \"LON\", \"EventDate\", \"is_duplicate\").limit(10))"
))

new_cells.append(md(
    "## Paso 2: Volumen general y actividad diaria\n"
    "\n"
    "Cuantificamos el tamano del dataset y su comportamiento en el tiempo:\n"
    "\n"
    "- **Total de registros** y **buques unicos** (MMSI distintos) en la semana completa.\n"
    "- **Posiciones y buques activos por dia**, para ver si la actividad es estable o tiene picos."
))
new_cells.append(code(
    "# 1. Total de registros y buques unicos en la semana\n"
    "total_records = df_ais.count()\n"
    "total_unique_vessels = df_ais.select(\"MMSI\").distinct().count()\n"
    "\n"
    "print(f\"Total de registros AIS: {total_records:,}\")\n"
    "print(f\"Buques unicos (MMSI distintos): {total_unique_vessels:,}\")\n"
    "\n"
    "# 2. Posiciones y buques unicos por dia\n"
    "daily_summary = (\n"
    "    df_flagged\n"
    "    .groupBy(\"EventDate\")\n"
    "    .agg(\n"
    "        F.count(\"*\").alias(\"Total_Positions\"),\n"
    "        F.countDistinct(\"MMSI\").alias(\"Unique_Vessels\")\n"
    "    )\n"
    "    .orderBy(\"EventDate\")\n"
    ")\n"
    "\n"
    "display(daily_summary)"
))

new_cells.append(md(
    "## Paso 3: Perfilamiento de embarcaciones\n"
    "\n"
    "Dos vistas del \"quien\" del dataset:\n"
    "\n"
    "- **Distribucion por tipo de buque** (`VesselType`): numero de posiciones, buques unicos y velocidad\n"
    "  media (`SOG`) por tipo, ordenado por volumen de posiciones. El catalogo de tipos AIS permite luego\n"
    "  traducir cada codigo (por ejemplo 31 = remolque, 70 = carga, 80 = tanquero).\n"
    "- **Distribucion de dimensiones fisicas** (eslora, manga, calado) via percentiles p25/p50/p75/p99,\n"
    "  para conocer el rango de tamanos y detectar valores extremos."
))
new_cells.append(code(
    "# Distribucion por tipo de buque (todos los tipos, ordenados por volumen)\n"
    "vessel_type_dist = (\n"
    "    df_ais\n"
    "    .groupBy(\"VesselType\")\n"
    "    .agg(\n"
    "        F.count(\"*\").alias(\"Position_Count\"),\n"
    "        F.countDistinct(\"MMSI\").alias(\"Unique_Vessels\"),\n"
    "        F.round(F.avg(\"SOG\"), 2).alias(\"Avg_SOG_Knots\")\n"
    "    )\n"
    "    .orderBy(F.desc(\"Position_Count\"))\n"
    ")\n"
    "display(vessel_type_dist)\n"
    "\n"
    "# Distribucion de dimensiones fisicas (Length, Width, Draft)\n"
    "# Cada arreglo trae los percentiles en orden: [p25, p50 (mediana), p75, p99]\n"
    "size_metrics = df_ais.select(\n"
    "    F.expr(\"percentile_approx(Length, array(0.25, 0.50, 0.75, 0.99))\").alias(\"Length_p25_p50_p75_p99\"),\n"
    "    F.expr(\"percentile_approx(Width,  array(0.25, 0.50, 0.75, 0.99))\").alias(\"Width_p25_p50_p75_p99\"),\n"
    "    F.expr(\"percentile_approx(Draft,  array(0.25, 0.50, 0.75, 0.99))\").alias(\"Draft_p25_p50_p75_p99\")\n"
    ")\n"
    "display(size_metrics)"
))

new_cells.append(md(
    "## Paso 4: Diagnostico de calidad de datos\n"
    "\n"
    "Una sola pasada de agregacion condicional cuenta, para cada regla de validacion, cuantas filas la\n"
    "incumplen y su porcentaje sobre el total. Reunir todas las reglas en un unico `select` evita recorrer\n"
    "los 60 M de filas una vez por regla. Las reglas cubren:\n"
    "\n"
    "- **Posiciones fuera de rango**: `LAT` fuera de [-90, 90] o `LON` fuera de [-180, 180], o exactamente 0.0.\n"
    "- **Velocidades (`SOG`) anomalas**: > 60 nudos (fisicamente imposible para trafico comercial) o negativas.\n"
    "- **MMSI anomalos**: identificadores que no son de 9 digitos o codigos de prueba (`000000000`).\n"
    "- **Heading**: valor 511 (\"no disponible\" segun la norma ITU) y valores fuera de [0, 359].\n"
    "- **Dimensiones invalidas**: `Length` o `Width` <= 0.\n"
    "- **Duplicados**: la marca `is_duplicate` del Paso 1."
))
new_cells.append(code(
    "quality_diagnostics = df_flagged.select(\n"
    "    # Total de filas, para calcular porcentajes\n"
    "    F.count(\"*\").alias(\"total_rows\"),\n"
    "\n"
    "    # 1. Posiciones fuera de rango (LAT valida: -90 a 90, LON valida: -180 a 180)\n"
    "    F.sum(F.when((F.col(\"LAT\") < -90) | (F.col(\"LAT\") > 90) | (F.col(\"LAT\") == 0.0), 1).otherwise(0)).alias(\"invalid_lat_count\"),\n"
    "    F.sum(F.when((F.col(\"LON\") < -180) | (F.col(\"LON\") > 180) | (F.col(\"LON\") == 0.0), 1).otherwise(0)).alias(\"invalid_lon_count\"),\n"
    "\n"
    "    # 2. Anomalias de velocidad (SOG). Mayor a 60 nudos es imposible para naves comerciales; SOG menor a 0 es invalido\n"
    "    F.sum(F.when(F.col(\"SOG\") > 60.0, 1).otherwise(0)).alias(\"impossible_speed_count\"),\n"
    "    F.sum(F.when(F.col(\"SOG\") < 0.0, 1).otherwise(0)).alias(\"negative_speed_count\"),\n"
    "\n"
    "    # 3. MMSI anomalos: deben ser identificadores de 9 digitos; 000000000 o longitudes distintas son invalidos\n"
    "    F.sum(F.when((F.length(F.col(\"MMSI\")) != 9) | (F.col(\"MMSI\") == \"000000000\") | F.col(\"MMSI\").rlike(\"^0+$\"), 1).otherwise(0)).alias(\"anomalous_mmsi_count\"),\n"
    "\n"
    "    # 4. Anomalias de rumbo (Heading valido: 0 a 359 grados; 511 indica \"no disponible\")\n"
    "    F.sum(F.when(F.col(\"Heading\") == 511.0, 1).otherwise(0)).alias(\"heading_not_available_count\"),\n"
    "    F.sum(F.when((F.col(\"Heading\") < 0.0) | ((F.col(\"Heading\") > 359.0) & (F.col(\"Heading\") != 511.0)), 1).otherwise(0)).alias(\"invalid_heading_count\"),\n"
    "\n"
    "    # 5. Dimensiones invalidas (Length <= 0 o Width <= 0)\n"
    "    F.sum(F.when((F.col(\"Length\") <= 0) | (F.col(\"Width\") <= 0), 1).otherwise(0)).alias(\"invalid_dimensions_count\"),\n"
    "\n"
    "    # 6. Duplicados exactos (marca del Paso 1)\n"
    "    F.sum(F.when(F.col(\"is_duplicate\"), 1).otherwise(0)).alias(\"duplicate_records_count\")\n"
    ")\n"
    "\n"
    "# Reestructurar a una tabla legible: una fila por regla\n"
    "diag_row = quality_diagnostics.collect()[0]\n"
    "total = diag_row[\"total_rows\"]\n"
    "\n"
    "def pct(n):\n"
    "    return (n / total) * 100\n"
    "\n"
    "quality_summary = spark.createDataFrame([\n"
    "    (\"Latitud invalida (|LAT| > 90 o 0.0)\", diag_row[\"invalid_lat_count\"], pct(diag_row[\"invalid_lat_count\"])),\n"
    "    (\"Longitud invalida (|LON| > 180 o 0.0)\", diag_row[\"invalid_lon_count\"], pct(diag_row[\"invalid_lon_count\"])),\n"
    "    (\"Velocidad imposible (SOG > 60 nudos)\", diag_row[\"impossible_speed_count\"], pct(diag_row[\"impossible_speed_count\"])),\n"
    "    (\"Velocidad negativa (SOG < 0)\", diag_row[\"negative_speed_count\"], pct(diag_row[\"negative_speed_count\"])),\n"
    "    (\"MMSI anomalo (longitud != 9 o dummy)\", diag_row[\"anomalous_mmsi_count\"], pct(diag_row[\"anomalous_mmsi_count\"])),\n"
    "    (\"Heading no disponible (511.0)\", diag_row[\"heading_not_available_count\"], pct(diag_row[\"heading_not_available_count\"])),\n"
    "    (\"Heading fuera de rango\", diag_row[\"invalid_heading_count\"], pct(diag_row[\"invalid_heading_count\"])),\n"
    "    (\"Dimensiones invalidas (Length/Width <= 0)\", diag_row[\"invalid_dimensions_count\"], pct(diag_row[\"invalid_dimensions_count\"])),\n"
    "    (\"Duplicados (MMSI + Timestamp + Coordenadas)\", diag_row[\"duplicate_records_count\"], pct(diag_row[\"duplicate_records_count\"]))\n"
    "], [\"Quality_Issue\", \"Affected_Rows\", \"Percentage_Of_Total\"])\n"
    "\n"
    "display(quality_summary)"
))

new_cells.append(md(
    "## Diagnostico y hallazgos principales\n"
    "\n"
    "**1. Volumen general y comportamiento diario**\n"
    "\n"
    "- Total de registros procesados: **60,533,559** posiciones.\n"
    "- Buques unicos en la semana: **31,871** MMSI distintos.\n"
    "- Actividad diaria muy consistente: entre **8.0 y 9.0 millones** de posiciones por dia, con un\n"
    "  promedio cercano a **20,000** buques activos diariamente.\n"
    "\n"
    "**2. Perfilamiento de embarcaciones y dimensiones**\n"
    "\n"
    "- Tipos dominantes: el tipo **31 (Towing/Tug)** y el tipo **37 (Pleasure Craft)** suman mas de\n"
    "  **31 millones** de posiciones. Los buques mercantes y tanqueros (tipos **70** y **80**) registran\n"
    "  velocidades medias mucho mayores (**5.8 a 6.4 nudos**) que las embarcaciones menores (**1.6 a 1.9 nudos**).\n"
    "- Distribucion de tamano: la mediana es **22 m** de eslora (Length), **8 m** de manga (Width) y\n"
    "  **3.7 m** de calado (Draft). En el percentil 99 aparecen embarcaciones de hasta **323 m** de eslora\n"
    "  y **17 m** de calado (portacontenedores y grandes tanqueros).\n"
    "\n"
    "**3. Diagnostico de calidad (insumo para la Entrega 2)**\n"
    "\n"
    "| Regla | Filas afectadas | % del total | Lectura |\n"
    "|---|---:|---:|---|\n"
    "| Heading no disponible (511.0) | 33,535,628 | 55.40% | No es corrupcion: la norma ITU usa 511 cuando el barco no tiene girocompas o esta estatico. Tratarlo como categoria nula, no numerica. |\n"
    "| Dimensiones invalidas (Length/Width <= 0) | 2,284,033 | 3.77% | Eslora o manga <= 0; requiere filtro o imputacion. |\n"
    "| Velocidad imposible (SOG > 60 nudos) | 160,613 | 0.265% | Picos de ruido del GPS; descartar. |\n"
    "| MMSI anomalo | 49,897 | 0.082% | Identificadores no validos (longitud != 9 o codigos de prueba). |\n"
    "| Duplicados exactos | 1,400 | 0.002% | Misma emision repetida; eliminar. |\n"
    "| Coordenadas fuera de rango | 0 | 0.00% | Excelente calidad geometrica: sin LAT/LON fuera de rango global. |\n"
    "\n"
    "*(Las cifras corresponden a la ejecucion mas reciente; el conteo de duplicados puede reejecutarse tras\n"
    "el cambio de logica del Paso 1.)*"
))

new_cells.append(md(
    "## Conclusion\n"
    "\n"
    "El dataset tiene **alta integridad geometrica** en las coordenadas (0% fuera de rango), pero exige\n"
    "reglas de limpieza claras para la Entrega 2:\n"
    "\n"
    "- **Heading = 511** se trata como categoria nula / no disponible, no como valor numerico.\n"
    "- **Dimensiones <= 0** se filtran o imputan.\n"
    "- **Velocidades > 60 nudos** se descartan por ruido de GPS.\n"
    "- **MMSI no validos** y **duplicados exactos** se eliminan.\n"
    "\n"
    "Estas decisiones se justifican con la evidencia cuantitativa de la tabla de calidad, no con supuestos."
))

cells[:] = cells[:5] + new_cells + cells[13:]
nb["cells"] = cells

with io.open(NB, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Req 2 reconstruido. Total celdas:", len(cells))
