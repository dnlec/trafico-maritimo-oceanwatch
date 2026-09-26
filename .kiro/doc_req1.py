import json, uuid, io

NB = "notebook/proyecto_ocean_watch.ipynb"
with io.open(NB, "r", encoding="utf-8") as f:
    nb = json.load(f)
cells = nb["cells"]

assert "".join(cells[0]["source"]).startswith("# Requisito 1"), "cell 0 no es el header de Req 1"
assert "import os" in "".join(cells[1]["source"]), "cell 1 no es la ingesta"
assert "ais_schema" in "".join(cells[2]["source"]), "cell 2 no es el schema/read"
assert "display(df_ais.limit(10))" in "".join(cells[3]["source"]), "cell 3 no es el preview"
assert "df_ais.count()" in "".join(cells[4]["source"]), "cell 4 no es el count"


def nuid():
    return str(uuid.uuid4())


def md(text):
    return {
        "cell_type": "markdown",
        "metadata": {"application/vnd.databricks.v1+cell": {
            "cellMetadata": {}, "inputWidgets": {}, "nuid": nuid(),
            "showTitle": False, "tableResultSettingsMap": {}, "title": ""}},
        "source": text.split("\n"),
    }


# 1. Header intro (replace cell 0 with header + short roadmap)
cells[0]["source"] = (
    "# Requisito 1 Ingesta\n"
    "\n"
    "Descargamos los 7 dias de datos AIS (NOAA, 2023-06-01 a 06-07) al Volume, los descomprimimos y los\n"
    "leemos como un unico DataFrame con esquema explicito. Tres pasos: **descarga + verificacion**, **lectura\n"
    "tipada** y una **comprobacion** rapida de que la ingesta quedo completa."
).split("\n")

# 2. Markdown before ingest code (cell 1) -> insert at index 1
md_ingest = md(
    "## Descarga, verificacion e ingesta\n"
    "\n"
    "Por cada URL: descarga con reintentos (`download_with_retries`), verifica que el zip no este corrupto\n"
    "(`verify_zip_integrity`), extrae los CSV al Volume y borra el zip. Si una descarga o la integridad\n"
    "falla, se detiene con error en vez de continuar con datos incompletos."
)

# 3. Markdown before schema-read (cell 2)
md_read = md(
    "## Lectura con esquema explicito\n"
    "\n"
    "Leemos todos los CSV con un esquema declarado (`ais_schema`) en vez de inferirlo: garantiza los tipos\n"
    "correctos (p. ej. `BaseDateTime` como `timestamp`, `LAT`/`LON` como `double`) y evita una pasada extra\n"
    "de inferencia sobre 60 M de filas."
)

# 4. Markdown before verification (cell 3 preview + cell 4 count)
md_check = md(
    "## Comprobacion de la ingesta\n"
    "\n"
    "Una vista de 10 filas para inspeccionar la estructura y el conteo total de registros, que confirma que\n"
    "los 7 dias se cargaron (~60.5 M de posiciones)."
)

# Insert from the back to keep indices valid
cells.insert(3, md_check)   # before cell 3 (preview)
cells.insert(2, md_read)    # before cell 2 (schema read)
cells.insert(1, md_ingest)  # before cell 1 (ingest)

nb["cells"] = cells
with io.open(NB, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)
print("Req 1 documentado. Total celdas:", len(cells))
