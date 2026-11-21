# Referencia espacial de la compuerta de agua

OceanWatch clasifica cada posición en una rejilla geográfica global de 0,005° sobre WGS84. La máscara está dividida en 64.800 teselas de un grado. Una tesela puede ser completamente agua (clase 0), completamente tierra (1) o mixta (2). En una tesela mixta se consultan 200 × 200 bits, en orden norte-sur y oeste-este. Un bit uno representa tierra. La dimensión de las teselas se valida al crear la tabla y el SHA-256 verifica el comprimido.

Se rasterizan primero los polígonos de tierra, después los lagos y los ejes hidrográficos. Las celdas atravesadas por una línea de costa TIGER o por una vía de la red navegable de USACE se admiten. Esto conserva señales costeras y fluviales que una máscara cartográfica general puede rechazar. No hay una excepción basada en el nombre del buque, el WPI o su comportamiento. La proximidad al puerto por sí sola no acredita que haya agua.

| Referencia | Datos empleados | Origen |
| --- | --- | --- |
| Natural Earth 1:10m | Tierra, lagos y ejes fluviales globales. Lagos y ríos suplementarios de América del Norte. | [Vectores físicos](https://www.naturalearthdata.com/downloads/10m-physical-vectors/) |
| US Census Bureau | Línea de costa nacional TIGER/Line 2023, en el año de las posiciones AIS. | [Ficha y descarga](https://catalog.data.gov/dataset/tiger-line-shapefile-2023-nation-u-s-coastline) |
| US Army Corps of Engineers / Bureau of Transportation Statistics | National Waterway Network, capa de líneas. Instantánea consultada el 21 de noviembre de 2026, 6.859 elementos. La red es una referencia espacial y puede diferir de su estado en 2023. | [Capa 1 del servicio oficial](https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Waterway_Networks/FeatureServer/1) |

Las costas y cauces son aproximaciones. Una clasificación en tierra conserva el registro en cuarentena para revisión. Una celda costera admitida puede contener parte de tierra. La resolución de la máscara y la de H3 cumplen funciones distintas.

## Reconstrucción

`python descargar_hidrografia.py referencias_originales` obtiene las capas. `python construir_mascara.py referencias_originales agua_ne_v1.csv.gz` genera el archivo con GDAL y NumPy. El proyecto entrega la máscara ya construida. Si una fuente dinámica cambia, comparar las huellas antes de promover una nueva versión y reprocesar Silver con un checkpoint independiente.

| Archivo | SHA-256 de la instantánea utilizada |
| --- | --- |
| `ne_10m_lakes.zip` | `0803a06f9c3cb4671d89b68c48b142aad9366ba40f665245e12a913fbc61722a` |
| `ne_10m_lakes_north_america.zip` | `96c824a2f2646ea8b16fe6711f7906ba690788a547f51e391654c8e94271ffba` |
| `ne_10m_land.zip` | `e547d749445eaa0964aba76738090ec88f5e63c4585122170f98c67a7ea922dc` |
| `ne_10m_rivers_lake_centerlines.zip` | `ded71b01870855ccfe19b51f2ec14c9bb48fae23c0e9f3c11974d426433b5c38` |
| `ne_10m_rivers_north_america.zip` | `7fbe85a2acbf1f3ef4361a16bb39677ad1ea86f888be1689f7a4caeafc7529e8` |
| `nwn_usace.geojson` | `cedf2ffd4fe2c64171837c5272496099b8f3b57305b3f40d7d0217c9631f4183` |
| `tl_2023_us_coastline.zip` | `ed0c22593b4b5a19f17256916824cb4d665a45a1c0811c28c25caf12811f9ba9` |
| `agua_ne_v1.csv.gz` | `0e2b94d4485ddd5d736a71e4960872261bcff0175f50d13b5743ab637ff8ad36` |
