import json, io

NB = "notebook/proyecto_ocean_watch.ipynb"
with io.open(NB, "r", encoding="utf-8") as f:
    nb = json.load(f)
cells = nb["cells"]

assert "".join(cells[20]["source"]).startswith("## Diagnostico y hallazgos"), "cell 20 no es Hallazgos"
assert "".join(cells[21]["source"]).startswith("## Conclusion"), "cell 21 no es Conclusion"

hallazgos = (
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
    "**3. Completitud por columna (Paso 5)**\n"
    "\n"
    "El nucleo dinamico del mensaje AIS esta **100% completo**: `MMSI`, `BaseDateTime`, `LAT`, `LON`,\n"
    "`SOG`, `COG`, `Heading` y `TransceiverClass` no tienen nulos. Los nulos se concentran en los atributos\n"
    "estaticos/opcionales:\n"
    "\n"
    "| Columna | % nulos | Implicacion para la Entrega 2 |\n"
    "|---|---:|---|\n"
    "| `Draft` | 64.4% | Casi inutilizable como feature sin imputacion; solo confiable para el subconjunto que lo reporta. |\n"
    "| `IMO` | 42.8% | No sirve como llave universal de buque; usar `MMSI`. |\n"
    "| `Status` | 33.0% | Estado de navegacion ausente en 1/3 de las filas. |\n"
    "| `Cargo` | 32.9% | Ademas suele igualar a `VesselType`; no es tonelaje. |\n"
    "| `CallSign` | 16.8% | Identificador secundario incompleto. |\n"
    "| `Width` / `Length` | 15.2% / 6.1% | Dimensiones parcialmente ausentes; afecta perfiles de tamano. |\n"
    "\n"
    "**4. Diagnostico de calidad y anomalias (Pasos 4, 6 y 7)**\n"
    "\n"
    "| Regla / anomalia | Filas o MMSI | % | Lectura |\n"
    "|---|---:|---:|---|\n"
    "| Heading no disponible (511.0) | 33,535,628 | 55.40% | No es corrupcion: la norma ITU usa 511 cuando el barco no tiene girocompas o esta estatico. Tratar como categoria nula, no numerica. |\n"
    "| Dimensiones invalidas (Length/Width <= 0) | 2,284,033 | 3.77% | Eslora o manga <= 0; filtrar o imputar. |\n"
    "| VesselType fuera de catalogo (nulo o fuera de 0-99) | 215,721 | 0.356% | 209,863 nulos + codigos como 107/136/200 que no existen en el catalogo AIS. Normalizar o marcar como \"desconocido\". |\n"
    "| Velocidad imposible (SOG > 60 nudos) | 160,613 | 0.265% | Picos de ruido del GPS; descartar. |\n"
    "| MMSI anomalo (longitud != 9 o dummy) | 49,897 | 0.082% | Identificadores no validos. |\n"
    "| MMSI = 0 (no atribuible a un buque) | 978 | 0.0016% | Subconjunto de los MMSI anomalos; sin buque asociado. |\n"
    "| Posiciones imposibles en el tiempo (mismo MMSI+instante, distinto lugar) | 544 | 0.0009% | Un buque no puede estar en dos lugares a la vez; resolver antes de calcular trayectorias. |\n"
    "| Duplicados exactos (MMSI + Timestamp + Coordenadas) | 1,400 | 0.002% | Misma emision repetida; eliminar. |\n"
    "| Coordenadas fuera de rango | 0 | 0.00% | Excelente calidad geometrica: sin LAT/LON fuera de rango global. |\n"
    "\n"
    "**5. Consistencia de atributos estaticos por MMSI (Paso 6): sin conflictos**\n"
    "\n"
    "Contra lo que suele ocurrir en feeds AIS, en esta semana **ningun MMSI** reporta mas de un\n"
    "`VesselType`, ni mas de una eslora, manga o nombre (0 de 31,871 en las cuatro pruebas). Es un\n"
    "resultado **positivo y verificado**: los atributos estaticos son internamente consistentes por buque,\n"
    "asi que en la Entrega 2 se pueden tomar directamente (no hace falta resolver el valor \"correcto\" por\n"
    "MMSI). El unico problema de `VesselType` es de **catalogo** (nulos/codigos invalidos), no de\n"
    "**inconsistencia** entre emisiones del mismo buque."
)

conclusion = (
    "## Conclusion\n"
    "\n"
    "El dataset tiene **alta integridad estructural**: el nucleo dinamico del mensaje (posicion, tiempo,\n"
    "velocidad, rumbo) esta **100% completo y sin coordenadas fuera de rango**, y los atributos estaticos\n"
    "son **consistentes por buque** (ningun MMSI cambia de tipo o dimensiones). Los problemas se concentran\n"
    "en columnas opcionales muy incompletas y en unos pocos valores centinela/ruido. Reglas de limpieza\n"
    "para la Entrega 2, ordenadas por impacto:\n"
    "\n"
    "1. **Heading = 511** (55.4%): tratar como categoria nula / no disponible, no como valor numerico.\n"
    "2. **Columnas con muchos nulos** (`Draft` 64%, `IMO` 43%, `Status`/`Cargo` 33%): decidir por columna\n"
    "   entre descartarla, imputarla o usarla solo en el subconjunto que la reporta. No usar `IMO` como\n"
    "   llave; usar `MMSI`.\n"
    "3. **Dimensiones <= 0** (3.8%): filtrar o imputar.\n"
    "4. **VesselType fuera de catalogo** (0.36%): normalizar a un valor \"desconocido\" en vez de tratar\n"
    "   codigos invalidos como categorias reales.\n"
    "5. **Velocidades > 60 nudos** (0.27%): descartar por ruido de GPS.\n"
    "6. **MMSI no validos / = 0, duplicados exactos y posiciones imposibles en el tiempo**: eliminar; son\n"
    "   volumen bajo pero rompen los calculos de trayectoria.\n"
    "\n"
    "Todas las decisiones se justifican con la evidencia cuantitativa de los Pasos 4 a 7, no con supuestos."
)

cells[20]["source"] = hallazgos.split("\n")
cells[21]["source"] = conclusion.split("\n")

with io.open(NB, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)
print("Hallazgos y Conclusion actualizados con cifras reales.")
