# **1. Importar el proyecto y preparar el entorno** 

1. **Entra al Databricks de la universidad.** Abre **Workspace → Users → tu correo** . En el menú ⋮ , elige **Import** , selecciona el ZIP completo y confirma. Databricks lo descomprime al importarlo. 

2. **Ubica la carpeta** **`src` .** Dentro de `OceanWatch_Equipo8` , abre `src` y comprueba que ves `_config` , `00_setup` , `01_ingesta` , `02_bronze` , `03_silver` , `04_gold` y `05_oceanwatch` . En el menú de **la carpeta** **`src`** , selecciona **Copy path** . Guarda esa ruta para el paso 5. 

3. **Enciende la máquina del equipo.** En **Compute** , abre `clase14-equipo8` , pulsa **Start** si está detenida y espera a que diga **Running** . Vuelve a `src/00_setup` y selecciona ese mismo cómputo en la barra superior del notebook. 

4. **Ejecuta** **`00_setup` una vez.** Pulsa **Run all** . La celda del World Port Index debe mostrar **`Puertos 3807`** y la última celda debe terminar correctamente. Si falta el CSV del WPI, el paso 2 del README indica subirlo al volume `ref/`. 

# **2. Configurar el job y procesar los datos** 

5. **Abre** **`05_oceanwatch` y conéctalo a** **`clase14-equipo8` .** La primera vez, ejecuta **solo las tres primeras celdas de código** , una por una con ▶. La segunda contiene `%run ./_config` y la tercera crea el selector `modo` . Cuando aparezcan los controles, elige **`modo = configurar_job`** , pega en **`ruta_src`** la ruta copiada en el paso 2 y pulsa **Run all** . Así el job compartido apunta a los notebooks que acabas de importar. 

6. **Ejecuta el primer día.** Abre **Jobs & Pipelines → Orquestracion OceanWatch** . Junto a **Run now** , pulsa la flecha y elige **Run now with different settings** o **Run now with different parameters** . Deja seleccionadas las cinco tareas, escribe **`2023-06-07`** en `fecha` y pulsa **Run** . Espera a que las cinco terminen en **Succeeded** . 

7. **Repite para los otros dos días.** Lanza **`2023-06-08`** , espera a que termine y luego lanza **`2023-06-09`** . El job ejecuta automáticamente `01 → 02 → 03 → 04 → 05` para cada fecha. El orden proporciona historia al modelo antes de evaluar el último día. 

# **3. Abrir el tablero y verificar la entrega** 

8. **Publica el punto 4.** En **SQL Warehouses** , abre el warehouse que usa tu equipo. En **Connection details → HTTP path** , copia el identificador que aparece después de `/warehouses/` . Vuelve a `05_oceanwatch` , selecciona **`modo = tablero`** , escribe **`fecha = 2023-06-09`** , pega el ID en `warehouse_id` y la ruta de `dashboards/OceanWatch_Institucional.lvdash.json` en `ruta_dashboard` . Pulsa **Run all** . Abre el enlace **`Tablero publicado`** y selecciona **9 de junio de 2023** también en el filtro del tablero. 

9. **Comprueba que las cifras cuadran.** En el mismo notebook, cambia a **`modo = verificar`** , conserva **`fecha = 2023-06-09`** y pulsa **Run all** . Debe aparecer **`plata = posiciones en trayectorias = tráfico`** . El resultado queda guardado en `oceanwatch_data/evidencias/2023-06-09/verificacion.json` . 

10. **Revisa gobierno y ML.** Ejecuta **`modo = gobernanza`** con el nombre del grupo lector acordado por el equipo. En **Catalog** , abre una tabla de oro y revisa **Lineage** . Para ML, abre el experimento `/Shared/OceanWatch_eq8_velocidad` y el modelo `oceanwatch_detector_velocidad` en el esquema `default` . Para acreditar la programación, guarda además un run exitoso disparado por el calendario, identificado como **`PERIODIC`** . 

11. **Ejecuta el bono desde Free Edition.** En el workspace institucional, corre 05 con **`modo = compartir`** y **`fecha = 2023-06-09`** . Luego sigue el **paso 7 del README** en el segundo workspace con `free/06_consumo_free.ipynb` . La confirmación que debes obtener es **`Las cuatro tablas coinciden entre los dos workspaces`** , junto con el enlace al tablero publicado desde Free. 

