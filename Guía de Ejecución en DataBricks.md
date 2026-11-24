# **1. Importar el proyecto y preparar el entorno** 

1. **Entra al Databricks de la universidad.** Abre **Workspace → Users → tu correo** . En el menú ⋮ , elige **Import** , selecciona el ZIP completo y confirma. Databricks lo descomprime al importarlo. 

2. **Ubica la carpeta** **`src` .** Dentro de `OceanWatch_Equipo8` , abre `src` y comprueba que ves `_config` , `00_setup` , `01_ingesta` , `02_bronze` , `03_silver` , `04_gold` y `05_ml` . En el menú de **la carpeta** **`src`** , selecciona **Copy path** . Guarda esa ruta para el paso 5. 

3. **Enciende la máquina del equipo.** En **Compute** , abre `clase14-equipo8` , pulsa **Start** si está detenida y espera a que diga **Running** . Vuelve a `src/00_setup` y selecciona ese mismo cómputo en la barra superior del notebook. 

4. **Ejecuta** **`00_setup` una vez.** Pulsa **Run all** . La celda del World Port Index debe mostrar **`Puertos 3807`** y la última celda debe terminar correctamente. Si falta el CSV del WPI, el paso 2 del README indica subirlo al volume `ref/`. 

# **2. Configurar el job y procesar los datos** 

5. **Arma el job en la interfaz.** Abre **Jobs & Pipelines → Orquestracion OceanWatch** . El cómputo de cada tarea es `clase14-equipo8` . Agrega, en orden, `01_ingesta` , `02_bronze` , `03_silver` , `04_gold` y `05_ml` . Cada una depende de la anterior. En las cinco, `fecha` = `{{job.parameters.fecha}}` , `ancla_replay` = `{{job.parameters.ancla_replay}}` e `inicio_run` = `{{job.start_time.iso_date}}` . `05_ml` no lleva modo. Parámetros del job: `fecha=AUTO` y `ancla_replay` con el día en que empieza el replay. Dos reintentos por tarea, 60 segundos de espera, una ejecución a la vez. Horario `0 0 6 * * ?` en `America/Bogota` , activo. Correo al fallar en **Notifications** . Con ancla `2026-11-24` y el job arrancando ese día, la fecha AIS es `2023-06-08`. Al día siguiente es `2023-06-09`. Un `fecha` explícito ignora la ancla. 

6. **Ejecuta el primer día.** Abre **Jobs & Pipelines → Orquestracion OceanWatch** . Junto a **Run now** , pulsa la flecha y elige **Run now with different settings** o **Run now with different parameters** . Deja seleccionadas las cinco tareas, escribe **`2023-06-07`** en `fecha` y pulsa **Run** . Espera a que las cinco terminen en **Succeeded** . 

7. **Repite para los otros dos días.** Lanza **`2023-06-08`** , espera a que termine y luego lanza **`2023-06-09`** . El job ejecuta automáticamente `01 → 02 → 03 → 04 → 05` para cada fecha. El orden proporciona historia al modelo antes de evaluar el último día. 

# **3. Abrir el tablero y revisar la entrega** 

8. **Publica el punto 4.** Abre **Dashboards** , elige **Import** y selecciona `dashboards/OceanWatch_Institucional.lvdash.json` . Asigna el SQL warehouse y pulsa **Publish** . Abre el tablero y selecciona **9 de junio de 2023** en **Día AIS · UTC** . 

9. **Comprueba que las cifras cuadran.** En Catalog, el conteo de `oceanwatch_silver_ais` para ese día debe coincidir con la suma de `n_posiciones` en trayectorias y en tráfico. 

10. **Revisa gobierno y ML.** Los comentarios de oro salen de la celda correspondiente en `00_setup`. En **Catalog** , abre una tabla de oro y revisa **Lineage** . Para ML, abre el experimento `/Shared/OceanWatch_eq8_velocidad` y el modelo `oceanwatch_detector_velocidad` en el esquema `default` . El primer día en oro no crea modelo. Hace falta un segundo día. Para acreditar la programación, guarda además un run exitoso disparado por el calendario, identificado como **`PERIODIC`** . 

11. **Ejecuta el bono desde Free Edition.** En el workspace institucional, corre `src/06_compartir` con **`fecha = 2023-06-09`** . Si `mine-group8` no puede crear shares, la celda termina en `PERMISSION_DENIED` y el bono queda pendiente de un administrador. Si el share ya existe, sigue el **paso 7 del README** en el segundo workspace con `free/06_consumo_free.ipynb` . La confirmación que debes obtener es **`Las cuatro tablas coinciden entre los dos workspaces`** , junto con el enlace al tablero publicado desde Free. 

