# Candidatos a cúmulos estelares con Gaia DR3 y PySpark

Grupo 3 — CC5212 Procesamiento Masivo de Datos, U. de Chile.

Identificación de **candidatos** a cúmulos estelares como sobredensidades espaciales
con movimiento propio y paralaje coherentes, sobre la partición `Norder=4/Dir=0` del
dataset `UniverseTBD/mmu_gaia_gaia`. El término *candidato* es deliberado: una
sobredensidad cinemáticamente coherente no prueba que el sistema esté ligado.

Entrega: informe de 5–6 páginas, presentación de 8 minutos y este repositorio.

## Datos

| Elemento | Valor |
|---|---|
| Dataset | `UniverseTBD/mmu_gaia_gaia` (Hugging Face) |
| Partición | `Norder=4/Dir=0`: 263 archivos Parquet |
| Tamaño | 1.800.261.960 B ≈ 1,68 GiB (verificado contra la API de HF) |
| Filas | **1.336.195** medidas, con `object_id` único (0 duplicadas); ver `docs/hallazgos.md` H-02 |
| Columnas usadas | `object_id`, `ra`, `dec`, `astrometry.parallax`, `astrometry.parallax_error`, `astrometry.pmra`, `astrometry.pmra_error`, `astrometry.pmdec`, `astrometry.pmdec_error`, `photometry.phot_g_mean_mag`, `photometry.bp_rp` |
| Columna derivada | `source_tile` = `Npix` del nombre de archivo (baldosa HEALPix de origen) |

El Parquet unificado tiene **12 columnas**. Los tres errores astrométricos no estaban en la
propuesta original pero son baratos (mismos archivos, ~17 MB extra) y habilitan dos cosas:
un corte de señal/ruido en paralaje (`parallax/parallax_error`) y comparar la dispersión
observada de una celda con la esperada por errores formales, que es la forma honesta de
decidir si una celda "viaja junta" o solo está lejos.

`Norder` es el orden HEALPix de la baldosa a la que corresponde cada archivo: a mayor
orden, baldosa más pequeña. `Dir` es solo una carpeta para agrupar archivos.

## Hallazgos

Lo verificado sobre el dataset y el clúster vive en [`docs/hallazgos.md`](docs/hallazgos.md),
con el número medido, su origen y su implicancia para el informe. Titulares:

- 1.336.195 filas, cero duplicados; el conteo previo (1.411.004) no se reproduce (H-02, H-11).
- 7.161 filas tienen `NaN` —no `null`— en los seis campos astrométricos, y `NaN > 0`
  es verdadero en Spark: sin filtrar `isnan()` se envenenan las agregaciones (H-03).
- El máximo real por celda de 0,25° es 847 estrellas, no las 5.738 del análisis previo (H-05).
- La partición cubre 8,6 % del cielo, no todo el cielo (H-06).
- Dos detectores independientes dan 16 y 3 candidatos; sus dispersiones internas
  (2,3–15,2 km/s) indican asociaciones, no cúmulos ligados (H-13, H-14).
- Con `eps = 0,8` los mismos datos dan 4 núcleos fríos (1,3–3,8 km/s) inmersos en esas
  asociaciones: hay estructura ligada, pero es minoritaria (H-19).

## Entorno del clúster

| Elemento | Valor |
|---|---|
| Nodo borde | `cluster-01`, 8 núcleos, 21 GB RAM |
| HDFS | `hdfs://cm:9000`, ~912 GB libres |
| YARN | 5 nodos, 5.120 MB / 5 vCores cada uno (25 GB / 25 vCores) |
| Spark | 3.3.2 (Scala 2.12) |
| Hadoop | 2.10.0 |
| Java | 11.0.26 |
| Python | 3.10.20 (`/usr/local/bin/python3`) |
| Librerías | `pyarrow`, `numpy`, `matplotlib`, `healpy`, `astropy`, `fsspec`, `requests` |
| Logs YARN | **agregación deshabilitada** → diagnóstico por stdout en `--deploy-mode client` |
| Raíz HDFS del proyecto | `/data/2026/uhadoop/teatime/gaia` |

**Análisis complementario local:** los scripts de comparación y visualización del catálogo utilizan `pandas`, `numpy`, `astropy`, `matplotlib` y `jinja2`. Las herramientas de validación de cobertura utilizan además `healpy`. Estas dependencias deben estar instaladas en el entorno Python local desde el que se ejecutan los scripts.

## Estructura

```text
config.json               parámetros y rutas (única fuente de verdad)
src/common.py             config, rutas, logging, saneamiento de tipos
src/download.py           descarga Norder=4/Dir=0 a raw/ (solo requests)
src/stage.py              unificación a Parquet de 12 columnas en HDFS (YARN)
src/clusters_dbscan.py    DBSCAN 5D por baldosa (numpy en los ejecutores)
src/clusters_celdas.py    celdas de área ~constante + pico cinemático (Spark SQL)
src/figures.py            figuras principales del informe desde las tablas
scripts/yarn_env.sh       flags de spark-submit validados en el clúster
scripts/stage_data.sh     preflight + descarga + unificación + HDFS
scripts/run_clusters.sh   corre cualquiera de los dos detectores (--method)
scripts/make_pylibs.sh    genera pylibs.zip con numpy (no versionado)
scripts/inspect_schema.py imprime el esquema del Parquet unificado
scripts/run_experiments.sh barrido de configuraciones con tiempos
scripts/compare_clusters.py compara dos tablas por conjunto (no por orden)
scripts/crossmatch_catalogo.py compara candidatos con Hunt & Reffert (2023)
scripts/fig_comparacion_catalogo.py visualización del crossmatch
scripts/fig_comparacion_posiciones.py distribución de candidatos y cúmulos conocidos
scripts/revisar_cobertura.py inspección de la cobertura HEALPix descargada
scripts/validar_footprint.py valida la posición de los candidatos respecto al footprint
tests/test_determinism.py regresión: la salida no depende del orden de entrada
tests/pyspark_stub/       stub mínimo de pyspark para correr el test sin Spark
docs/hallazgos.md         bitácora de hallazgos con evidencia y pendientes
docs/catalogo_hunt_reffert.csv catálogo de cúmulos de Hunt & Reffert (2023)
results/staging/          resúmenes y evidencia del staging
results/clusters/         tablas de candidatos, una por método
results/experiments/      configs, timings y una carpeta por variante
results/figures/          figuras principales y complementarias
results/comparacion_catalogo.csv resultados detallados del crossmatch
results/tabla_crossmatch.csv tabla resumida del crossmatch
results/tabla_crossmatch.tex tabla LaTeX del crossmatch
assets/eso0932a.jpg       imágenes auxiliares panorámica ESO
raw/                      parquet crudos (no versionado)
```

## Cómo reproducir

```bash
# 1. staging (nodo borde, ~25 min; usar screen/tmux)
screen -S gaia
mkdir -p results/staging
bash scripts/stage_data.sh 2>&1 | tee results/staging/stage_run.log
# Ctrl-A D para salir sin matar la sesión; screen -r gaia para volver

# 2. detección de candidatos
bash scripts/run_clusters.sh --method celdas   # Spark SQL puro
bash scripts/run_clusters.sh --method dbscan   # DBSCAN 5D (numpy en ejecutores)

# 3. experimentos y tiempos (6 variantes, ~20 min)
bash scripts/run_experiments.sh

# 4. dejar las tablas canónicas al día (el barrido escribe en su propia carpeta)
bash scripts/run_clusters.sh --method dbscan
bash scripts/run_clusters.sh --method celdas
```

Cada corrida deja su log en `results/clusters/clusters_<método>_run.log` (no versionado),
su tabla en `results/clusters/clusters_<método>.csv` y el embudo en
`results/clusters/run_summary_<método>.json`.

Con `--out DIR --tag NOMBRE` las salidas locales van a `DIR` y los parquet a
`gaia_clusters_NOMBRE.parquet`, `gaia_clusters_celdas_NOMBRE.parquet` y
`gaia_cluster_members_celdas_NOMBRE.parquet`. Es lo que usa el barrido para que dos
variantes no se pisen ni en disco ni en HDFS.

DBSCAN necesita numpy en los contenedores de YARN. Si no lo ven, `scripts/make_pylibs.sh`
genera `pylibs/pylibs.zip` (numpy con `numpy/` en la raíz, de modo que
`--archives pylibs.zip#pylibs` + `PYTHONPATH=./pylibs` lo resuelva) y
`scripts/run_clusters.sh` lo agrega solo cuando el archivo existe. El método por celdas
no requiere ninguna librería extra.

Variables de entorno útiles: `GAIA_RAW_DIR` (dónde bajar los crudos; por defecto `raw/`),
`GAIA_MASTER` (por defecto `yarn`; `local[6]` es el plan B sin YARN),
`GAIA_NUM_EXECUTORS`, `GAIA_EXEC_MEMORY`.

Transporte entre la máquina local y el clúster. El repositorio vive en
`~/teatime/gaia/gaia-cc5212` en el nodo borde (el mismo prefijo que la raíz HDFS) y el
clúster solo escribe en `results/`:

```bash
rsync -av --delete --exclude .git --exclude raw --exclude results \
  --exclude libs --exclude pylibs \
  -e 'ssh -p 220' ./ uhadoop@cm.dcc.uchile.cl:~/teatime/gaia/gaia-cc5212/

rsync -av -e 'ssh -p 220' \
  uhadoop@cm.dcc.uchile.cl:~/teatime/gaia/gaia-cc5212/results/ ./results/
```

`libs/` y `pylibs/` se excluyen porque se generan en el nodo borde con
`scripts/make_pylibs.sh`; no se versionan.

## Experimentos

El barrido mide dos cosas: **costo** (tiempo de pared) y **sensibilidad** (cuántos
candidatos aparecen) al mover parámetros que hoy están fijos en `config.json`. Cada
variante se materializa como un `config.json` propio en `results/experiments/configs/`,
así que la corrida queda auditable sin tocar el config del repositorio.

| Eje | Valores de la matriz por defecto |
|---|---|
| método | `dbscan` y `celdas` (una corrida base por método) |
| `clusters.dbscan.eps` | 1,0 (base), 0,8, 1,2 |
| `clusters.celdas.kin_rel` | 2,0 (base), 1,5 |
| `clusters.min_plx_snr` | 10 (base), 5 |

Fijo en todas: `max_gmag = 18`, 8 ejecutores, `GAIA_MASTER=yarn`.

```bash
bash scripts/run_experiments.sh --dry-run      # imprime la matriz, no ejecuta
bash scripts/run_experiments.sh                # corre las 6 variantes
bash scripts/run_experiments.sh --only celdas  # solo un método
bash scripts/run_experiments.sh --tag eps_1.2  # una sola variante
```

Cada corrida agrega un registro a `results/experiments/timings.json`:

| Campo | Qué es |
|---|---|
| `wall_s` | punta a punta del `spark-submit`, incluye el arranque en YARN |
| `spark_s` | el `Tiempo total` que imprime el script (incluye crear la sesión, no el lanzador) |
| `tag`, `method`, `overrides`, `rc` | trazabilidad: qué config se usó y cómo terminó |

`wall_s − spark_s` es solo el lanzador (bash + JVM de `spark-submit` + cierre): **8–17 s**.
No es el arranque de la aplicación, porque el cronómetro del script arranca antes de crear
la sesión y `spark_s` ya lo incluye. El arranque en sí (asignación de ejecutores en YARN) es
la parte más variable: 48–56 s en la sesión del 22-nov, cuando el clúster estaba cargado.

Cada variante escribe en su propia carpeta, así que nada se pisa:

```
results/experiments/<tag>/clusters_<método>.csv
results/experiments/<tag>/clusters_<método>_run.log
results/experiments/<tag>/run_summary_<método>.json
results/experiments/<tag>/cells_<res>_hist.json     (solo el método por celdas)
HDFS: gaia_clusters_<tag>.parquet, gaia_clusters_celdas_<tag>.parquet, ...
```

La corrida sin `--tag` escribe en `results/clusters/`, que es la tabla que cita el informe.

**Cuidado con los rótulos.** El plan de Spark es perezoso: la lectura del Parquet ocurre
dentro del primer job que la necesita, no donde el código la declara (H-09). Los tiempos por
etapa que imprimen los scripts son de los jobs de Spark, no de las líneas de Python.

## Figuras

El módulo `src/figures.py` genera las ocho figuras principales del proyecto a partir de los resultados tabulares y archivos auxiliares versionados en el repositorio. No requiere ejecutar Spark ni volver a procesar los datos Parquet originales.

Cada figura registra su descripción y fuentes de datos en `results/figures/figures.json`. La figura de contexto galáctico utiliza adicionalmente la panorámica de ESO (`assets/eso0932a.jpg`) y el archivo de cobertura Gaia (`results/staging/files.tsv`).

Para generar todas las figuras:

```bash
python3 src/figures.py
```

También es posible generar figuras específicas:

```bash
python3 src/figures.py --only eps,cielo
```

Las figuras generadas son:

| Archivo | Descripción |
|---|---|
| `fig_01_embudo.png` | Resumen del proceso de selección de datos. |
| `fig_02_cielo.png` | Distribución espacial de los candidatos. |
| `fig_03_astrometria.png` | Distribución de parámetros astrométricos. |
| `fig_04_hr.png` | Diagrama color-magnitud de los candidatos. |
| `fig_05_multiplicidad.png` | Análisis de multiplicidad de candidatos. |
| `fig_06_experimentos.png` | Comparación de los experimentos realizados. |
| `fig_07_tiempos.png` | Comparación de tiempos de ejecución. |
| `fig_08_via_lactea.png` | Candidatos y cobertura Gaia sobre una panorámica de la Vía Láctea. |

### Análisis complementario: catálogo de Hunt & Reffert (2023)

Se incluyen scripts independientes para comparar los candidatos detectados con el catálogo de cúmulos estelares de Hunt & Reffert (2023).

```bash
python3 scripts/crossmatch_catalogo.py
python3 scripts/fig_comparacion_catalogo.py
python3 scripts/fig_comparacion_posiciones.py
```

Estos scripts generan tablas de comparación y las siguientes visualizaciones:

| Archivo | Descripción |
|---|---|
| `fig_09_comparacion_catalogo.png` | Comparación de los candidatos con sus vecinos más cercanos del catálogo. |
| `fig_10_comparacion_posiciones.png` | Distribución galáctica de los candidatos y los cúmulos catalogados cercanos. |

Estas visualizaciones son complementarias y se generan de forma independiente de `src/figures.py`. La proximidad angular con un cúmulo catalogado no implica necesariamente que exista una asociación física.

## Decisiones de diseño

- **NaN no es null.** Los faltantes astrométricos vienen como `NaN` y Spark los deja
  pasar por `isNotNull()` y por `parallax > 0`. `src/stage.py` cuenta nulos y NaN por
  columna antes de limpiar y convierte `NaN → null` en las columnas float (magnitud del
  problema en H-03: 7.161 filas).
- **`source_tile`.** Se conserva la baldosa de origen de cada estrella para poder
  auditar las celdas más densas (¿cuántas filas tiene esa baldosa?) y detectar
  duplicados entre archivos.
- **Staging distribuido.** El nodo borde solo descarga (red); los 263 crudos se suben
  a HDFS con replicación 1 (1,68 GiB sobre ~912 GB libres, 0,2 %) y la unificación
  corre en YARN leyendo desde HDFS. Al ser archivos de ~7 MB, Spark los empaqueta por
  `maxPartitionBytes` (128 MB) en ~14 particiones de lectura, no en 263 tareas
  diminutas: es la medición que desarma el argumento del *Small Files Problem* que la
  propuesta original usaba para justificar `Norder=4`. `local[6]` queda como plan B y
  lee del mismo `raw` en HDFS.
- **YARN: memoria virtual.** El smoke test moría con `exit 143` / `-103` por
  `vmem-pmem-ratio ≈ 2,1` sobre el ApplicationMaster. Solución validada (y adoptada
  en `scripts/yarn_env.sh`): 1536m de memoria por executor y por AM, 512 de overhead,
  `-XX:MaxMetaspaceSize=256m -XX:ReservedCodeCacheSize=128m -Xss512k`.
  No era el conector Cassandra; igual se usa `--conf spark.jars.packages=` vacío.
- **Structs, no nombres planos.** El Parquet trae `astrometry`, `photometry` y
  `spectral_coefficients` como structs anidados; pandas los aplana como
  `astrometry.parallax`, Spark no. `src/stage.py` los resuelve con `getField` y
  ofrece `--print-schema` para inspeccionar la estructura sin correr el pipeline.
- **`config.json`, no YAML.** PyYAML no está garantizado en el clúster.
- **Ruta HDFS corregida.** El plan original usaba `/uhadoop2026/projects/grupo3`;
  la raíz real del proyecto es `/data/2026/uhadoop/teatime/gaia`.
- **Dos métodos, un mismo Parquet.** DBSCAN 5D no impone una grilla, pero es O(n²) por
  baldosa; las celdas son O(n) y escalan mejor, pero dependen de umbrales. Se reportan
  ambos y se comparan entre sí.
- **Spark SQL salvo donde no alcanza.** El método por celdas es Spark SQL puro, sin UDFs.
  DBSCAN corre en numpy sobre las estrellas agrupadas por baldosa: una excepción
  deliberada, porque una UDF por fila sería órdenes de magnitud más lenta. `collect()`
  solo se usa sobre tablas ya agregadas (celdas, candidatos, tiempos).

## Estado

- [x] Staging (2026-11-14): descarga, PUT a HDFS con replicación 1, unificación
      distribuida en YARN con `source_tile` y los 3 errores astrométricos, contabilidad
      de nulos/NaN y test de duplicados y celdas por baldosa. Resultado en
      `results/staging/stage_summary.json`; lectura en `docs/hallazgos.md`.
- [x] Detección de candidatos (2026-11-15): dos métodos independientes sobre el mismo
      Parquet — DBSCAN 5D por baldosa (`src/clusters_dbscan.py`) y celdas de área
      ~constante con pico cinemático (`src/clusters_celdas.py`). Tablas en
      `results/clusters/clusters_dbscan.csv` y `results/clusters/clusters_celdas.csv`.
- [x] Experimentos (2026-11-22): 6 variantes con `rc = 0`, cada una en su carpeta, tiempos
      en `results/experiments/timings.json`. Sensibilidad de `eps` (4 / 16 / 24 candidatos),
      de `kin_rel` (3 → 4) y del SNR (3 → 6) en H-19, H-16 y H-20.
- [x] Reproducibilidad (2026-11-22): orden de entrada fijo en DBSCAN; las dos corridas
      canónicas son idénticas byte a byte y `tests/test_determinism.py` lo verifica sin
      Spark (H-22).
- [x] Banda de paralaje calculada sobre los miembros del grupo, no sobre el campo (H-23).
- [x] Figuras (2026-11-22): 8 figuras principales generadas mediante
      `src/figures.py`, incluyendo la distribución de candidatos y la
      cobertura Gaia sobre la panorámica de la Vía Láctea. Se incorpora
      además un análisis complementario del catálogo Hunt & Reffert (2023),
      con dos visualizaciones y tablas de comparación.
- [x] Comparación con catálogo externo (2026-11-22): crossmatch posicional y astrométrico con Hunt & Reffert (2023). Se calcularon separaciones angulares y diferencias de paralaje y movimiento propio para los candidatos de ambos métodos.
- [ ] Validación adicional: contraste con M7 y SIMBAD; evaluación de posibles asociaciones físicas con cúmulos catalogados.

## Limitaciones (van al informe)

- La muestra son las baldosas de orden 4 del subconjunto BP/RP (mayormente G ≲ 17,6),
  no todo Gaia ni todo el cielo; las regiones densas del plano galáctico están
  subrepresentadas por construcción de HATS.
- Las celdas tienen área ~constante (~1 deg²) por construcción, pero cerca de los polos
  son tiras muy alargadas en RA: el "vecino" de una celda no es un vecino físico
  simétrico, y la fusión de celdas contiguas lo hereda.
- El umbral de densidad es un cuantil fijo (percentil 99 ≈ 1 % de las celdas no vacías),
  no un contraste adaptativo: el número de candidatos depende de la resolución (H-21).
- `eps = 1,0` no detecta el grupo ligado de 237 pc ni los 4 núcleos fríos que aparecen con
  `eps = 0,8`, y `eps = 1,2` funde cuatro poblaciones en un solo grupo de 569 miembros
  (H-19). El valor elegido es un compromiso declarado, no un óptimo.
- El filtro `SNR ≥ 10` descarta el 32 % de las filas; con `SNR ≥ 5` los dos métodos
  dejan de coincidir en paralaje (H-20).
- El criterio cinemático absoluto (dispersión de movimiento propio < 3 mas/año) depende
  de la distancia y del entorno; el método por celdas usa en su lugar un pico cinemático
  relativo a la mediana de las celdas densas.
- Los candidatos no están confirmados como cúmulos ligados.
- Con ~1,4 M filas Spark no es estrictamente necesario: su valor es que el mismo
  código escala al catálogo Gaia completo. Los tiempos por configuración y por
  número de ejecutores son la evidencia.
