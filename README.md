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

## Estructura

```
config.json               resolución, umbrales, criterio cinemático y rutas
src/common.py             config, rutas, logging, saneamiento de tipos
src/download.py           descarga Norder=4/Dir=0 a raw/ (solo requests)
src/stage.py              unificación a Parquet de 12 columnas en HDFS (YARN)
src/pipeline.py           (pendiente) celdas, umbrales, cinemática, candidatos
src/figures.py            (pendiente) mapas y diagramas
scripts/yarn_env.sh       flags de spark-submit validados en el clúster
scripts/stage_data.sh     preflight + descarga + unificación + HDFS
scripts/run_experiments.sh (pendiente) 4 configuraciones y tiempos
docs/hallazgos.md         bitácora de hallazgos con evidencia y pendientes
results/                  resúmenes JSON, tablas CSV y tiempos
raw/                      parquet crudos (no versionado)
```

## Cómo reproducir

```bash
# 1. staging (nodo borde, ~25 min; usar screen/tmux)
screen -S gaia
bash scripts/stage_data.sh 2>&1 | tee results/stage_run.log
# Ctrl-A D para salir sin matar la sesión; screen -r gaia para volver

# 2. análisis (pendiente)
bash scripts/run_experiments.sh
```

Variables de entorno útiles: `GAIA_RAW_DIR` (dónde bajar los crudos; por defecto `raw/`),
`GAIA_MASTER` (por defecto `yarn`; `local[6]` es el plan B sin YARN),
`GAIA_NUM_EXECUTORS`, `GAIA_EXEC_MEMORY`.

Transporte entre la máquina local y el clúster (el cluster solo escribe en `results/`):

```bash
rsync -av --delete --exclude .git --exclude raw --exclude results \
  -e 'ssh -p 220' ./ uhadoop@cm.dcc.uchile.cl:~/gaia-cc5212/

rsync -av -e 'ssh -p 220' \
  uhadoop@cm.dcc.uchile.cl:~/gaia-cc5212/results/ ./results/
```

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
- **Sin UDFs de Python** y `collect()` solo sobre tablas ya agregadas (celdas,
  candidatos, tiempos).

## Estado

- [x] Staging (2026-11-14): descarga, PUT a HDFS con replicación 1, unificación
      distribuida en YARN con `source_tile` y los 3 errores astrométricos, contabilidad
      de nulos/NaN y test de duplicados y celdas por baldosa. Resultado en
      `results/stage_summary.json`; lectura en `docs/hallazgos.md`.
- [ ] Pipeline: celdas por resolución, umbrales μ+3σ y percentil 99, criterio
      cinemático relativo, fusión de celdas contiguas.
- [ ] Experimentos: 4 configuraciones × mitigación de skew, con tiempos.
- [ ] Figuras: densidad con `LogNorm`, `pmra` vs `pmdec`, color–magnitud.
- [ ] Validación: M7, SIMBAD, Hunt & Reffert (2023).

## Limitaciones (van al informe)

- La muestra son las baldosas de orden 4 del subconjunto BP/RP (mayormente G ≲ 17,6),
  no todo Gaia ni todo el cielo; las regiones densas del plano galáctico están
  subrepresentadas por construcción de HATS.
- Binning rectangular en RA/Dec: las celdas tienen área distinta (factor `cos dec`).
- El criterio cinemático absoluto depende de la distancia; por eso el pipeline usa
  un criterio relativo a las celdas vecinas más coherencia de paralaje.
- Los candidatos no están confirmados como cúmulos ligados.
- Con ~1,4 M filas Spark no es estrictamente necesario: su valor es que el mismo
  código escala al catálogo Gaia completo. Los tiempos por configuración y por
  número de ejecutores son la evidencia.
