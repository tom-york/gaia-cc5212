# Registro de hallazgos

Bitácora de lo verificado sobre el dataset, el clúster y el método. Regla de la casa:
**nada entra acá sin un número medido y su origen**. Lo que es hipótesis va marcado como
tal. Cada entrada dice qué implica para el informe.

Fuente de los números de staging: `results/stage_summary.json` (corrida del 2026-11-14
05:19, Spark 3.3.2 sobre YARN, 8 ejecutores).

---

## H-01 · La partición son 263 archivos y 1.800.261.960 B

**Medido.** 263 archivos, 1.800.261.960 B = 1,68 GiB. Verificado contra la API de HF
(`results/files.tsv` guarda ruta y tamaño de cada uno) y contra el PUT en HDFS
(`hdfs dfs -count`: 263 archivos, 1,7 G).

**Implicancia.** El "1,8 GB" de la propuesta es correcto. La afirmación de que
`Norder=9` tiene "cientos de miles de archivos diminutos" sigue sin sustento: se
midieron 435 archivos.

---

## H-02 · La partición tiene 1.336.195 filas y cero duplicados

**Medido.** `rows = 1.336.195`, `distinct(object_id) = 1.336.195`,
`duplicated_rows = 0`. También por baldosa: `rows == uniq` en las 263.

**Implicancia.** Se cae la sospecha de duplicados entre archivos. El número de la
propuesta (1.411.004) no se reproduce: ver H-11. **En el informe va 1.336.195.**

---

## H-03 · NaN ≠ null: 7.161 filas sin astrometría

**Medido.** Conteo de `null` = **0 en las 11 columnas**. Conteo de `NaN`:
`parallax`, `parallax_error`, `pmra`, `pmra_error`, `pmdec`, `pmdec_error` = **7.161**
cada uno (las mismas filas: fuentes sin solución astrométrica). `phot_g_mean_mag` y
`bp_rp` = 7 filas. `ra` y `dec` = 0.

**Por qué importa.** En Spark `isNull()` no ve los NaN y `NaN > 0` es **verdadero**.
Un filtro "sin nulos y parallax > 0" deja pasar las 7.161, y después `avg`, `stddev` y
`1000/parallax` devuelven NaN en silencio.

**Implicancia.** Es el hallazgo de calidad de datos del informe: no es una precaución
teórica, son 7.161 filas medidas. El pipeline normaliza `NaN → null` antes de filtrar y
deja la contabilidad por columna en el JSON.

---

## H-04 · El filtro base descarta 25.474 filas (1,90 %)

**Medido.** Se conservan 1.310.721 filas. Descartes: **7.161** sin astrometría (H-03) +
**18.313** con `parallax ≤ 0`. La suma cierra exacta contra las 1.336.195 de entrada.

**Implicancia.** Da el denominador de todo lo que sigue y permite escribir en el
informe cuánto se pierde por cada criterio, no un total agregado.

---

## H-05 · El máximo real por celda de 0,25° es 847, no 5.738

**Medido.** 54.417 celdas ocupadas de 0,25°, media 24,1 estrellas/celda, máximo **847**
(baldosa 1909, `ci=1207`, `cj=37` → RA ∈ [301,75; 302,00), Dec ∈ [9,25; 9,50)).
Esa baldosa tiene 5.450 filas en total. La segunda región densa es RA ≈ 110°, Dec ≈ −2°
(589). La zona de M7, que dominaba la tabla del análisis previo, no aparece en el top 10.

**Implicancia.** Las 5.738 del análisis previo son 6,8× el máximo medido: el número no
se reproduce y **no va al informe**. Coherente con H-11 (el pipeline previo sobrecontaba).

---

## H-06 · La muestra cubre 8,6 % del cielo, no todo el cielo

**Medido.** 263 baldosas × 13,43 deg² = 3.532 deg² = **8,56 %** de los 41.253 deg² de la
esfera. Ocupación de celdas de 0,25° dentro de esa área: 54.417 de 56.513 = **96,3 %**.

**Implicancia.** Confirma que `Norder=4` es la muestra de las baldosas de orden 4 y no una
cobertura de todo el cielo. El 48 % de cobertura que se atribuía al catálogo completo no
aplica a esta partición.

---

## H-07 · Tope de ~8.192 filas por baldosa

**Medido.** La baldosa más grande tiene **8.172** filas (tile 765) y las diez mayores están
entre 8.059 y 8.172; la media es 5.081.

**Implicancia.** Respalda la hipótesis de un límite de ~8.192 filas por baldosa al
construir la partición, y explica por qué ninguna celda puede acercarse a las 5.738 sin
que el resto de la baldosa quede casi vacío.

---

## H-08 · Calidad astrométrica: 86 % con SNR ≥ 5

**Medido.** Sobre las 1.310.721 filas filtradas: SNR = `parallax/parallax_error` ≥ 5 en
**1.132.223** (86,4 %) y ≥ 10 en **913.727** (69,7 %). Percentiles de SNR: p10 = 3,86,
p50 = **19,12**, p90 = 97,03. `parallax_error` medio = 0,061 mas.

**Implicancia.** Justifica el perfil de filtro `snr5` (pierde 13,6 % de las filas) y
permite comparar la dispersión observada de una celda con la esperada por errores
formales. Sin `parallax_error` cargado nada de esto existiría.

---

## H-09 · El staging distribuido tarda 2,6 min de Spark

**Medido.** 157,9 s de Spark: lectura + agregados de contabilidad 97,3 s, escritura 29,7 s,
verificación 30,9 s. A eso se suman ~40 s de arranque de la aplicación en YARN. Punta a
punta del script: 3 min 8 s. Salida: 32 archivos de parte + `_SUCCESS`.

**Cuidado al citar.** `read_and_clean` mide solo los jobs de contabilidad; la lectura real
del Parquet ocurre dentro de `write` porque el plan es perezoso. No usar esos rótulos como
si fueran etapas físicas separadas.

**Implicancia.** Base de comparación para los tiempos del pipeline con 8 ejecutores.

---

## H-10 · YARN fallaba por memoria virtual, no por el conector Cassandra

**Medido (smoke test previo).** `exit 143` / `exitCode -103` con el diagnóstico
`Container ... is running beyond virtual memory limits` y `vmem-pmem-ratio ≈ 2,1`; la JVM
11 reservaba ~2,2 GB virtuales usando 195 MB reales.

**Solución validada y en uso.** 1536m de memoria por executor y por AM, 512 de overhead,
`-XX:MaxMetaspaceSize=256m -XX:ReservedCodeCacheSize=128m -Xss512k`, y
`--conf spark.jars.packages=` vacío. Con eso corre el staging completo (H-09).

**Implicancia.** La sección de optimización del informe puede contarlo como diagnóstico
real: la causa estaba en la configuración, no en las dependencias.

---

## H-11 · El conteo previo (1.411.004) no se reproduce

**Medido.** Diferencia: +74.809 filas (+5,30 %) respecto de nuestro conteo. La partición no
cambió desde el 2026-01-07 (últimos commits de datos en HF; después solo README), así que
no es una actualización del dataset. Nuestros archivos coinciden byte a byte con la API y
no hay duplicados (H-02).

**Hipótesis (no confirmada).** 74.809 ≈ 14,7 baldosas del tamaño medio (5.081 filas): el
pipeline previo, no versionado, probablemente leyó parquet adicionales desde un directorio
compartido (glob recursivo sobre todo `raw/`).

**Implicancia.** En el informe: usar 1.336.195, declarar la diferencia y no intentar
explicarla más allá de lo que se puede verificar.

---

## H-12 · Las columnas del Parquet son structs, no nombres planos

**Medido.** `raw.columns` lista 12 campos de primer nivel (`astrometry`, `photometry`,
`spectral_coefficients`, …). Los campos internos se acceden con `getField`, no con
`col("astrometry.parallax")` heredado de pandas.

**Implicancia.** Lección de implementación, no del dominio. Queda `--print-schema` en
`src/stage.py` para inspeccionar el esquema sin correr el pipeline.

---

## H-13 · Dos detectores sobre el mismo Parquet dan 16 y 3 candidatos

**Medido.** Mismo filtro de calidad (`parallax > 0`, `parallax/parallax_error ≥ 10`,
`G ≤ 18`) y mismas columnas para los dos métodos.

- DBSCAN 5D por baldosa (`eps = 1,0` en espacio normalizado, `min_pts = 8`,
  `min_members = 15`): **16 candidatos**, de 15 a 245 miembros.
- Celdas de área ~constante con pico cinemático (`k = 3`, p99, `kin_rel = 2,0`,
  resoluciones 0,25/0,5/1,0°): **3 candidatos**, de 21 a 29 miembros.

Origen: `results/clusters_dbscan.csv` y `results/clusters_celdas.csv`.

**Implicancia.** No hay una cifra única de "cúmulos encontrados": el informe presenta los
dos métodos y sus criterios, no un total agregado.

---

## H-14 · Los 16 candidatos de DBSCAN no son cúmulos ligados

**Medido.** Sobre `results/clusters_dbscan.csv`: dispersión interna de movimiento propio
σ_pm = 0,27–1,13 mas/año, que a la paralaje de cada grupo equivale a **2,3–15,1 km/s**
(v = 4,7405·σ_pm/plx); dispersión relativa de paralaje σ_plx/plx = **0,07–0,32**;
distancias 1,6–3,5 kpc.

**Comparación (cita, no medición nuestra).** Un cúmulo abierto ligado tiene dispersión
interna ≲ 1–2 km/s y σ_plx/plx ≲ 0,05 a esas distancias.

**Implicancia.** Lo detectado son asociaciones jóvenes / sobre-densidades del disco, no
cúmulos ligados. Es la conclusión interpretativa del informe y va con estas cifras. El
candidato 1 (245 miembros, 15,1 km/s, σ_plx/plx = 0,32) es el caso más claro.

---

## H-15 · Los dos métodos coinciden en las mismas 3 regiones

**Medido.** Cada uno de los 3 candidatos por celdas tiene un grupo de DBSCAN a
**0,014–0,025°** de distancia angular. Las paralajes, en cambio, no coinciden: en la región
de celdas 1, DBSCAN separa cuatro grupos con plx = 0,388, 0,480, 0,532 y 0,619 mas,
mientras celdas 1 reporta una sola con 0,454 mas. Con `BIN_PLX = 0,3` mas, tres de esos
cuatro grupos caen en el mismo bin.

**Implicancia.** La coincidencia espacial valida la detección (dos métodos independientes
ven lo mismo), pero el método por celdas **promedia estructura en profundidad**: sirve para
decir "hay algo ahí", no para fijar la distancia.

---

## H-16 · El método por celdas depende de la resolución

**Medido.** `results/clusters_celdas.csv`: 1 candidato a 0,5° (`n_cells = 2`), 2 a 1,0°
(`n_cells = 1` cada uno) y **ninguno a 0,25°**. El umbral de densidad es
`max(μ + 3σ, p99, 15)` y el criterio cinemático exige que la fracción del pico supere
`2 × mediana` de las celdas densas.

**Implicancia.** A 1° cada celda abarca ~1 deg², así que el grupo no queda localizado (el
radio reportado se calcula sobre las estrellas del pico, no sobre la celda); a 0,25° el
criterio autorreferente no deja pasar nada. Sin barrido de `kin_rel` no se puede afirmar que
3 sea el número correcto: el informe lo presenta como sensibilidad, no como censo.

---

## H-17 · Las distancias tienen dos sesgos conocidos

**Aritmética sobre valores de literatura (no medición).** Ambos métodos usan
`d = 1000/⟨plx⟩`.

- **Jensen.** `1/⟨plx⟩` no es el promedio de las distancias individuales:
  `E[1/plx] ≥ 1/E[plx]`. Usar la media de plx subestima la distancia media, y es sensible a
  las colas.
- **Punto cero.** El paralaje de DR3 tiene un punto cero de ≈ −0,017 mas. A plx = 0,350 mas
  eso es ~5 % de sobreestimación de la distancia (1000/0,350 = 2.857 pc frente a
  1000/0,367 = 2.725 pc).

**Implicancia.** Citar distancias con banda y decir que son estimaciones, no mediciones;
no usarlas para afirmar pertenencia.

---

## H-18 · numpy en los ejecutores: el archivo sí hace falta

**Medido (2026-11-15).** Una corrida de `run_clusters.sh --method dbscan` sin
`pylibs/pylibs.zip` terminó con `rc = 1` y `ModuleNotFoundError: No module named 'numpy'`
en los workers, tras **92,8 s** de aplicación. El driver sí tiene numpy: el error aparece al
importar en los ejecutores, no al arrancar.

**Corrige la hipótesis anterior.** Los contenedores de YARN no ven el numpy del nodo borde,
así que `pylibs/pylibs.zip` es parte del runtime de DBSCAN, no un adorno. El zip correcto
tiene `numpy/` en la raíz (18 MB, 1.643 entradas), que es exactamente lo que
`PYTHONPATH=./pylibs` necesita; el árbol `pylibs/libs/numpy/` que se había versionado no
calzaba con esa ruta.

**Implicancia.** Se regenera con `scripts/make_pylibs.sh` o se copia por rsync; el método
por celdas no lo necesita. `run_clusters.sh` ahora aborta con código 3 si falta, en vez de
gastar 90 s en un fallo seguro.

---

## Pendientes

- [ ] Volcar la tabla de celdas y los umbrales calculados (hoy se usan y se descartan). Los
      del análisis previo (217,9 y 107,0) salieron de una tabla sobrecontada y no sirven.
- [x] Candidatos detectados por dos métodos independientes (H-13), con coincidencia espacial
      (H-15).
- [ ] Criterio cinemático con errores formales (`T_obs` vs `T_error`, σ_plx/plx ≤ 0,5): **no
      implementado**. Va como limitación del trabajo, no como resultado.
- [ ] Probes de validación: M7 (268,5 / −34,8), Pléyades, Híades, Pesebre.
- [ ] Cruce con SIMBAD y con Hunt & Reffert (2023).
- [ ] Ablación del filtro ingenuo (`isNotNull()` sin `isnan`): cuantificar cuántas celdas
      quedan envenenadas. Evidencia opcional para la rúbrica.
- [x] H-18 verificado: numpy en los ejecutores es necesario; el zip se regenera con
      `scripts/make_pylibs.sh`.
