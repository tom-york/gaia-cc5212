#!/usr/bin/env python3
"""Unifica la partición Norder=4/Dir=0 en un único Parquet de 12 columnas.

Hace, en este orden:
  1. lee los 263 parquet crudos desde HDFS (o desde disco local, `--raw`);
  2. conserva las 8 columnas de la propuesta, agrega los 3 errores
     astrométricos (`parallax_error`, `pmra_error`, `pmdec_error`) y
     `source_tile`, extraído del nombre de archivo (`Npix=N.parquet`);
  3. cuenta nulos y NaN por columna ANTES de limpiar (en Spark `isNull()`
     no ve los NaN y `NaN > 0` es verdadero: sin este paso, un filtro
     `parallax > 0` deja pasar basura);
  4. convierte NaN -> null en las columnas float y normaliza `ra >= 360`
     a `ra - 360`;
  5. escribe con repartition(parts) y replicación 1 en HDFS;
  6. relee el resultado y deja un JSON con conteos, duplicados de
     `object_id`, cobertura y el test de celdas por baldosa (el punto 6.3
     del plan: de qué baldosa salen las celdas más densas).

Uso:
    spark-submit src/stage.py --config config.json [--raw-uri hdfs://...] [--parts 32]
    spark-submit src/stage.py --config config.json --print-schema   # diagnóstico

Ojo: el Parquet trae structs (`astrometry`, `photometry`) y no nombres planos
con punto; las columnas se resuelven con `field()` (getField).
"""

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import dump_json, hdfs_uri, load_config, log, plain, resolve, results_path  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.utils import AnalysisException  # noqa: E402

# Columnas de la propuesta + errores astrométricos. El nombre a la izquierda es
# el del dataset (columnas anidadas aplanadas con punto) y a la derecha el alias
# corto. Los errores habilitan el corte de señal/ruido y comparar la dispersión
# observada con la esperada por errores formales.
COLS = {
    "object_id": "object_id",
    "ra": "ra",
    "dec": "dec",
    "astrometry.parallax": "parallax",
    "astrometry.parallax_error": "parallax_error",
    "astrometry.pmra": "pmra",
    "astrometry.pmra_error": "pmra_error",
    "astrometry.pmdec": "pmdec",
    "astrometry.pmdec_error": "pmdec_error",
    "photometry.phot_g_mean_mag": "phot_g_mean_mag",
    "photometry.bp_rp": "bp_rp",
}
KEEP = list(COLS.values())
OUT_COLS = KEEP + ["source_tile"]


def field(path):
    """Accede a un campo del Parquet crudo, incluyendo structs anidados.

    El dataset trae structs (`astrometry`, `photometry`) en vez de nombres
    planos con punto, que es como los muestra pandas. Se usa `getField` en
    lugar de `col("astrometry.parallax")` para que el error, si el campo no
    existe, sea explícito.
    """
    parts = path.split(".")
    col = F.col(parts[0])
    for part in parts[1:]:
        col = col.getField(part)
    return col


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--raw-uri", help="ruta HDFS de los parquet crudos; sobreescribe config")
    ap.add_argument("--parts", type=int, help="particiones de salida (default: config)")
    ap.add_argument("--print-schema", action="store_true",
                    help="imprime el esquema crudo y termina (diagnóstico)")
    args = ap.parse_args()

    cfg = load_config(args.config)
    raw_dir = args.raw_uri or os.environ.get("GAIA_RAW_DIR") or resolve(cfg["local"]["raw"])
    log("origen de los crudos: {}".format(raw_dir))
    out_uri = hdfs_uri(cfg, "merged")
    results_dir = results_path(cfg, "staging")
    parts = args.parts or cfg["spark"]["parts"]
    res = float(cfg["binning"]["verify_res_deg"])

    t_start = time.perf_counter()
    spark = (
        SparkSession.builder.appName("gaia_stage")
        .config("spark.sql.shuffle.partitions", cfg["spark"]["shuffle_partitions"])
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log("spark {} | master {}".format(spark.version, spark.sparkContext.master))

    # ---------------------------------------------------------------- lectura
    raw = spark.read.parquet(raw_dir)
    log("leídos {} archivos, {} columnas de primer nivel".format(
        len(raw.inputFiles()), len(raw.columns)))

    if args.print_schema:
        raw.printSchema()
        log("columnas de primer nivel: {}".format(sorted(raw.columns)))
        return 0

    # Los nombres del dataset son rutas dentro de structs: se valida el primer
    # nivel y se resuelve el resto con getField (ver `field`).
    missing = sorted({src.split(".")[0] for src in COLS} - set(raw.columns))
    if missing:
        raise SystemExit(
            "faltan structs/columnas en el parquet crudo: {}\n"
            "columnas de primer nivel: {}\n"
            "corre --print-schema para ver la estructura completa".format(
                missing, sorted(raw.columns)
            )
        )

    try:
        base = raw.select(
            *[field(src).alias(dst) for src, dst in COLS.items()],
            F.regexp_extract(F.input_file_name(), r"Npix=(\d+)", 1)
            .cast("int").alias("source_tile"),
        )
    except AnalysisException as exc:
        raise SystemExit(
            "no pude resolver las columnas pedidas dentro del esquema: {}\n"
            "corre --print-schema para ver la estructura completa".format(exc)
        )

    dtypes = dict(base.dtypes)
    float_cols = [c for c in KEEP if dtypes[c] in ("float", "double")]

    # ------------------------------------------- contabilidad previa a limpiar
    aggs = [F.count("*").alias("rows"), F.countDistinct("source_tile").alias("n_tiles")]
    for col in KEEP:
        aggs.append(F.sum(F.col(col).isNull().cast("int")).alias("null__" + col))
    for col in float_cols:
        aggs.append(F.sum(F.isnan(F.col(col)).cast("int")).alias("nan__" + col))
    stats_in = base.agg(*aggs).first().asDict()
    log("entrada: {} filas, {} baldosas".format(stats_in["rows"], stats_in["n_tiles"]))
    log("nulos: {}".format({k[6:]: v for k, v in stats_in.items() if k.startswith("null__")}))
    log("NaN:   {}".format({k[5:]: v for k, v in stats_in.items() if k.startswith("nan__")}))

    # ------------------------------------------------- limpieza y normalización
    clean = base.select(
        *[
            F.when(F.isnan(c), None).otherwise(F.col(c)).alias(c) if c in float_cols
            else F.col(c).alias(c)
            for c in OUT_COLS
        ]
    )
    n_wrap = clean.filter(F.col("ra") >= 360.0).count()
    if n_wrap:
        clean = clean.withColumn(
            "ra", F.when(F.col("ra") >= 360.0, F.col("ra") - 360.0).otherwise(F.col("ra"))
        )
        log("normalizadas {} filas con ra >= 360".format(n_wrap))

    # ---------------------------------------------------------------- escritura
    t_before_write = time.perf_counter()
    log("escribiendo {} con repartition({})".format(out_uri, parts))
    clean.repartition(parts).write.mode("overwrite").parquet(out_uri)
    t_after_write = time.perf_counter()

    # -------------------------------------------------------------- verificación
    m = spark.read.parquet(out_uri)
    rows = m.count()
    distinct = m.select("object_id").distinct().count()
    ra = m.agg(F.min("ra").alias("min"), F.max("ra").alias("max")).first().asDict()

    tiles_top = [
        plain(r.asDict())
        for r in m.groupBy("source_tile")
        .agg(F.count("*").alias("rows"), F.countDistinct("object_id").alias("uniq"))
        .orderBy(F.desc("rows"))
        .limit(10)
        .collect()
    ]

    ok = m.filter(
        F.col("parallax").isNotNull() & (F.col("parallax") > 0)
        & F.col("pmra").isNotNull() & F.col("pmdec").isNotNull()
        & F.col("ra").isNotNull() & F.col("dec").isNotNull()
    )
    cells = (
        ok.withColumn("ci", F.floor(F.col("ra") / res).cast("long"))
        .withColumn("cj", F.floor(F.col("dec") / res).cast("long"))
        .groupBy("source_tile", "ci", "cj")
        .count()
    )
    filtered = cells.agg(F.sum("count")).first()[0] or 0
    n_cells = cells.count()
    top_cells = [
        plain(r.asDict()) for r in cells.orderBy(F.desc("count")).limit(10).collect()
    ]
    per_tile = cells.groupBy("source_tile").agg(
        F.sum("count").alias("rows"), F.max("count").alias("max_cell")
    )
    tiles_by_max = [
        plain(r.asDict())
        for r in per_tile.orderBy(F.desc("max_cell")).limit(10).collect()
    ]

    # Calidad astrométrica: cuántas estrellas sobreviven un corte de señal/ruido
    # en paralaje y cómo se distribuye parallax/parallax_error.
    snr_df = ok.filter(
        F.col("parallax_error").isNotNull() & (F.col("parallax_error") > 0)
    ).select((F.col("parallax") / F.col("parallax_error")).alias("snr"),
             F.col("parallax_error"))
    quality = snr_df.agg(
        F.count("*").alias("rows_with_error"),
        F.sum((F.col("snr") >= 5).cast("int")).alias("rows_snr_ge_5"),
        F.sum((F.col("snr") >= 10).cast("int")).alias("rows_snr_ge_10"),
        F.avg("parallax_error").alias("mean_parallax_error"),
    ).first().asDict()
    quality["snr_percentiles"] = dict(
        zip(["p10", "p50", "p90"], snr_df.approxQuantile("snr", [0.1, 0.5, 0.9], 0.01))
    )

    summary = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "spark_version": spark.version,
        "master": spark.sparkContext.master,
        "dataset": cfg["dataset"],
        "schema": {c: dtypes[c] for c in KEEP},
        "float_columns": float_cols,
        "input": stats_in,
        "output": {
            "uri": out_uri,
            "parts": parts,
            "rows": rows,
            "distinct_object_id": distinct,
            "duplicated_rows": rows - distinct,
            "ra_min": ra["min"],
            "ra_max": ra["max"],
            "ra_wrapped": n_wrap,
        },
        "filter": {
            "rule": "ra, dec, pmra, pmdec no nulos; parallax no nulo y > 0 (NaN ya es null)",
            "rows_after_filter": filtered,
            "rows_dropped": rows - filtered,
        },
        "cells": {
            "res_deg": res,
            "n_cells": n_cells,
            "top_by_count": top_cells,
            "tiles_by_max_cell": tiles_by_max,
        },
        "tiles": {"n": stats_in["n_tiles"], "top_by_rows": tiles_top},
        "quality": quality,
        "timings_s": {
            "read_and_clean": round(t_before_write - t_start, 1),
            "write": round(t_after_write - t_before_write, 1),
            "verify": round(time.perf_counter() - t_after_write, 1),
            "total": round(time.perf_counter() - t_start, 1),
        },
    }
    dump_json(summary, os.path.join(results_dir, "stage_summary.json"))

    log("filas escritas={}  distintos(object_id)={}  duplicadas={}".format(
        rows, distinct, rows - distinct))
    log("tras filtro={} (descartadas {}), celdas de {}°={}".format(
        filtered, rows - filtered, res, n_cells))
    if top_cells:
        top = top_cells[0]
        log("celda más densa: tile={} ci={} cj={} count={}".format(
            top["source_tile"], top["ci"], top["cj"], top["count"]))
    log("calidad: {} filas con parallax_error, SNR>=5: {}, p50(SNR)={}".format(
        quality["rows_with_error"], quality["rows_snr_ge_5"],
        round(quality["snr_percentiles"]["p50"], 2)))
    log("SUMMARY_OK {}".format(os.path.join(results_dir, "stage_summary.json")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
