# clusters_dbscan.py
# Detección híbrida de candidatos a clusters estelares:
#   Spark  -> filtro de calidad + reparto por baldosa HEALPix (source_tile)
#   numpy  -> DBSCAN en 5D dentro de cada baldosa (sin pandas ni sklearn)
#   driver -> fusión de duplicados en bordes + tabla final
#
# Parámetros en config.json (sección "clusters"). Uso:
#   spark-submit src/clusters_dbscan.py [--config config.json]
import argparse
import csv
import math
import os
import sys
import time

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import dump_json, hdfs_uri, load_config, log, repo_root, results_path, tagged  # noqa: E402


# ----------------------------- DBSCAN (numpy) --------------------------------
def dbscan(X, eps, min_pts, chunk=500):
    n = len(X)
    X = X - X.mean(axis=0)
    sq = (X * X).sum(axis=1)
    eps2 = eps * eps
    counts = np.zeros(n, dtype=np.int64)
    neigh = {}
    for i in range(0, n, chunk):
        blk = X[i:i + chunk]
        d2 = sq[i:i + chunk, None] + sq[None, :] - 2.0 * (blk @ X.T)
        mask = d2 <= eps2
        c = mask.sum(axis=1)
        counts[i:i + chunk] = c
        for k in np.flatnonzero(c >= min_pts):
            neigh[i + k] = np.flatnonzero(mask[k])
    core = counts >= min_pts
    labels = -np.ones(n, dtype=np.int64)
    cid = 0
    for i in range(n):
        if labels[i] != -1 or not core[i]:
            continue
        labels[i] = cid
        stack = [i]
        while stack:
            j = stack.pop()
            if not core[j]:
                continue
            for k in neigh[j]:
                if labels[k] == -1:
                    labels[k] = cid
                    stack.append(k)
        cid += 1
    return labels


def find_clusters(tile, pts, p):
    """Corre en los ejecutores: recibe todas las estrellas de una baldosa."""
    a = np.array(pts, dtype=np.float64)  # cols: oid, ra, dec, plx, pmra, pmdec, g
    # Orden determinista (H-22): groupByKey no fija el orden de las filas dentro de
    # una baldosa, y de ese orden dependen los promedios, el recorte por magnitud y
    # el flood-fill de DBSCAN. Sin fijarlo, dos corridas con la misma config
    # devuelven grupos distintos.
    a = a[np.lexsort((a[:, 0], a[:, 5], a[:, 4], a[:, 3], a[:, 2], a[:, 1]))]
    if len(a) > p["max_stars_per_tile"]:
        a = a[np.argsort(a[:, 6], kind="stable")[:p["max_stars_per_tile"]]]
    if len(a) < p["min_members"]:
        return []

    ra, dec = a[:, 1], a[:, 2]
    rar = np.radians(ra)
    ra0 = math.degrees(math.atan2(np.sin(rar).mean(), np.cos(rar).mean()))
    dec0 = dec.mean()
    dra = ((ra - ra0 + 180.0) % 360.0) - 180.0
    x = dra * np.cos(np.radians(dec))
    y = dec - dec0

    X = np.column_stack([
        x / p["scale_pos"], y / p["scale_pos"],
        a[:, 3] / p["scale_plx"], a[:, 4] / p["scale_pm"], a[:, 5] / p["scale_pm"],
    ])
    labels = dbscan(X, p["eps"], p["min_pts"])

    out = []
    for c in range(labels.max() + 1):
        m = labels == c
        n = int(m.sum())
        if n < p["min_members"]:
            continue
        rad = float(np.median(np.hypot(x[m] - x[m].mean(), y[m] - y[m].mean())))
        out.append((
            int(tile), n,
            float((ra0 + dra[m].mean()) % 360.0), float(dec[m].mean()),
            float(a[m, 3].mean()), float(a[m, 4].mean()), float(a[m, 5].mean()),
            float(a[m, 3].std()), float(a[m, 4].std()), float(a[m, 5].std()),
            rad,
        ))
    return out


# ----------------------------- FUSIÓN EN DRIVER ------------------------------
def merge_clusters(cl, p):
    # El orden de llegada desde flatMap tampoco es estable: se fija para que los
    # promedios ponderados y la numeración de cluster_id sean reproducibles.
    cl = sorted(cl, key=lambda c: (c[0], c[2], c[3], c[4], c[5], c[6]))
    n = len(cl)
    if n == 0:
        return []
    ra = np.array([c[2] for c in cl]); dec = np.array([c[3] for c in cl])
    plx = np.array([c[4] for c in cl])
    pmra = np.array([c[5] for c in cl]); pmdec = np.array([c[6] for c in cl])

    parent = list(range(n))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    for i in range(n - 1):
        j = np.arange(i + 1, n)
        dra = ((ra[j] - ra[i] + 180.0) % 360.0) - 180.0
        sep = np.hypot(dra * math.cos(math.radians(dec[i])), dec[j] - dec[i])
        ok = ((sep < p["merge"]["pos_deg"])
              & (np.abs(plx[j] - plx[i]) < p["merge"]["plx"])
              & (np.hypot(pmra[j] - pmra[i], pmdec[j] - pmdec[i]) < p["merge"]["pm"]))
        for k in j[ok]:
            parent[find(int(k))] = find(i)

    groups = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(cl[i])

    merged = []
    for g in groups.values():
        w = np.array([c[1] for c in g], dtype=float)
        W = w.sum()
        wm = lambda idx: float((w * np.array([c[idx] for c in g])).sum() / W)
        rr = np.radians([c[2] for c in g])
        ra_m = math.degrees(math.atan2((w * np.sin(rr)).sum(), (w * np.cos(rr)).sum())) % 360.0
        merged.append((
            int(W), ra_m, wm(3), wm(4), wm(5), wm(6),
            wm(7), wm(8), wm(9), max(c[10] for c in g),
        ))
    merged.sort(key=lambda r: -r[0])
    # cluster_id, n, ra, dec, plx, dist_pc, pmra, pmdec, sd_plx, sd_pmra, sd_pmdec, radio
    return [(i + 1, r[0], r[1], r[2], r[3], 1000.0 / r[3], r[4], r[5],
             r[6], r[7], r[8], r[9]) for i, r in enumerate(merged)]


def add_parallax_band(tabla, base):
    """Recalcula la paralaje como mediana con banda p16/p84 sobre las estrellas del grupo.

    El promedio de paralaje no es el promedio de las distancias (H-17), así que se
    reporta la mediana y su banda. La selección es la misma que definió al grupo:
    dentro de 2 radios, 3 sigma de paralaje y 3 sigma de movimiento propio.
    """
    c = (tabla.select(
        "cluster_id",
        F.col("ra").alias("c_ra"), F.col("dec").alias("c_dec"),
        F.col("parallax").alias("c_plx"),
        F.greatest(F.col("sd_parallax"), F.lit(0.01)).alias("c_sdp"),
        F.col("pmra").alias("c_pmra"), F.col("pmdec").alias("c_pmdec"),
        F.greatest(F.hypot(F.col("sd_pmra"), F.col("sd_pmdec")),
                   F.lit(0.01)).alias("c_sdpm"),
        F.greatest(F.col("radius_deg"), F.lit(0.05)).alias("c_rad")))

    dra = ((F.col("ra") - F.col("c_ra") + 540.0) % 360.0) - 180.0
    sep = F.sqrt(F.pow(dra * F.cos(F.radians(F.col("c_dec"))), 2)
                 + F.pow(F.col("dec") - F.col("c_dec"), 2))
    dpm = F.hypot(F.col("pmra") - F.col("c_pmra"), F.col("pmdec") - F.col("c_pmdec"))

    sel = (base.join(F.broadcast(c), how="inner")
               .filter((sep <= 2.0 * F.col("c_rad"))
                       & (F.abs(F.col("parallax") - F.col("c_plx")) <= 3.0 * F.col("c_sdp"))
                       & (dpm <= 3.0 * F.col("c_sdpm"))))

    q = (sel.groupBy("cluster_id").agg(
        F.count("*").alias("n_ref"),
        F.percentile_approx("parallax", 0.5).alias("plx_med"),
        F.percentile_approx("parallax", 0.16).alias("plx_p16"),
        F.percentile_approx("parallax", 0.84).alias("plx_p84")))
    # La banda se invierte al pasar a distancia: plx_p84 es la distancia menor.
    q = (q.withColumn("dist_med_pc", 1000.0 / F.col("plx_med"))
          .withColumn("dist_p16_pc", 1000.0 / F.col("plx_p84"))
          .withColumn("dist_p84_pc", 1000.0 / F.col("plx_p16")))
    return tabla.join(q, "cluster_id", "left")


# ----------------------------- PIPELINE --------------------------------------
def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="DBSCAN 5D por baldosa HEALPix")
    ap.add_argument("--config", default=os.path.join(repo_root(), "config.json"),
                    help="ruta al config.json")
    ap.add_argument("--out", default=None,
                    help="carpeta local de salida (por defecto results/clusters)")
    ap.add_argument("--tag", default="",
                    help="sufijo de la variante: se agrega al parquet en HDFS")
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    cfg = load_config(args.config)
    p = dict(cfg["clusters"]["dbscan"])
    p["min_plx_snr"] = float(cfg["clusters"]["min_plx_snr"])
    p["max_gmag"] = float(cfg["clusters"]["max_gmag"])

    out_dir = args.out or results_path(cfg, "clusters")
    path_in = hdfs_uri(cfg, "merged")
    path_out = tagged(hdfs_uri(cfg, "clusters_dbscan"), args.tag)
    csv_out = os.path.join(out_dir, "clusters_dbscan.csv")
    summary_out = os.path.join(out_dir, "run_summary_dbscan.json")

    t0 = time.time()
    app = "gaia_clusters_dbscan" + ("_" + args.tag if args.tag else "")
    spark = SparkSession.builder.appName(app).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    log("entrada: {}".format(path_in))
    log("salidas: csv={} parquet={}".format(csv_out, path_out))
    log("parámetros: " + ", ".join(
        "{}={}".format(k, v) for k, v in p.items() if not isinstance(v, dict)))

    df = spark.read.parquet(path_in)
    n_total = df.count()
    print("\nFilas totales:", n_total)

    base = (
        df.dropna(subset=["object_id", "ra", "dec", "parallax", "parallax_error",
                          "pmra", "pmdec", "phot_g_mean_mag", "source_tile"])
          .filter((F.col("parallax") > 0)
                  & (F.col("parallax") / F.col("parallax_error") >= p["min_plx_snr"])
                  & (F.col("phot_g_mean_mag") <= p["max_gmag"]))
          .select("source_tile", "object_id", "ra", "dec", "parallax",
                  "pmra", "pmdec", "phot_g_mean_mag")
    )
    n_filtered = base.count()
    print("Filas tras filtro de calidad:", n_filtered)

    rows = base.rdd.map(lambda r: (int(r[0]), tuple(float(v) for v in r[1:])))
    found = (rows.groupByKey(p["num_partitions"])
                 .flatMap(lambda kv: find_clusters(kv[0], list(kv[1]), p))
                 .collect())
    print("Candidatos por baldosa (antes de fusionar):", len(found))

    final = merge_clusters(found, p)
    print("Clusters finales:", len(final))

    cols = ["cluster_id", "n_members", "ra", "dec", "parallax", "dist_pc",
            "pmra", "pmdec", "sd_parallax", "sd_pmra", "sd_pmdec", "radius_deg",
            "plx_med", "plx_p16", "plx_p84",
            "dist_med_pc", "dist_p16_pc", "dist_p84_pc", "n_ref"]

    if final:
        print("\n=== CLUSTERS ENCONTRADOS ===")
        print(f"{'cluster':>8} {'n':>5} {'ra':>9} {'dec':>9} {'plx':>7} "
              f"{'dist_pc':>8} {'pmra':>8} {'pmdec':>8} {'r_deg':>6}")
        for r in final[:30]:
            print(f"{'cluster ' + str(r[0]):>8} {r[1]:>5} {r[2]:>9.3f} {r[3]:>9.3f} "
                  f"{r[4]:>7.3f} {r[5]:>8.1f} {r[6]:>8.2f} {r[7]:>8.2f} {r[11]:>6.2f}")
        if len(final) > 30:
            print(f"... ({len(final) - 30} más en {csv_out})")

        schema = ("cluster_id int, n_members int, ra double, dec double, parallax double, "
                  "dist_pc double, pmra double, pmdec double, sd_parallax double, "
                  "sd_pmra double, sd_pmdec double, radius_deg double")
        tabla = add_parallax_band(spark.createDataFrame(final, schema), base).persist()
        tabla.show(10, truncate=False)

        os.makedirs(out_dir, exist_ok=True)
        with open(csv_out, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows([[r[c] for c in cols]
                         for r in tabla.orderBy("cluster_id").collect()])
        print("\nCSV local:", csv_out)

        tabla.write.mode("overwrite").parquet(path_out)
        print("Parquet en HDFS:", path_out)
        tabla.unpersist()
    else:
        print("No se encontraron clusters: prueba bajar min_pts/min_members o subir eps.")

    elapsed = time.time() - t0
    dump_json({
        "method": "dbscan",
        "tag": args.tag,
        "config": os.path.abspath(args.config),
        "rows_total": n_total,
        "rows_after_filter": n_filtered,
        "candidates_pre_merge": len(found),
        "clusters_final": len(final),
        "params": {k: v for k, v in p.items() if not isinstance(v, dict)},
        "seconds": round(elapsed, 1),
        "outputs": {"csv": csv_out, "parquet": path_out},
    }, summary_out)

    print(f"\nTiempo total: {elapsed:.1f} s")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
