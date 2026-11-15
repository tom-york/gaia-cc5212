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
from common import hdfs_uri, load_config, log, repo_root, results_path  # noqa: E402


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
    if len(a) > p["max_stars_per_tile"]:
        a = a[np.argsort(a[:, 6])[:p["max_stars_per_tile"]]]
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


# ----------------------------- PIPELINE --------------------------------------
def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="DBSCAN 5D por baldosa HEALPix")
    ap.add_argument("--config", default=os.path.join(repo_root(), "config.json"),
                    help="ruta al config.json")
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    cfg = load_config(args.config)
    p = dict(cfg["clusters"]["dbscan"])
    p["min_plx_snr"] = float(cfg["clusters"]["min_plx_snr"])
    p["max_gmag"] = float(cfg["clusters"]["max_gmag"])

    path_in = hdfs_uri(cfg, "merged")
    path_out = hdfs_uri(cfg, "clusters_dbscan")
    csv_out = results_path(cfg, "clusters", "clusters_dbscan.csv")

    t0 = time.time()
    spark = SparkSession.builder.appName("gaia_clusters_dbscan").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    log("entrada: {}".format(path_in))
    log("parámetros: " + ", ".join(
        "{}={}".format(k, v) for k, v in p.items() if not isinstance(v, dict)))

    df = spark.read.parquet(path_in)
    print("\nFilas totales:", df.count())

    base = (
        df.dropna(subset=["object_id", "ra", "dec", "parallax", "parallax_error",
                          "pmra", "pmdec", "phot_g_mean_mag", "source_tile"])
          .filter((F.col("parallax") > 0)
                  & (F.col("parallax") / F.col("parallax_error") >= p["min_plx_snr"])
                  & (F.col("phot_g_mean_mag") <= p["max_gmag"]))
          .select("source_tile", "object_id", "ra", "dec", "parallax",
                  "pmra", "pmdec", "phot_g_mean_mag")
    )
    print("Filas tras filtro de calidad:", base.count())

    rows = base.rdd.map(lambda r: (int(r[0]), tuple(float(v) for v in r[1:])))
    found = (rows.groupByKey(p["num_partitions"])
                 .flatMap(lambda kv: find_clusters(kv[0], list(kv[1]), p))
                 .collect())
    print("Candidatos por baldosa (antes de fusionar):", len(found))

    final = merge_clusters(found, p)
    print("Clusters finales:", len(final))

    cols = ["cluster_id", "n_members", "ra", "dec", "parallax", "dist_pc",
            "pmra", "pmdec", "sd_parallax", "sd_pmra", "sd_pmdec", "radius_deg"]

    if final:
        print("\n=== CLUSTERS ENCONTRADOS ===")
        print(f"{'cluster':>8} {'n':>5} {'ra':>9} {'dec':>9} {'plx':>7} "
              f"{'dist_pc':>8} {'pmra':>8} {'pmdec':>8} {'r_deg':>6}")
        for r in final[:30]:
            print(f"{'cluster ' + str(r[0]):>8} {r[1]:>5} {r[2]:>9.3f} {r[3]:>9.3f} "
                  f"{r[4]:>7.3f} {r[5]:>8.1f} {r[6]:>8.2f} {r[7]:>8.2f} {r[11]:>6.2f}")
        if len(final) > 30:
            print(f"... ({len(final) - 30} más en {csv_out})")

        os.makedirs(os.path.dirname(csv_out), exist_ok=True)
        with open(csv_out, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
            w.writerows(final)
        print("\nCSV local:", csv_out)

        schema = ("cluster_id int, n_members int, ra double, dec double, parallax double, "
                  "dist_pc double, pmra double, pmdec double, sd_parallax double, "
                  "sd_pmra double, sd_pmdec double, radius_deg double")
        tabla = spark.createDataFrame(final, schema)
        tabla.show(10, truncate=False)
        tabla.write.mode("overwrite").parquet(path_out)
        print("Parquet en HDFS:", path_out)
    else:
        print("No se encontraron clusters: prueba bajar min_pts/min_members o subir eps.")

    print(f"\nTiempo total: {time.time() - t0:.1f} s")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
