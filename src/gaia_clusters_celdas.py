# gaia_clusters_celdas.py
# Detección de candidatos a clusters estelares por CELDAS (todo en Spark SQL,
# no necesita numpy ni otras librerías en los ejecutores):
#   1. Grilla del cielo de área ~constante a varias resoluciones.
#   2. Celdas densas: conteo >= max(mu + 3*sigma, percentil 99) de los conteos.
#   3. Criterio cinemático relativo: dentro de cada celda densa se busca el
#      "pico" en (paralaje, pmra, pmdec); se acepta si la fracción de estrellas
#      en ese pico supera KIN_REL veces la mediana de las celdas densas.
#   4. Fusión de celdas contiguas con cinemática parecida -> un cluster.
#   5. Eliminación de duplicados entre resoluciones y tabla final.
import csv
import math
import os
import time

from pyspark.sql import SparkSession, Window, functions as F

# ----------------------------- PARÁMETROS ------------------------------------
BASE = "hdfs://cm:9000/data/2026/uhadoop/teatime/gaia"
PATH_IN = f"{BASE}/gaia_mmu_merged.parquet"
PATH_OUT = f"{BASE}/gaia_clusters_celdas.parquet"
PATH_MEMBERS = f"{BASE}/gaia_cluster_members_celdas.parquet"
CSV_OUT = "results/clusters_celdas.csv"   # relativo a donde ejecutes (raíz del repo)
WRITE_MEMBERS = True                      # guarda object_id -> cluster_id en HDFS

# Filtro de calidad (igual que en la versión DBSCAN)
MIN_PLX_SNR = 10.0
MAX_GMAG = 18.0

# Resolución de las celdas (grados de lado, aprox.)
RESOLUTIONS = [0.25, 0.5, 1.0]

# Celdas densas
MIN_CELL_STARS = 15     # piso absoluto de estrellas por celda
SIGMA_K = 3.0           # umbral mu + K*sigma
PERCENTILE = 0.99       # umbral percentil

# Pico cinemático dentro de la celda
BIN_PLX = 0.3           # mas
BIN_PM = 1.0            # mas/año
MIN_PEAK = 10           # estrellas mínimas en el pico
KIN_REL = 2.0           # fracción del pico >= KIN_REL * mediana de celdas densas

# Fusión de celdas contiguas y duplicados entre resoluciones
MERGE_PLX = 0.3
MERGE_PM = 1.0
MIN_MEMBERS = 15


# ----------------------------- UTILIDADES ------------------------------------
def bands_for(d):
    """(j, nra): bandas de declinación y nº de celdas en ra por banda (área ~cte)."""
    nb = int(round(180.0 / d))
    out = []
    for j in range(nb):
        dec_c = -90.0 + (j + 0.5) * d
        nra = max(1, int(math.floor(360.0 * math.cos(math.radians(dec_c)) / d + 0.5)))
        out.append((j, nra))
    return out


def add_cell(spark, df, d):
    bands = bands_for(d)
    nb = len(bands)
    bdf = spark.createDataFrame(bands, "j int, nra int")
    df = df.withColumn(
        "j", F.least(F.floor((F.col("dec") + 90.0) / d), F.lit(nb - 1)).cast("int"))
    df = df.join(F.broadcast(bdf), "j")
    df = df.withColumn(
        "i", F.least(F.floor(F.col("ra") / 360.0 * F.col("nra")),
                     F.col("nra") - 1).cast("int"))
    return df.drop("nra"), dict(bands)


def neighbors(j, i, nra_of):
    n0 = nra_of[j]
    out = [(j, (i + 1) % n0), (j, (i - 1) % n0)]
    for jj in (j - 1, j + 1):
        if jj in nra_of:
            n1 = nra_of[jj]
            lo = int(math.floor(i / n0 * n1))
            hi = int(math.floor((i + 1) / n0 * n1 - 1e-9))
            for k in range(lo - 1, hi + 2):
                out.append((jj, k % n1))
    return out


def merge_cells(cells, nra_of):
    """cells: lista de (j, i, plx, pmra, pmdec). Devuelve {(j,i): cid} por componentes."""
    idx = {(c[0], c[1]): n for n, c in enumerate(cells)}
    parent = list(range(len(cells)))

    def find(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]
            a = parent[a]
        return a

    for n, c in enumerate(cells):
        for nb in neighbors(c[0], c[1], nra_of):
            m = idx.get(nb)
            if m is None or m == n:
                continue
            o = cells[m]
            if (abs(c[2] - o[2]) < MERGE_PLX
                    and math.hypot(c[3] - o[3], c[4] - o[4]) < MERGE_PM):
                parent[find(m)] = find(n)
    roots = {}
    out = {}
    for n, c in enumerate(cells):
        r = find(n)
        out[(c[0], c[1])] = roots.setdefault(r, len(roots) + 1)
    return out


def circ_sep(ra1, dec1, ra2, dec2):
    dra = ((ra2 - ra1 + 180.0) % 360.0) - 180.0
    return math.hypot(dra * math.cos(math.radians(dec1)), dec2 - dec1)


# ----------------------------- PIPELINE --------------------------------------
if __name__ == "__main__":
    t0 = time.time()
    spark = SparkSession.builder.appName("gaia_clusters_celdas").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(PATH_IN)
    print("\nFilas totales:", df.count())

    base = (
        df.dropna(subset=["object_id", "ra", "dec", "parallax", "parallax_error",
                          "pmra", "pmdec", "phot_g_mean_mag"])
          .filter((df.parallax > 0)
                  & (df.parallax / df.parallax_error >= MIN_PLX_SNR)
                  & (df.phot_g_mean_mag <= MAX_GMAG))
          .select("object_id", "ra", "dec", "parallax", "pmra", "pmdec", "phot_g_mean_mag")
          .withColumn("bp", F.floor(F.col("parallax") / BIN_PLX).cast("int"))
          .withColumn("br", F.floor(F.col("pmra") / BIN_PM).cast("int"))
          .withColumn("bd", F.floor(F.col("pmdec") / BIN_PM).cast("int"))
          .persist()
    )
    print("Filas tras filtro de calidad:", base.count())

    all_rows = []        # (res, cid_local, n_cells, n, ra, dec, plx, pmra, pmdec, sdp, sdr, sdd, rad)
    members_dfs = []

    for d in RESOLUTIONS:
        t1 = time.time()
        print(f"\n--- Resolución {d}° ---")
        stars, nra_of = add_cell(spark, base, d)

        # 1-2. Conteo por celda y umbrales
        cells = stars.groupBy("j", "i").agg(F.count("*").alias("n"))
        st = cells.agg(F.count("*").alias("nc"), F.mean("n").alias("mu"),
                       F.stddev("n").alias("sd"),
                       F.percentile_approx("n", PERCENTILE).alias("pc")).first()
        mu, sd, pc = float(st["mu"]), float(st["sd"] or 0.0), float(st["pc"])
        thr = max(mu + SIGMA_K * sd, pc, float(MIN_CELL_STARS))
        print(f"celdas no vacías: {st['nc']} | mu={mu:.1f} sigma={sd:.1f} "
              f"p{int(PERCENTILE * 100)}={pc:.0f} -> umbral={thr:.1f}")
        dense = cells.filter(F.col("n") >= thr)
        n_dense = dense.count()
        print("celdas densas:", n_dense)
        if n_dense == 0:
            continue

        # 3. Pico cinemático por celda densa
        kb = (stars.join(F.broadcast(dense), ["j", "i"])
                   .groupBy("j", "i", "n", "bp", "br", "bd")
                   .agg(F.count("*").alias("k")))
        w = Window.partitionBy("j", "i").orderBy(F.desc("k"))
        peak = (kb.withColumn("rk", F.row_number().over(w)).filter("rk = 1")
                  .drop("rk").withColumn("frac", F.col("k") / F.col("n")))
        med = float(peak.agg(F.percentile_approx("frac", 0.5)).first()[0])
        flagged = peak.filter((F.col("k") >= MIN_PEAK) & (F.col("frac") >= KIN_REL * med))
        flagged = flagged.select("j", "i", "bp", "br", "bd").persist()
        n_flag = flagged.count()
        print(f"fracción mediana del pico: {med:.3f} | celdas con pico cinemático: {n_flag}")
        if n_flag == 0:
            continue

        pm_stars = (stars.join(F.broadcast(flagged), ["j", "i", "bp", "br", "bd"])
                         .select("j", "i", "object_id", "ra", "dec", "parallax",
                                 "pmra", "pmdec", "phot_g_mean_mag").persist())
        cs = (pm_stars.groupBy("j", "i")
                      .agg(F.avg("parallax"), F.avg("pmra"), F.avg("pmdec")).collect())
        cell_list = [(r[0], r[1], r[2], r[3], r[4]) for r in cs]

        # 4. Fusión de celdas contiguas
        cmap = merge_cells(cell_list, nra_of)
        ncell_of = {}
        for cid in cmap.values():
            ncell_of[cid] = ncell_of.get(cid, 0) + 1
        cmap_df = spark.createDataFrame([(j, i, c) for (j, i), c in cmap.items()],
                                        "j int, i int, cid int")
        members = pm_stars.join(F.broadcast(cmap_df), ["j", "i"])

        # Propiedades por cluster
        cen = (members.groupBy("cid").agg(
            F.count("*").alias("n"),
            F.degrees(F.atan2(F.avg(F.sin(F.radians("ra"))),
                              F.avg(F.cos(F.radians("ra"))))).alias("ra_raw"),
            F.avg("dec").alias("dec_c"), F.avg("parallax").alias("plx"),
            F.avg("pmra").alias("pmra_m"), F.avg("pmdec").alias("pmdec_m"),
            F.stddev("parallax").alias("sdp"), F.stddev("pmra").alias("sdr"),
            F.stddev("pmdec").alias("sdd"))
            .withColumn("ra_c", (F.col("ra_raw") + 360.0) % 360.0)
            .filter(F.col("n") >= MIN_MEMBERS))
        cen = cen.persist()
        rad = (members.join(F.broadcast(cen.select("cid", "ra_c", "dec_c")), "cid")
                      .withColumn("dra", ((F.col("ra") - F.col("ra_c") + 540.0) % 360.0) - 180.0)
                      .withColumn("r", F.sqrt(
                          F.pow(F.col("dra") * F.cos(F.radians("dec")), 2)
                          + F.pow(F.col("dec") - F.col("dec_c"), 2)))
                      .groupBy("cid").agg(F.percentile_approx("r", 0.5).alias("rad")))
        res = cen.join(rad, "cid").collect()
        for r in res:
            all_rows.append((d, int(r["cid"]), ncell_of[int(r["cid"])], int(r["n"]),
                             float(r["ra_c"]), float(r["dec_c"]), float(r["plx"]),
                             float(r["pmra_m"]), float(r["pmdec_m"]),
                             float(r["sdp"] or 0), float(r["sdr"] or 0),
                             float(r["sdd"] or 0), float(r["rad"])))
        print(f"clusters en esta resolución: {len(res)} | tiempo: {time.time() - t1:.1f} s")
        if WRITE_MEMBERS:
            members_dfs.append(members.select(
                F.lit(d).alias("res"), "cid", "object_id", "ra", "dec",
                "parallax", "pmra", "pmdec", "phot_g_mean_mag"))

    # 5. Duplicados entre resoluciones (se conserva el de más miembros)
    all_rows.sort(key=lambda r: -r[3])
    kept = []
    for r in all_rows:
        dup = False
        for k in kept:
            if (circ_sep(k[4], k[5], r[4], r[5]) < max(k[0], r[0])
                    and abs(k[6] - r[6]) < MERGE_PLX
                    and math.hypot(k[7] - r[7], k[8] - r[8]) < MERGE_PM):
                dup = True
                break
        if not dup:
            kept.append(r)

    final = []
    for n, r in enumerate(kept, 1):
        # cluster_id, resolution, n_cells, n_members, ra, dec, plx, dist_pc, pmra, pmdec, sds, radio
        final.append((n, r[0], r[2], r[3], r[4], r[5], r[6], 1000.0 / r[6], r[7], r[8],
                      r[9], r[10], r[11], r[12]))
    print(f"\nCandidatos totales: {len(all_rows)} | tras quitar duplicados: {len(final)}")

    cols = ["cluster_id", "resolution_deg", "n_cells", "n_members", "ra", "dec",
            "parallax", "dist_pc", "pmra", "pmdec", "sd_parallax", "sd_pmra",
            "sd_pmdec", "radius_deg"]
    if final:
        print("\n=== CLUSTERS ENCONTRADOS ===")
        print(f"{'cluster':>10} {'res':>5} {'celdas':>6} {'n':>5} {'ra':>9} {'dec':>9} "
              f"{'plx':>7} {'dist_pc':>8} {'pmra':>8} {'pmdec':>8} {'r_deg':>6}")
        for r in final[:30]:
            print(f"{'cluster ' + str(r[0]):>10} {r[1]:>5} {r[2]:>6} {r[3]:>5} {r[4]:>9.3f} "
                  f"{r[5]:>9.3f} {r[6]:>7.3f} {r[7]:>8.1f} {r[8]:>8.2f} {r[9]:>8.2f} {r[13]:>6.2f}")
        if len(final) > 30:
            print(f"... ({len(final) - 30} más en {CSV_OUT})")

        os.makedirs(os.path.dirname(CSV_OUT), exist_ok=True)
        with open(CSV_OUT, "w", newline="") as f:
            wr = csv.writer(f)
            wr.writerow(cols)
            wr.writerows(final)
        print("\nCSV local:", CSV_OUT)

        schema = ("cluster_id int, resolution_deg double, n_cells int, n_members int, "
                  "ra double, dec double, parallax double, dist_pc double, pmra double, "
                  "pmdec double, sd_parallax double, sd_pmra double, sd_pmdec double, "
                  "radius_deg double")
        tabla = spark.createDataFrame(final, schema)
        tabla.show(10, truncate=False)
        tabla.write.mode("overwrite").parquet(PATH_OUT)
        print("Parquet en HDFS:", PATH_OUT)

        if WRITE_MEMBERS and members_dfs:
            mapping = spark.createDataFrame(
                [(float(r[0]), r[1], fid) for fid, r in
                 ((n, kept[n - 1]) for n in range(1, len(kept) + 1))],
                "res double, cid int, cluster_id int")
            allm = members_dfs[0]
            for m in members_dfs[1:]:
                allm = allm.unionByName(m)
            out = (allm.join(F.broadcast(mapping), ["res", "cid"])
                       .select("cluster_id", "object_id", "ra", "dec", "parallax",
                               "pmra", "pmdec", "phot_g_mean_mag"))
            out.write.mode("overwrite").parquet(PATH_MEMBERS)
            print("Miembros en HDFS:", PATH_MEMBERS)
    else:
        print("No se encontraron clusters: baja KIN_REL / MIN_PEAK o prueba otras resoluciones.")

    print(f"\nTiempo total: {time.time() - t0:.1f} s")
    spark.stop()
