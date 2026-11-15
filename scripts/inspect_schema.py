# inspect_schema.py — imprime el esquema del Parquet unificado en HDFS.
#
#   python3 scripts/inspect_schema.py [--config config.json]
import argparse
import os
import sys

from pyspark.sql import SparkSession

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))
from common import hdfs_uri, load_config  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description="Esquema del Parquet unificado")
    ap.add_argument("--config", default=os.path.join(ROOT, "config.json"),
                    help="ruta al config.json")
    args = ap.parse_args()

    cfg = load_config(args.config)
    spark = SparkSession.builder.appName("gaia_inspect_schema").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(hdfs_uri(cfg, "merged"))

    print("\n=== ESQUEMA ===")
    df.printSchema()

    print("\n=== COLUMNAS (nombre, tipo) ===")
    for nombre, tipo in df.dtypes:
        print(f"{nombre:35s} {tipo}")

    print("\nTotal de columnas:", len(df.columns))

    print("\n=== PRIMERAS 5 FILAS ===")
    df.show(5, truncate=False)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
