# inspect_schema.py — imprime el esquema del Parquet unificado en HDFS.
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("gaia_columnas")
    .getOrCreate()
)

PATH = "hdfs://cm:9000/data/2026/uhadoop/teatime/gaia/gaia_mmu_merged.parquet"

df = spark.read.parquet(PATH)

print("\n=== ESQUEMA ===")
df.printSchema()

print("\n=== COLUMNAS (nombre, tipo) ===")
for nombre, tipo in df.dtypes:
    print(f"{nombre:35s} {tipo}")

print("\nTotal de columnas:", len(df.columns))

print("\n=== PRIMERAS 5 FILAS ===")
df.show(5, truncate=False)

spark.stop()
