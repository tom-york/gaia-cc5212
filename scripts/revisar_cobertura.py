import pandas as pd
import re

df = pd.read_csv(
    "results/staging/files.tsv",
    sep="\t",
    header=None,
    names=["archivo", "dato"]
)

# Extraer Npix desde cada ruta
df["npix"] = df["archivo"].str.extract(r"Npix=(\d+)").astype(int)

print("Cantidad de archivos:", len(df))
print("Cantidad de índices únicos:", df["npix"].nunique())
print("Índice mínimo:", df["npix"].min())
print("Índice máximo:", df["npix"].max())

print("\nPrimeros índices:")
print(df["npix"].head(15).tolist())

print("\n¿Hay índices repetidos?")
print(df["npix"].duplicated().any())
