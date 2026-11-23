
import pandas as pd
import healpy as hp
import numpy as np

# Leer los 263 píxeles de la cobertura
archivos = pd.read_csv(
    "results/staging/files.tsv",
    sep="\t",
    header=None,
    names=["archivo", "dato"]
)

pixeles = set(
    archivos["archivo"]
    .str.extract(r"Npix=(\d+)")[0]
    .astype(int)
)

# Resolución HEALPix
nside = 16

def comprobar_candidatos(ruta, nombre):
    df = pd.read_csv(ruta)

    ra = pd.to_numeric(df["ra"])
    dec = pd.to_numeric(df["dec"])

    # Convertir RA/Dec a índices HEALPix NESTED
    theta = np.radians(90 - dec)
    phi = np.radians(ra)

    indices = hp.ang2pix(
        nside,
        theta,
        phi,
        nest=True
    )

    dentro = np.array([i in pixeles for i in indices])

    print(f"\n{nombre}")
    print("Candidatos:", len(df))
    print("Dentro de la cobertura:", dentro.sum())
    print("Fuera de la cobertura:", (~dentro).sum())

    print("\nDetalle:")
    for i, pertenece in zip(indices, dentro):
        print(f"Npix={i:4d} | En cobertura: {pertenece}")


comprobar_candidatos(
    "results/clusters/clusters_dbscan.csv",
    "DBSCAN"
)

comprobar_candidatos(
    "results/clusters/clusters_celdas.csv",
    "Método de celdas"
)
