
from pathlib import Path

import pandas as pd
import numpy as np
from astropy.coordinates import SkyCoord
import astropy.units as u


# ==========================================
# 1. Rutas y carga de datos
# ==========================================

ROOT = Path(__file__).resolve().parents[1]

RESULTS = ROOT / "results"
CLUSTERS = RESULTS / "clusters"
DOCS = ROOT / "docs"

dbscan = pd.read_csv(CLUSTERS / "clusters_dbscan.csv")
celdas = pd.read_csv(CLUSTERS / "clusters_celdas.csv")
catalogo = pd.read_csv(DOCS / "catalogo_hunt_reffert.csv")

# Identificar el método de cada candidato
candidatos = pd.concat([
    dbscan.assign(metodo="DBSCAN"),
    celdas.assign(metodo="Celdas")
], ignore_index=True)

# Crear carpeta de resultados si no existe
RESULTS.mkdir(exist_ok=True)


# ==========================================
# 2. Coordenadas celestes
# ==========================================

# Conservar únicamente los cúmulos del catálogo
# que tengan coordenadas válidas
catalogo = catalogo.dropna(
    subset=["ra_deg", "dec_deg"]
).copy()

coord_candidatos = SkyCoord(
    ra=candidatos["ra"].to_numpy() * u.deg,
    dec=candidatos["dec"].to_numpy() * u.deg,
    frame="icrs"
)

coord_catalogo = SkyCoord(
    ra=catalogo["ra_deg"].to_numpy() * u.deg,
    dec=catalogo["dec_deg"].to_numpy() * u.deg,
    frame="icrs"
)

# Matriz de separaciones angulares
separaciones = coord_candidatos[:, None].separation(
    coord_catalogo[None, :]
).to(u.deg).value


# ==========================================
# 3. Buscar los 5 cúmulos más cercanos
# ==========================================

resultados = []

for i, candidato in candidatos.iterrows():

    indices = np.argsort(separaciones[i])[:5]

    for j in indices:

        conocido = catalogo.iloc[j]

        resultados.append({
            "metodo": candidato["metodo"],
            "candidato": candidato["cluster_id"],
            "n_members": candidato["n_members"],

            "catalogo": conocido["name"],
            "all_names": conocido["all_names"],

            "separacion_deg": separaciones[i, j],

            "plx_candidato": candidato["plx_med"],
            "plx_catalogo": conocido["parallax_mas"],
            "delta_plx": abs(
                candidato["plx_med"] - conocido["parallax_mas"]
            ),

            "pmra_candidato": candidato["pmra"],
            "pmra_catalogo": conocido["pmra_masyr"],
            "delta_pmra": abs(
                candidato["pmra"] - conocido["pmra_masyr"]
            ),

            "pmdec_candidato": candidato["pmdec"],
            "pmdec_catalogo": conocido["pmdec_masyr"],
            "delta_pmdec": abs(
                candidato["pmdec"] - conocido["pmdec_masyr"]
            )
        })


# ==========================================
# 4. Guardar resultados del crossmatch
# ==========================================

resultado = pd.DataFrame(resultados)

resultado = resultado.sort_values(
    ["metodo", "candidato", "separacion_deg"]
)

archivo_comparacion = RESULTS / "comparacion_catalogo.csv"

resultado.to_csv(
    archivo_comparacion,
    index=False
)


# ==========================================
# 5. Mostrar resumen
# ==========================================

print("\n" + "=" * 65)
print("COMPARACIÓN CON HUNT & REFFERT (2023)")
print("=" * 65)

for (metodo, candidato), grupo in resultado.groupby(
    ["metodo", "candidato"]
):

    print(f"\n{metodo} | Candidato {candidato}")

    for _, fila in grupo.head(3).iterrows():

        print(
            f"  {fila['catalogo']} | "
            f"Separación: {fila['separacion_deg']:.3f}° | "
            f"Δplx: {fila['delta_plx']:.3f} mas | "
            f"Δpmra: {fila['delta_pmra']:.3f} mas/yr | "
            f"Δpmdec: {fila['delta_pmdec']:.3f} mas/yr"
        )


# ==========================================
# 6. Construir tabla resumida
# ==========================================

# Seleccionar el cúmulo espacialmente más cercano
# para cada candidato
idx = resultado.groupby(
    ["metodo", "candidato"]
)["separacion_deg"].idxmin()

tabla = resultado.loc[idx].copy()

# Diferencia vectorial de movimiento propio
tabla["delta_pm"] = np.sqrt(
    tabla["delta_pmra"]**2 + tabla["delta_pmdec"]**2
)

# Seleccionar y renombrar columnas
tabla = tabla[[
    "metodo",
    "candidato",
    "catalogo",
    "separacion_deg",
    "delta_plx",
    "delta_pm"
]].rename(columns={
    "metodo": "Método",
    "candidato": "Candidato",
    "catalogo": "Cúmulo conocido",
    "separacion_deg": "Separación [°]",
    "delta_plx": "Δplx [mas]",
    "delta_pm": "ΔPM [mas/año]"
})

# Ordenar y redondear
tabla = tabla.sort_values(["Método", "Candidato"])
tabla.iloc[:, 3:] = tabla.iloc[:, 3:].round(3)


# ==========================================
# 7. Guardar tabla CSV y LaTeX
# ==========================================

tabla.to_csv(
    RESULTS / "tabla_crossmatch.csv",
    index=False,
    encoding="utf-8-sig"
)

tabla.to_latex(
    RESULTS / "tabla_crossmatch.tex",
    index=False,
    escape=True,
    float_format="%.3f"
)


# ==========================================
# 8. Resumen final
# ==========================================

print("\n" + "=" * 65)
print("TABLA RESUMIDA DEL CROSSMATCH")
print("=" * 65)

print(tabla.to_string(index=False))

print("\nArchivos guardados:")
print("- results/comparacion_catalogo.csv")
print("- results/tabla_crossmatch.csv")
print("- results/tabla_crossmatch.tex")
