
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

df = pd.read_csv("results/comparacion_catalogo.csv")

# Conservar el cúmulo espacialmente más cercano a cada candidato
idx = df.groupby(["metodo", "candidato"])["separacion_deg"].idxmin()
cercanos = df.loc[idx].copy()

# Diferencia total en movimiento propio
cercanos["delta_pm"] = np.sqrt(
    cercanos["delta_pmra"]**2 +
    cercanos["delta_pmdec"]**2
)

cercanos = cercanos.sort_values(
    ["metodo", "candidato"]
).reset_index(drop=True)

fig, ax = plt.subplots(figsize=(11, 8))

marcadores = {"DBSCAN": "o", "Celdas": "^"}

for metodo, grupo in cercanos.groupby("metodo"):
    y = grupo.index
    puntos = ax.scatter(
        grupo["separacion_deg"],
        y,
        c=grupo["delta_plx"],
        cmap="viridis",
        marker=marcadores[metodo],
        s=85,
        edgecolor="black",
        linewidth=0.5,
        label=metodo,
        zorder=3
    )

    for _, fila in grupo.iterrows():
        ax.annotate(
            fila["catalogo"],
            (fila["separacion_deg"], fila.name),
            xytext=(7, 0),
            textcoords="offset points",
            va="center",
            fontsize=8
        )

ax.set_yticks(cercanos.index)
ax.set_yticklabels([
    f"{r.metodo} {r.candidato}"
    for r in cercanos.itertuples()
])

ax.set_xlabel("Separación angular [°]")
ax.set_ylabel("Candidato")
ax.set_title("Candidatos y cúmulos conocidos más cercanos")

cbar = plt.colorbar(puntos, ax=ax)
cbar.set_label(r"$\Delta$ paralaje [mas]")

ax.grid(axis="x", alpha=0.25)
ax.legend(title="Método")
fig.tight_layout()

fig.savefig(
    "results/figures/fig_09_comparacion_catalogo.png",
    dpi=200,
    bbox_inches="tight"
)
plt.show()
