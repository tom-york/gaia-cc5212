
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from astropy.coordinates import SkyCoord
import astropy.units as u


# =====================================================
# 1. CARGAR DATOS
# =====================================================

dbscan = pd.read_csv("results/clusters/clusters_dbscan.csv")
celdas = pd.read_csv("results/clusters/clusters_celdas.csv")
catalogo = pd.read_csv("docs/catalogo_hunt_reffert.csv")

dbscan["metodo"] = "DBSCAN"
celdas["metodo"] = "Celdas"

candidatos = pd.concat(
    [dbscan, celdas],
    ignore_index=True
)

# Eliminar registros sin coordenadas válidas
catalogo = catalogo.dropna(subset=["ra_deg", "dec_deg"])
candidatos = candidatos.dropna(subset=["ra", "dec"])

# Coordenadas ecuatoriales
coord_candidatos = SkyCoord(
    ra=candidatos["ra"].values * u.deg,
    dec=candidatos["dec"].values * u.deg,
    frame="icrs"
)

coord_catalogo = SkyCoord(
    ra=catalogo["ra_deg"].values * u.deg,
    dec=catalogo["dec_deg"].values * u.deg,
    frame="icrs"
)

# Transformar a coordenadas galácticas
gal_candidatos = coord_candidatos.galactic
gal_catalogo = coord_catalogo.galactic


# =====================================================
# 2. ENCONTRAR CÚMULO CONOCIDO MÁS CERCANO
# =====================================================

separaciones = coord_candidatos[:, None].separation(
    coord_catalogo[None, :]
).to_value(u.deg)

idx_cercano = np.argmin(separaciones, axis=1)
sep_min = separaciones[
    np.arange(len(candidatos)), idx_cercano
]

# Umbral visual para destacar asociaciones espaciales
umbral = 1.5

catalogo_cercano = catalogo.iloc[idx_cercano].copy()
catalogo_cercano["separacion"] = sep_min

# Cúmulos cercanos únicos
indices_destacados = np.unique(
    idx_cercano[sep_min < umbral]
)

destacados = catalogo.iloc[indices_destacados]


# =====================================================
# 3. CONSTRUIR FIGURA
# =====================================================

fig, ax = plt.subplots(figsize=(16, 9))

# Todos los cúmulos conocidos
ax.scatter(
    gal_catalogo.l.wrap_at(180*u.deg).deg,
    gal_catalogo.b.deg,
    s=9,
    color="#A0A0A0",
    alpha=0.35,
    label="Hunt & Reffert (2023)",
    zorder=1
)

# Destacar cúmulos conocidos cercanos
gal_destacados = coord_catalogo[indices_destacados].galactic

ax.scatter(
    gal_destacados.l.wrap_at(180*u.deg).deg,
    gal_destacados.b.deg,
    s=85,
    facecolors="none",
    edgecolors="#F2C14E",
    linewidths=1.7,
    label="Cúmulos conocidos cercanos",
    zorder=3
)

# Etiquetas de los cúmulos conocidos
for i, fila in zip(indices_destacados, destacados.itertuples()):
    x = gal_destacados.l.wrap_at(180*u.deg).deg[
        list(indices_destacados).index(i)
    ]
    y = gal_destacados.b.deg[
        list(indices_destacados).index(i)
    ]

    ax.annotate(
        str(fila.name),
        (x, y),
        xytext=(5, 6),
        textcoords="offset points",
        fontsize=8,
        color="#F2C14E",
        weight="bold",
        zorder=5
    )


# =====================================================
# 4. DIBUJAR CANDIDATOS Y CONEXIONES
# =====================================================

estilos = {
    "DBSCAN": {
        "marker": "*",
        "color": "#318CE7",
        "size": 190
    },
    "Celdas": {
        "marker": "^",
        "color": "#F28E2B",
        "size": 115
    }
}

x_cand = gal_candidatos.l.wrap_at(180*u.deg).deg
y_cand = gal_candidatos.b.deg

for metodo, estilo in estilos.items():

    mask = candidatos["metodo"].values == metodo

    ax.scatter(
        x_cand[mask],
        y_cand[mask],
        marker=estilo["marker"],
        s=estilo["size"],
        color=estilo["color"],
        edgecolors="white",
        linewidths=0.8,
        label=metodo,
        zorder=6
    )


# Conectar cada candidato con su vecino conocido
for i in range(len(candidatos)):

    if sep_min[i] > umbral:
        continue

    x1 = x_cand[i]
    y1 = y_cand[i]

    x2 = gal_catalogo.l.wrap_at(180*u.deg).deg[idx_cercano[i]]
    y2 = gal_catalogo.b.deg[idx_cercano[i]]

    # Evitar líneas largas por el cruce del meridiano
    dx = (x2 - x1 + 180) % 360 - 180
    x2_ajustado = x1 + dx

    ax.plot(
        [x1, x2_ajustado],
        [y1, y2],
        color="white",
        alpha=0.45,
        linewidth=0.8,
        linestyle="--",
        zorder=2
    )


# =====================================================
# 5. FORMATO FINAL
# =====================================================

ax.set_xlabel("Longitud galáctica [°]", fontsize=12)
ax.set_ylabel("Latitud galáctica [°]", fontsize=12)

ax.set_title(
    "Candidatos a cúmulos estelares vs. catálogo conocido",
    fontsize=17,
    weight="bold",
    pad=18
)

ax.set_xlim(180, -180)
ax.set_ylim(-90, 90)

ax.grid(
    True,
    linestyle=":",
    alpha=0.25
)

ax.set_facecolor("#17202A")
fig.patch.set_facecolor("#17202A")

ax.tick_params(colors="white", labelsize=10)
ax.xaxis.label.set_color("white")
ax.yaxis.label.set_color("white")
ax.title.set_color("white")

legend = ax.legend(
    loc="upper right",
    fontsize=10,
    facecolor="#263442",
    edgecolor="white",
    labelcolor="white"
)

fig.tight_layout()

# Guardar
fig.savefig(
    "results/figures/fig_10_comparacion_posiciones.png",
    dpi=300,
    facecolor=fig.get_facecolor(),
    bbox_inches="tight"
)

fig.savefig(
    "results/figures/fig_10_comparacion_posiciones.pdf",
    facecolor=fig.get_facecolor(),
    bbox_inches="tight"
)

plt.show()

print("Figura guardada correctamente.")
print(f"Cúmulos conocidos: {len(catalogo)}")
print(f"Cúmulos cercanos destacados: {len(destacados)}")
print(f"Candidatos: {len(candidatos)}")
