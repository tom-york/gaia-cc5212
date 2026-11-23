#!/usr/bin/env python3
"""Figuras del informe a partir de las tablas versionadas.

No necesita Spark ni los datos crudos: todo sale de `results/clusters/*.csv`,
`results/experiments/<tag>/` y los histogramas `cells_<res>_hist.json`. Uso:

    python3 src/figures.py                      # todo a results/figures/
    python3 src/figures.py --out /tmp/figs      # otra carpeta
    python3 src/figures.py --only eps,cielo     # un subconjunto

Cada figura declara su fuente en el manifiesto `figures.json`, para que el
informe pueda citar de dónde salió cada panel.
"""
import argparse
import csv
import json
import math
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.lines import Line2D  # noqa: E402

from astropy.coordinates import SkyCoord
import astropy.units as u
import healpy as hp
from matplotlib.patches import Ellipse
from matplotlib.collections import PolyCollection

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import dump_json, load_config, repo_root, results_path  # noqa: E402

AZUL = "#1f4e79"
NARANJA = "#d97706"
VERDE = "#2f7d4f"
GRIS = "#6b7280"
ROJO = "#a61b29"

plt.rcParams.update({
    "figure.dpi": 160,
    "savefig.dpi": 160,
    "font.size": 9,
    "axes.titlesize": 10,
    "axes.labelsize": 9,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.25,
    "grid.linewidth": 0.6,
    "legend.frameon": False,
    "legend.fontsize": 8,
})


# ----------------------------- LECTURA --------------------------------------
def leer_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def flota(r, campo, defecto=None):
    try:
        v = float(r[campo])
    except (KeyError, TypeError, ValueError):
        return defecto
    return v if math.isfinite(v) else defecto


def v_disp(r):
    """Dispersión de movimiento propio en km/s: 4,7405 · σ_pm / plx."""
    pm = math.hypot(flota(r, "sd_pmra", 0.0), flota(r, "sd_pmdec", 0.0))
    plx = flota(r, "parallax", 0.0)
    return 4.7405 * pm / plx if plx > 0 else float("nan")


def etiqueta(r):
    return f"{flota(r, 'n_members', 0):.0f}"


def cargar(results):
    datos = {
        "dbscan": leer_csv(os.path.join(results, "clusters", "clusters_dbscan.csv")),
        "celdas": leer_csv(os.path.join(results, "clusters", "clusters_celdas.csv")),
        "variantes": {},
        "hist": {},
        "tiempos": [],
        "resumen": {},
    }
    exp = os.path.join(results, "experiments")
    for tag, metodo in (("eps_0.8", "dbscan"), ("dbscan_base", "dbscan"),
                        ("eps_1.2", "dbscan"), ("celdas_base", "celdas"),
                        ("kin_rel_1.5", "celdas"), ("snr5", "celdas")):
        path = os.path.join(exp, tag, f"clusters_{metodo}.csv")
        if os.path.exists(path):
            datos["variantes"][tag] = leer_csv(path)
    for tag in ("celdas_base", "snr5"):
        for res in ("0.25", "0.5", "1.0"):
            path = os.path.join(exp, tag, f"cells_{res}_hist.json")
            if os.path.exists(path):
                with open(path, encoding="utf-8") as fh:
                    datos["hist"][(tag, res)] = json.load(fh)
    path = os.path.join(exp, "timings.json")
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            datos["tiempos"] = json.load(fh)
    for metodo in ("dbscan", "celdas"):
        path = os.path.join(results, "clusters", f"run_summary_{metodo}.json")
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                datos["resumen"][metodo] = json.load(fh)
    return datos


# ----------------------------- FIGURAS --------------------------------------
def fig_embudo(d, out):
    """Del total de la partición a los candidatos, con el corte de calidad."""
    db = d["resumen"].get("dbscan", {})
    ce = d["resumen"].get("celdas", {})
    total = db.get("rows_total") or ce.get("rows_total")
    filtradas = db.get("rows_after_filter") or ce.get("rows_after_filter")
    if not total or not filtradas:
        return None

    filas = [("Partición\nNorder=4/Dir=0", total),
             ("Filtro de calidad\nplx/σ ≥ 10, G ≤ 18", filtradas),
             ("Candidatos\nDBSCAN 5D", len(d["dbscan"])),
             ("Candidatos\nceldas", len(d["celdas"]))]

    fig, ax = plt.subplots(figsize=(6.4, 2.9))
    y = range(len(filas))
    valores = [f[1] for f in filas]
    colores = [GRIS, GRIS, AZUL, NARANJA]
    ax.barh(list(y), valores, color=colores, height=0.62)
    ax.set_yticks(list(y))
    ax.set_yticklabels([f[0] for f in filas])
    ax.invert_yaxis()
    ax.set_xscale("log")
    ax.set_xlim(1, total * 3)
    ax.set_xlabel("filas (escala logarítmica)")
    for i, v in enumerate(valores):
        extra = f"  {100.0 * v / total:.1f} %".replace(".", ",") if i == 1 else ""
        ax.text(v * 1.15, i, f"{v:,}".replace(",", ".") + extra, va="center", fontsize=8.5)
    ax.grid(axis="y", visible=False)
    ax.set_title("Del catálogo a los candidatos")
    fig.tight_layout()
    path = os.path.join(out, "fig_01_embudo.png")
    fig.savefig(path)
    plt.close(fig)
    return (path, "embudo: total, filtro de calidad y candidatos por método",
            "run_summary_dbscan.json, run_summary_celdas.json")


def fig_cielo(d, out):
    """Posición de los candidatos, con zoom a las tres regiones."""
    db, ce = d["dbscan"], d["celdas"]
    fig, (izq, der) = plt.subplots(1, 2, figsize=(7.6, 3.5),
                                   gridspec_kw={"width_ratios": [1.15, 1]})
    fig.subplots_adjust(left=0.075, right=0.855, top=0.82, bottom=0.17, wspace=0.2)

    def pinta(ax):
        for r in db:
            ax.scatter(flota(r, "ra"), flota(r, "dec"),
                       s=8 + 2.2 * flota(r, "n_members", 0),
                       c=flota(r, "dist_med_pc", flota(r, "dist_pc")),
                       cmap="viridis", vmin=1500, vmax=3500,
                       edgecolor="black", linewidth=0.4, alpha=0.9, zorder=3)
        for r in ce:
            ax.scatter(flota(r, "ra"), flota(r, "dec"), s=70, marker="s",
                       facecolor="none", edgecolor=NARANJA, linewidth=1.3, zorder=4)

    for ax in (izq, der):
        pinta(ax)
    izq.set_xlim(0, 360)
    izq.set_ylim(-90, 90)
    izq.set_xlabel("RA [°]")
    izq.set_ylabel("Dec [°]")
    izq.set_title("Partición completa")
    izq.invert_xaxis()
    izq.set_xticks([360, 270, 180, 90, 0])

    der.set_xlim(358, 315)
    der.set_ylim(45, 70)
    der.set_xlabel("RA [°]")
    der.set_title("Zoom: las tres regiones")

    sm = plt.cm.ScalarMappable(cmap="viridis",
                               norm=plt.Normalize(vmin=1500, vmax=3500))
    cb = fig.colorbar(sm, cax=fig.add_axes([0.875, 0.17, 0.018, 0.65]))
    cb.set_label("distancia mediana [pc]")
    leyenda = [Line2D([], [], marker="o", linestyle="", markersize=5,
                      markerfacecolor=GRIS, markeredgecolor="black",
                      label="DBSCAN (tamaño ∝ n)"),
               Line2D([], [], marker="s", linestyle="", markersize=7,
                      markerfacecolor="none", markeredgecolor=NARANJA,
                      label="celdas")]
    izq.legend(handles=leyenda, loc="lower left")
    fig.suptitle("Candidatos sobre el cielo; solo las baldosas de la partición", y=0.98)
    path = os.path.join(out, "fig_02_cielo.png")
    fig.savefig(path)
    plt.close(fig)
    return (path, "posición de los 16 + 3 candidatos, con zoom a las tres regiones",
            "results/clusters/clusters_dbscan.csv, clusters_celdas.csv")

def fig_via_lactea(d, out):
    """Candidatos y cobertura Gaia sobre la panorámica ESO."""

    imagen_path = os.path.join(repo_root(), "assets", "eso0932a.jpg")

    if not os.path.exists(imagen_path):
        print(f"No se encuentra la imagen: {imagen_path}")
        return None

    imagen = plt.imread(imagen_path)

    # Conversión RA/Dec -> coordenadas galácticas
    def coordenadas_galacticas(filas):
        ra = np.array([flota(r, "ra") for r in filas])
        dec = np.array([flota(r, "dec") for r in filas])

        coords = SkyCoord(
            ra=ra * u.deg,
            dec=dec * u.deg,
            frame="icrs"
        ).galactic

        x = (180 - coords.l.deg) % 360
        y = coords.b.deg

        return x, y, coords

    fig, ax = plt.subplots(figsize=(12, 6))

    ax.set_facecolor("black")

    # Elipse de recorte
    clip = Ellipse(
        (180, 0),
        width=360,
        height=180,
        transform=ax.transData
    )

    # Fotografía ESO
    fondo = ax.imshow(
        imagen,
        extent=(0, 360, -90, 90),
        aspect="auto",
        origin="upper",
        alpha=0.90,
        zorder=0
    )
    fondo.set_clip_path(clip)

    # ---------------------------------
    # Cobertura Gaia: 263 píxeles HEALPix
    # ---------------------------------

    archivos = pd.read_csv(
        os.path.join(repo_root(), "results", "staging", "files.tsv"),
        sep="\t",
        header=None,
        names=["archivo", "dato"]
    )

    pixeles = (
        archivos["archivo"]
        .str.extract(r"Npix=(\d+)")[0]
        .astype(int)
        .to_numpy()
    )

    nside = 16
    poligonos = []

    for pix in pixeles:
        vertices = hp.boundaries(
            nside,
            pix,
            step=5,
            nest=True
        )

        xyz = vertices.T

        theta, phi = hp.vec2ang(xyz)

        ra = np.degrees(phi)
        dec = 90 - np.degrees(theta)

        coords = SkyCoord(
            ra=ra * u.deg,
            dec=dec * u.deg,
            frame="icrs"
        ).galactic

        x = (180 - coords.l.deg) % 360
        y = coords.b.deg

        # Evitar polígonos que crucen el borde horizontal
        if np.ptp(x) > 180:
            continue

        poligonos.append(np.column_stack([x, y]))

    cobertura = PolyCollection(
        poligonos,
        facecolor="#54B8D8",
        edgecolor="none",
        alpha=0.20,
        label="Cobertura Gaia (263 baldosas)",
        zorder=2
    )

    ax.add_collection(cobertura)
    cobertura.set_clip_path(clip)

    # ---------------------------------
    # Candidatos DBSCAN
    # ---------------------------------

    x_db, y_db, coords_db = coordenadas_galacticas(d["dbscan"])

    db = ax.scatter(
        x_db, y_db,
        marker="*",
        s=38,
        color=AZUL,
        edgecolor="white",
        linewidth=0.45,
        label="DBSCAN",
        zorder=4
    )
    db.set_clip_path(clip)

    # ---------------------------------
    # Método de celdas
    # ---------------------------------

    x_ce, y_ce, coords_ce = coordenadas_galacticas(d["celdas"])

    ce = ax.scatter(
        x_ce, y_ce,
        marker="^",
        s=32,
        color=NARANJA,
        edgecolor="white",
        linewidth=0.45,
        label="Método de celdas",
        zorder=5
    )
    ce.set_clip_path(clip)

    # ---------------------------------
    # Coincidencias entre métodos
    # ---------------------------------

    separaciones = coords_db[:, None].separation(coords_ce[None, :])
    coincidentes = (separaciones < 0.5 * u.deg).any(axis=1)

    if coincidentes.any():
        anillos = ax.scatter(
            x_db[coincidentes],
            y_db[coincidentes],
            marker="o",
            s=85,
            facecolors="none",
            edgecolors="white",
            linewidths=1.0,
            label="Candidatos coincidentes",
            zorder=6
        )
        anillos.set_clip_path(clip)

    # Borde ovalado
    borde = Ellipse(
        (180, 0),
        width=360,
        height=180,
        facecolor="none",
        edgecolor="white",
        linewidth=1.0,
        zorder=7
    )
    ax.add_patch(borde)

    ax.set_xlim(0, 360)
    ax.set_ylim(-90, 90)
    ax.set_aspect("equal")

    ax.set_xlabel("Longitud galáctica proyectada [°]", color="white")
    ax.set_ylabel("Latitud galáctica [°]", color="white")

    ax.set_title(
        "Candidatos a cúmulos estelares y cobertura Gaia",
        color="white",
        pad=12
    )

    ax.tick_params(colors="white")

    ax.legend(
        loc="upper right",
        facecolor="black",
        edgecolor="white",
        labelcolor="white",
        framealpha=0.85,
        fontsize=8
    )

    fig.text(
        0.99, 0.015,
        "Background: ESO/S. Brunier | Gaia footprint: schematic projection",
        ha="right",
        fontsize=7,
        color="white"
    )

    fig.tight_layout()

    path = os.path.join(out, "fig_08_via_lactea.png")

    fig.savefig(
        path,
        dpi=200,
        facecolor="black",
        bbox_inches="tight"
    )

    plt.close(fig)

    return (
        path,
        "Candidatos y cobertura Gaia sobre panorámica de la Vía Láctea",
        "clusters_dbscan.csv, clusters_celdas.csv, files.tsv; imagen ESO/S. Brunier"


def fig_eps(d, out):
    """n vs dispersión para los tres eps: los núcleos fríos y el encadenamiento."""
    series = [("eps_0.8", "eps = 0,8", VERDE, "o"),
              ("dbscan_base", "eps = 1,0", AZUL, "o"),
              ("eps_1.2", "eps = 1,2", ROJO, "o")]
    fig, ax = plt.subplots(figsize=(6.2, 3.6))
    for tag, rotulo, color, marca in series:
        filas = d["variantes"].get(tag, [])
        if not filas:
            continue
        xs = [flota(r, "n_members", 0) for r in filas]
        ys = [v_disp(r) for r in filas]
        ax.scatter(xs, ys, s=34, marker=marca, facecolor="none",
                   edgecolor=color, linewidth=1.2, label=f"{rotulo}  ({len(filas)})")
    for ref, txt in ((1.5, "1,5 km/s"), (3.0, "3 km/s")):
        ax.axhline(ref, color=GRIS, linestyle=":", linewidth=0.9)
        ax.text(0.985, ref + 0.3, txt, color=GRIS, fontsize=7.5,
                ha="right", transform=ax.get_yaxis_transform())
    ax.set_xscale("log")
    ax.set_ylim(0, 22)
    ax.set_xlabel("miembros del grupo")
    ax.set_ylabel("σ de movimiento propio [km/s]")
    ax.set_title("Más eps, más miembros y más dispersión")
    ax.legend(title="variante (nº de grupos)", loc="upper left")
    fig.tight_layout()
    path = os.path.join(out, "fig_03_eps.png")
    fig.savefig(path)
    plt.close(fig)
    return (path, "n vs dispersión interna para eps = 0,8 / 1,0 / 1,2",
            "results/experiments/{eps_0.8,dbscan_base,eps_1.2}/clusters_dbscan.csv")


def fig_distancias(d, out):
    """Distancia con banda p16–p84 y dispersión, con los casos notables rotulados."""
    filas = d["dbscan"]
    fig, ax = plt.subplots(figsize=(6.2, 3.6))
    for r in filas:
        dist = flota(r, "dist_med_pc")
        p16, p84 = flota(r, "dist_p16_pc"), flota(r, "dist_p84_pc")
        ax.errorbar(dist, v_disp(r), xerr=[[dist - p16], [p84 - dist]],
                    fmt="o", markersize=4.5, color=AZUL, ecolor=GRIS,
                    elinewidth=0.8, capsize=1.6, zorder=3)
    for r in d["variantes"].get("eps_1.2", []):
        if flota(r, "dist_med_pc") < 500:
            ax.scatter(flota(r, "dist_med_pc"), v_disp(r), s=40, marker="*",
                       color=ROJO, zorder=4)
            ax.annotate("237 pc, 0,4 km/s", (flota(r, "dist_med_pc"), v_disp(r)),
                        textcoords="offset points", xytext=(8, -2), fontsize=7.5,
                        color=ROJO)
    for r in filas:
        if flota(r, "n_members", 0) >= 200:
            ax.annotate(f"n = {etiqueta(r)}", (flota(r, "dist_med_pc"), v_disp(r)),
                        textcoords="offset points", xytext=(8, 3), fontsize=7.5)
    ax.set_xlabel("distancia mediana [pc], barras = p16–p84")
    ax.set_ylabel("σ de movimiento propio [km/s]")
    ax.set_title("Los 16 candidatos y, aparte, el grupo de 237 pc de eps = 1,2")
    fig.tight_layout()
    path = os.path.join(out, "fig_04_distancias.png")
    fig.savefig(path)
    plt.close(fig)
    return (path, "distancia con banda p16–p84 frente a dispersión interna",
            "results/clusters/clusters_dbscan.csv")


def fig_movimiento(d, out):
    """Plano de movimiento propio de los grupos: familias cinemáticas."""
    fig, ax = plt.subplots(figsize=(5.6, 3.6))
    for r in d["dbscan"]:
        ax.scatter(flota(r, "pmra"), flota(r, "pmdec"),
                   s=10 + 1.8 * flota(r, "n_members", 0),
                   c=flota(r, "plx_med"), cmap="plasma", vmin=0.25, vmax=0.65,
                   edgecolor="black", linewidth=0.4, zorder=3)
    for r in d["celdas"]:
        ax.scatter(flota(r, "pmra"), flota(r, "pmdec"), s=70, marker="s",
                   facecolor="none", edgecolor=NARANJA, linewidth=1.3, zorder=4)
    ax.set_xlabel("μ_α* [mas/año]")
    ax.set_ylabel("μ_δ [mas/año]")
    ax.set_title("Los grupos se separan en el plano de movimiento propio")
    sm = plt.cm.ScalarMappable(cmap="plasma", norm=plt.Normalize(0.25, 0.65))
    cb = fig.colorbar(sm, ax=ax, fraction=0.04, pad=0.02)
    cb.set_label("paralaje mediana [mas]")
    fig.tight_layout()
    path = os.path.join(out, "fig_05_movimiento.png")
    fig.savefig(path)
    plt.close(fig)
    return (path, "plano de movimiento propio de los grupos",
            "results/clusters/clusters_dbscan.csv, clusters_celdas.csv")


def fig_celdas(d, out):
    """Histograma de conteos por celda con el umbral p99 marcado."""
    res = ["0.25", "0.5", "1.0"]
    fig, axes = plt.subplots(1, 3, figsize=(7.2, 2.6), sharey=False)
    for ax, r in zip(axes, res):
        hist = d["hist"].get(("celdas_base", r))
        if not hist:
            ax.set_visible(False)
            continue
        ns = [b["n"] for b in hist["histogram"]]
        cs = [b["cells"] for b in hist["histogram"]]
        ax.bar(ns, cs, width=(ns[1] - ns[0]) * 0.9 if len(ns) > 1 else 1,
               color=GRIS, alpha=0.75)
        ax.axvline(hist["threshold"], color=ROJO, linestyle="--", linewidth=1.1)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_title(f"{r}°  ·  umbral {hist['threshold']:.0f} (p99)")
        ax.set_xlabel("estrellas por celda")
        ax.text(0.03, 0.92, f"{hist['n_cells']:,}".replace(",", ".") + " celdas",
                transform=ax.transAxes, fontsize=7.5, va="top")
    axes[0].set_ylabel("nº de celdas")
    fig.suptitle("Distribución de conteos por celda: el corte es el percentil 99", y=1.0)
    fig.tight_layout()
    path = os.path.join(out, "fig_06_celdas.png")
    fig.savefig(path)
    plt.close(fig)
    return (path, "histograma de conteos por celda y umbral p99 por resolución",
            "results/experiments/celdas_base/cells_<res>_hist.json")


def fig_tiempos(d, out):
    """Costo por variante: tiempo de Spark y de pared."""
    filas = d["tiempos"]
    if not filas:
        return None
    orden = sorted(filas, key=lambda r: (r["method"], r["wall_s"]))
    fig, ax = plt.subplots(figsize=(6.2, 3.2))
    y = range(len(orden))
    spark = [r["spark_s"] or r["wall_s"] for r in orden]
    pared = [r["wall_s"] for r in orden]
    ax.barh(list(y), pared, color=GRIS, alpha=0.35, height=0.6, label="pared")
    ax.barh(list(y), spark, color=[AZUL if r["method"] == "dbscan" else NARANJA
                                   for r in orden], height=0.6, label="Spark")
    ax.set_yticks(list(y))
    ax.set_yticklabels([f"{r['tag']}  ({r['method']})" for r in orden])
    ax.invert_yaxis()
    ax.set_xlabel("segundos")
    for i, (s, w) in enumerate(zip(spark, pared)):
        ax.text(w + 2, i, f"{w:.0f} s", va="center", fontsize=7.5)
    ax.set_title("Costo por variante (gris = lanzador)")
    ax.legend(loc="lower right", bbox_to_anchor=(1.0, 1.0))
    fig.tight_layout()
    path = os.path.join(out, "fig_07_tiempos.png")
    fig.savefig(path)
    plt.close(fig)
    return (path, "tiempo de pared y de Spark por variante",
            "results/experiments/timings.json")


FIGURAS = {
    "embudo": fig_embudo,
    "cielo": fig_cielo,
    "eps": fig_eps,
    "distancias": fig_distancias,
    "movimiento": fig_movimiento,
    "celdas": fig_celdas,
    "tiempos": fig_tiempos,
}


def main(argv=None):
    ap = argparse.ArgumentParser(description="Figuras del informe")
    ap.add_argument("--config", default=os.path.join(repo_root(), "config.json"))
    ap.add_argument("--results", default=None, help="carpeta results/")
    ap.add_argument("--out", default=None, help="carpeta de salida")
    ap.add_argument("--only", default="", help="lista separada por comas")
    args = ap.parse_args(argv)

    cfg = load_config(args.config)
    results = args.results or results_path(cfg, "")
    if not os.path.isdir(results):
        results = os.path.join(repo_root(), "results")
    out = args.out or os.path.join(results, "figures")
    os.makedirs(out, exist_ok=True)

    datos = cargar(results)
    pedidas = [n.strip() for n in args.only.split(",") if n.strip()] or list(FIGURAS)
    manifiesto = []
    for nombre in pedidas:
        fn = FIGURAS.get(nombre)
        if fn is None:
            print(f"figura desconocida: {nombre}", file=sys.stderr)
            continue
        hecho = fn(datos, out)
        if hecho is None:
            print(f"{nombre}: faltan datos, se omite")
            continue
        path, titulo, fuente = hecho
        manifiesto.append({"archivo": os.path.basename(path), "titulo": titulo,
                           "fuente": fuente})
        print(f"{os.path.basename(path):24s} {titulo}")

    if manifiesto:
        dump_json({"figuras": manifiesto}, os.path.join(out, "figures.json"))
    print(f"\n{len(manifiesto)} figuras en {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
