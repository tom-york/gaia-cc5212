#!/usr/bin/env python3
"""Regresión de determinismo de clusters_dbscan.py.

groupByKey no garantiza el orden de las filas dentro de una baldosa y flatMap no
garantiza el orden de los grupos que llegan al driver. De esos órdenes dependían
los promedios, el recorte por magnitud, el flood-fill de DBSCAN y el desempate de
la tabla final, así que dos corridas con la misma config daban grupos distintos.

Se prueba barajando la entrada en tres puntos y exigiendo salida idéntica:

  1. find_clusters   con las filas de la baldosa en otro orden
  2. merge_clusters  con los grupos del driver en otro orden
  3. merge_clusters  con dos grupos empatados en n_members

Corre sin Spark ni cluster:  python3 tests/test_determinism.py
"""
import importlib.util
import random
import sys
from pathlib import Path

import numpy as np

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "tests" / "pyspark_stub"))
sys.path.insert(0, str(RAIZ / "src"))

PARAMS = dict(
    max_stars_per_tile=30000, min_members=15, scale_pos=0.5, scale_plx=0.15,
    scale_pm=0.5, eps=1.0, min_pts=8,
    merge=dict(pos_deg=0.3, plx=0.15, pm=2.0),
)


def cargar_modulo():
    ruta = RAIZ / "src" / "clusters_dbscan.py"
    spec = importlib.util.spec_from_file_location("clusters_dbscan_test", ruta)
    modulo = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = modulo
    spec.loader.exec_module(modulo)
    return modulo


def datos(seed=7):
    """Una baldosa sintética: tres grupos separados en paralaje y 200 estrellas de fondo."""
    rng = np.random.default_rng(seed)
    filas = []

    def grupo(n, ra, dec, plx, dispersion_plx, pm):
        for _ in range(n):
            filas.append((
                rng.uniform(1e17, 9e17),
                ra + rng.normal(0, 300 / 3600),
                dec + rng.normal(0, 300 / 3600),
                plx + rng.normal(0, dispersion_plx),
                pm[0] + rng.normal(0, 0.4),
                pm[1] + rng.normal(0, 0.4),
                rng.uniform(12, 18),
            ))

    grupo(220, 322.80, 54.85, 0.380, 0.015, (-3.0, 1.0))
    grupo(180, 322.95, 54.92, 0.620, 0.020, (-3.2, 0.9))
    grupo(120, 322.75, 54.80, 0.860, 0.015, (-2.0, 0.5))
    grupo(60, 323.10, 55.05, 0.700, 0.010, (-2.8, 1.2))
    for _ in range(200):
        filas.append((
            rng.uniform(1e17, 9e17),
            322.85 + rng.normal(0, 0.5),
            54.87 + rng.normal(0, 0.5),
            rng.uniform(0.1, 0.9),
            rng.uniform(-8, 2),
            rng.uniform(-2, 4),
            rng.uniform(12, 18),
        ))
    return filas


def barajar(items, seed):
    copia = list(items)
    random.Random(seed).shuffle(copia)
    return copia


def comparar(nombre, referencia, obtenido, k, fallos):
    if referencia == obtenido:
        return
    fallos.append(nombre)
    if sorted(referencia) == sorted(obtenido):
        detalle = "mismos grupos, distinto orden o numeración"
    else:
        detalle = (f"contenido distinto: n {sorted(r[1] for r in referencia)}"
                   f" vs {sorted(r[1] for r in obtenido)}")
    print(f"  {nombre} orden {k}: {detalle}")


def main():
    mod = cargar_modulo()
    fallos = []

    filas = datos()
    referencia = mod.find_clusters(1, filas, PARAMS)
    print(f"find_clusters: {len(referencia)} grupos, n = {sorted(r[1] for r in referencia)}")
    for k in range(10):
        comparar("find_clusters", referencia,
                 mod.find_clusters(1, barajar(filas, k), PARAMS), k, fallos)
    print(f"find_clusters: {len(fallos)}/10 órdenes distintos")

    fallos_driver = []
    referencia_m = mod.merge_clusters(referencia, PARAMS)
    for k in range(10):
        comparar("merge_clusters", referencia_m,
                 mod.merge_clusters(barajar(referencia, 100 + k), PARAMS), k, fallos_driver)
    print(f"merge_clusters: {len(fallos_driver)}/10 órdenes distintos")

    # (baldosa, n, ra, dec, plx, pmra, pmdec, sd_plx, sd_pmra, sd_pmdec, radio,
    #  plx_med, plx_p16, plx_p84)
    empate = [
        (1, 20, 322.80, 54.85, 0.450, -3.0, 1.0, 0.01, 0.1, 0.1, 0.10, 0.448, 0.44, 0.46),
        (2, 20, 100.00, -20.00, 0.300, 5.0, -2.0, 0.01, 0.1, 0.1, 0.10, 0.301, 0.29, 0.31),
        (3, 30, 200.00, 10.00, 0.500, 1.0, 3.0, 0.01, 0.1, 0.1, 0.10, 0.499, 0.49, 0.51),
    ]
    fallos_empate = []
    referencia_e = mod.merge_clusters(empate, PARAMS)
    for k in range(6):
        comparar("empate n", referencia_e,
                 mod.merge_clusters(barajar(empate, 200 + k), PARAMS), k, fallos_empate)
    print(f"empate n: {len(fallos_empate)}/6 órdenes distintos")

    # La banda de paralaje debe salir de los miembros del grupo: si se calculara
    # sobre una vecindad relajada, la mediana se sesgaria hacia paralajes mayores.
    desordenadas = [g for g in referencia if not (g[12] <= g[11] <= g[13])]
    print(f"banda de paralaje: {len(desordenadas)} grupos con p16 > mediana o mediana > p84")

    total = len(fallos) + len(fallos_driver) + len(fallos_empate) + len(desordenadas)
    print("\nOK: la salida no depende del orden de entrada" if not total
          else f"\nFALLA: {total} casos dependen del orden de entrada")
    return 0 if not total else 1


if __name__ == "__main__":
    sys.exit(main())
