#!/usr/bin/env python3
"""Compara dos tablas de candidatos por conjunto, no por orden de filas.

    python3 scripts/compare_clusters.py results/clusters/clusters_dbscan.csv /tmp/nueva.csv

El `cluster_id` se asigna por orden de tamaño y no es estable entre corridas, así que
comparar los CSV byte a byte siempre falla. Esto empareja cada grupo de A con el más
cercano de B en el cielo y compara miembros y paralaje con tolerancia relativa.

Sale con código 1 si algún grupo de A no tiene contraparte en B.
"""
import argparse
import csv
import math
import sys


def load(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def sep_deg(ra1, dec1, ra2, dec2):
    r = math.radians
    c = (math.sin(r(dec1)) * math.sin(r(dec2))
         + math.cos(r(dec1)) * math.cos(r(dec2)) * math.cos(r(ra1 - ra2)))
    return math.degrees(math.acos(max(-1.0, min(1.0, c))))


def close(a, b, tol):
    """Tolerancia relativa, con piso absoluto para valores chicos."""
    return abs(a - b) <= tol * max(abs(a), abs(b), 1e-9)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Compara dos tablas de candidatos")
    ap.add_argument("a", help="tabla de referencia")
    ap.add_argument("b", help="tabla a verificar")
    ap.add_argument("--tol-deg", type=float, default=0.05,
                    help="separación máxima para considerar el mismo grupo (default 0.05)")
    ap.add_argument("--tol-rel", type=float, default=0.02,
                    help="tolerancia relativa en n_members y parallax (default 0.02)")
    args = ap.parse_args(argv)

    A, B = load(args.a), load(args.b)
    print(f"A: {args.a} ({len(A)} grupos)")
    print(f"B: {args.b} ({len(B)} grupos)")
    print(f"tolerancias: {args.tol_deg} deg, {args.tol_rel:.0%} relativo\n")

    bad = 0
    used = set()
    for a in A:
        ra, dec = float(a["ra"]), float(a["dec"])
        cand = [(sep_deg(ra, dec, float(b["ra"]), float(b["dec"])), k, b)
                for k, b in enumerate(B)]
        d, k, b = min(cand, key=lambda t: t[0])
        if d > args.tol_deg:
            print(f"  SIN PAR  A[{a['cluster_id']}] n={a['n_members']:>4} "
                  f"ra={ra:7.3f} dec={dec:7.3f} (el más cercano a {d:.2f} deg)")
            bad += 1
            continue
        used.add(k)
        ok_n = close(float(a["n_members"]), float(b["n_members"]), args.tol_rel)
        ok_p = close(float(a["parallax"]), float(b["parallax"]), args.tol_rel)
        flag = "ok " if (ok_n and ok_p) else "DIF"
        if not (ok_n and ok_p):
            bad += 1
        print(f"  {flag} A[{a['cluster_id']}] -> B[{b['cluster_id']}] a {d:.3f} deg | "
              f"n {a['n_members']} vs {b['n_members']} | "
              f"plx {float(a['parallax']):.3f} vs {float(b['parallax']):.3f}")

    extra = len(B) - len(used)
    print(f"\nemparejados: {len(A) - bad}/{len(A)} | problemas: {bad} | "
          f"grupos de B sin par: {extra}")
    return 0 if bad == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
