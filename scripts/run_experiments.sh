#!/usr/bin/env bash
# Barrido de configuraciones de los dos detectores, con tiempos.
#
#   bash scripts/run_experiments.sh --dry-run      # imprime la matriz, no corre nada
#   bash scripts/run_experiments.sh                # corre toda la matriz
#   bash scripts/run_experiments.sh --only celdas  # solo un método
#   bash scripts/run_experiments.sh --tag eps_1.2  # una sola variante
#
# Cada variante se materializa como results/experiments/configs/<tag>.json, así
# que la corrida queda auditable sin tocar el config.json del repositorio.
set -euo pipefail

cd "$(dirname "$0")/.."

BASE_CONFIG="config.json"
OUT_DIR="results/experiments"
CONF_DIR="$OUT_DIR/configs"
TIMINGS="$OUT_DIR/timings.json"
LOG_DIR="results/clusters"   # donde run_clusters.sh deja el log de cada método

ONLY=""
TAG=""
DRY=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY=1; shift ;;
    --only) ONLY="${2:?falta el valor de --only}"; shift 2 ;;
    --tag) TAG="${2:?falta el valor de --tag}"; shift 2 ;;
    -h|--help) sed -n '2,9p' "$0"; exit 0 ;;
    *) echo "argumento desconocido: $1" >&2; exit 2 ;;
  esac
done

# método | tag | overrides  (rutas punteadas separadas por coma)
MATRIX=(
  "dbscan|dbscan_base|"
  "dbscan|eps_0.8|clusters.dbscan.eps=0.8"
  "dbscan|eps_1.2|clusters.dbscan.eps=1.2"
  "celdas|celdas_base|"
  "celdas|kin_rel_1.5|clusters.celdas.kin_rel=1.5"
  "celdas|snr5|clusters.min_plx_snr=5"
)

selected=()
for row in "${MATRIX[@]}"; do
  IFS='|' read -r method tag overrides <<<"$row"
  if [[ -n "$ONLY" && "$method" != "$ONLY" ]]; then continue; fi
  if [[ -n "$TAG" && "$tag" != "$TAG" ]]; then continue; fi
  selected+=("$row")
done

if [[ ${#selected[@]} -eq 0 ]]; then
  echo "no hay variantes que coincidan (--only='$ONLY' --tag='$TAG')" >&2
  exit 2
fi

echo "variantes seleccionadas: ${#selected[@]}"
for row in "${selected[@]}"; do
  IFS='|' read -r method tag overrides <<<"$row"
  printf '  %-8s %-14s %s\n' "$method" "$tag" "${overrides:-(base)}"
done

if [[ $DRY -eq 1 ]]; then
  echo "(--dry-run: no se ejecuta nada)"
  exit 0
fi

mkdir -p "$CONF_DIR"

for row in "${selected[@]}"; do
  IFS='|' read -r method tag overrides <<<"$row"
  cfg="$CONF_DIR/${tag}.json"

  python3 - "$BASE_CONFIG" "$cfg" "$overrides" <<'PY'
import json
import sys

base, out, overrides = sys.argv[1], sys.argv[2], sys.argv[3]
with open(base, encoding="utf-8") as fh:
    cfg = json.load(fh)

for item in (o.strip() for o in overrides.split(",")):
    if not item:
        continue
    path, _, raw = item.partition("=")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        value = raw
    node, keys = cfg, path.split(".")
    for key in keys[:-1]:
        node = node[key]
    node[keys[-1]] = value

with open(out, "w", encoding="utf-8") as fh:
    json.dump(cfg, fh, indent=2, ensure_ascii=False)
PY

  log="$LOG_DIR/clusters_${method}_run.log"
  echo
  echo "=== $tag ($method) ==="
  t0=$(date +%s.%N)
  rc=0
  bash scripts/run_clusters.sh --method "$method" --config "$cfg" || rc=$?
  t1=$(date +%s.%N)

  spark_s=""
  if [[ -f "$log" ]]; then
    spark_s=$(grep -oE 'Tiempo total: [0-9.]+ s' "$log" | tail -1 | grep -oE '[0-9.]+' || true)
  fi

  python3 - "$TIMINGS" "$tag" "$method" "$overrides" "$cfg" "$t0" "$t1" "$rc" "$spark_s" <<'PY'
import json
import os
import sys
from datetime import datetime

path, tag, method, overrides, cfg, t0, t1, rc, spark_s = sys.argv[1:10]
records = []
if os.path.exists(path):
    with open(path, encoding="utf-8") as fh:
        records = json.load(fh)
records = [r for r in records if r.get("tag") != tag]   # re-correr reemplaza
records.append({
    "tag": tag,
    "method": method,
    "overrides": overrides,
    "config": cfg,
    "wall_s": round(float(t1) - float(t0), 1),
    "spark_s": float(spark_s) if spark_s else None,
    "rc": int(rc),
    "finished": datetime.now().isoformat(timespec="seconds"),
})
with open(path, "w", encoding="utf-8") as fh:
    json.dump(records, fh, indent=2, ensure_ascii=False)
print("  wall_s={} spark_s={} rc={}".format(
    records[-1]["wall_s"], records[-1]["spark_s"], rc))
PY
done

echo
echo "tiempos en $TIMINGS"
