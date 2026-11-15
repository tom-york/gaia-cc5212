#!/usr/bin/env bash
# Corre uno de los dos detectores de candidatos sobre el Parquet unificado.
#   bash scripts/run_clusters.sh --method celdas   # solo Spark SQL
#   bash scripts/run_clusters.sh --method dbscan   # DBSCAN 5D, numpy en ejecutores
set -euo pipefail

cd "$(dirname "$0")/.."

METHOD="dbscan"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --method) METHOD="${2:?falta el valor de --method}"; shift 2 ;;
    -h|--help) sed -n '2,4p' "$0"; exit 0 ;;
    *) echo "argumento desconocido: $1" >&2; exit 2 ;;
  esac
done

case "$METHOD" in
  dbscan) SCRIPT="src/clusters_dbscan.py" ;;
  celdas) SCRIPT="src/clusters_celdas.py" ;;
  *) echo "método inválido: $METHOD (dbscan|celdas)" >&2; exit 2 ;;
esac

source scripts/yarn_env.sh
gaia_spark_conf

mkdir -p results
LOG="results/clusters_${METHOD}_run.log"
NOISE='^[0-9]{2}/[0-9]{2}/[0-9]{2}'

run_spark() {
  set +e
  spark-submit "$@" 2>&1 | tee "$LOG" | grep -vE "$NOISE"
  local rc=${PIPESTATUS[0]}
  set -e
  if [[ $rc -ne 0 ]]; then
    echo "spark-submit falló con código $rc (log: $LOG)" >&2
    exit "$rc"
  fi
}

# numpy solo hace falta en los ejecutores para DBSCAN. Si el clúster ya lo tiene,
# borra pylibs/ y este bloque desaparece solo (ver README, sección Reproducir).
if [[ "$METHOD" == "dbscan" && -f pylibs/pylibs.zip ]]; then
  run_spark "${GAIA_CONF[@]}" \
    --archives pylibs/pylibs.zip#pylibs \
    --conf spark.executorEnv.PYTHONPATH=./pylibs \
    "$SCRIPT"
else
  run_spark "${GAIA_CONF[@]}" "$SCRIPT"
fi
