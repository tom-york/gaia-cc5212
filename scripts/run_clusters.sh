#!/usr/bin/env bash
# Corre uno de los dos detectores de candidatos sobre el Parquet unificado.
#   bash scripts/run_clusters.sh --method celdas   # solo Spark SQL
#   bash scripts/run_clusters.sh --method dbscan   # DBSCAN 5D, numpy en ejecutores
#   bash scripts/run_clusters.sh --method dbscan --config results/experiments/configs/eps_0.8.json
#   bash scripts/run_clusters.sh --method celdas --out results/experiments/snr5 --tag snr5
#
# --out  carpeta local del CSV, del log y del resumen (por defecto results/clusters)
# --tag  sufijo de los parquet en HDFS, para que dos variantes no se pisen
set -euo pipefail

ORIG_PWD="$PWD"
cd "$(dirname "$0")/.."

METHOD="dbscan"
CONFIG=""
OUT=""
TAG=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --method) METHOD="${2:?falta el valor de --method}"; shift 2 ;;
    --config) CONFIG="${2:?falta el valor de --config}"; shift 2 ;;
    --out) OUT="${2:?falta el valor de --out}"; shift 2 ;;
    --tag) TAG="${2:?falta el valor de --tag}"; shift 2 ;;
    -h|--help) sed -n '2,9p' "$0"; exit 0 ;;
    *) echo "argumento desconocido: $1" >&2; exit 2 ;;
  esac
done

case "$METHOD" in
  dbscan) SCRIPT="src/clusters_dbscan.py" ;;
  celdas) SCRIPT="src/clusters_celdas.py" ;;
  *) echo "método inválido: $METHOD (dbscan|celdas)" >&2; exit 2 ;;
esac

# Las rutas relativas se resuelven contra el directorio desde donde se invocó,
# no contra la raíz del repo (ya hicimos cd). La salida por defecto es del repo.
if [[ -z "$OUT" ]]; then
  OUT="$PWD/results/clusters"
elif [[ "$OUT" != /* ]]; then
  OUT="$ORIG_PWD/$OUT"
fi
if [[ -n "$CONFIG" && "$CONFIG" != /* ]]; then
  CONFIG="$ORIG_PWD/$CONFIG"
fi

SCRIPT_ARGS=(--out "$OUT")
if [[ -n "$CONFIG" ]]; then
  SCRIPT_ARGS+=(--config "$CONFIG")
fi
if [[ -n "$TAG" ]]; then
  SCRIPT_ARGS+=(--tag "$TAG")
fi

source scripts/yarn_env.sh
gaia_spark_conf

mkdir -p "$OUT"
LOG="$OUT/clusters_${METHOD}_run.log"
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

# numpy solo hace falta en los ejecutores para DBSCAN. Sin el archivo la app
# arranca, lee los datos y muere al importar en los workers (~90 s perdidos),
# así que se aborta antes. GAIA_SKIP_NUMPY_CHECK=1 si los ejecutores ya lo tienen.
USE_ARCHIVE=0
if [[ "$METHOD" == "dbscan" ]]; then
  if [[ -f pylibs/pylibs.zip ]]; then
    USE_ARCHIVE=1
  elif [[ "${GAIA_SKIP_NUMPY_CHECK:-0}" == "1" ]]; then
    echo "AVISO: se corre sin pylibs/ (GAIA_SKIP_NUMPY_CHECK=1)" >&2
  else
    echo "ERROR: DBSCAN importa numpy en los ejecutores y no existe pylibs/pylibs.zip." >&2
    echo "       Generarlo en el nodo borde:  bash scripts/make_pylibs.sh" >&2
    echo "       O copiarlo desde tu maquina:" >&2
    echo "         rsync -av -e 'ssh -p 220' pylibs/pylibs.zip" >&2
    echo "           uhadoop@cm.dcc.uchile.cl:~/teatime/gaia/gaia-cc5212/pylibs/" >&2
    echo "       Si los ejecutores ya tienen numpy: export GAIA_SKIP_NUMPY_CHECK=1" >&2
    exit 3
  fi
fi

if [[ $USE_ARCHIVE -eq 1 ]]; then
  run_spark "${GAIA_CONF[@]}" \
    --archives pylibs/pylibs.zip#pylibs \
    --conf spark.executorEnv.PYTHONPATH=./pylibs \
    "$SCRIPT" "${SCRIPT_ARGS[@]}"
else
  run_spark "${GAIA_CONF[@]}" "$SCRIPT" "${SCRIPT_ARGS[@]}"
fi
