#!/usr/bin/env bash
# Staging de datos: descarga Norder=4/Dir=0, unifica y deja el Parquet en HDFS.
#
# Por qué en el nodo borde y no en YARN: los ejecutores no pueden leer el disco
# local del nodo borde, así que copiar 263 archivos de ~7 MB a HDFS para luego
# leerlos distribuidos cuesta 1,68 GiB de escritura extra y no aporta nada
# (es I/O, no cómputo). El análisis posterior sí corre en YARN.
#
# Uso:  bash scripts/stage_data.sh
#       GAIA_RAW_DIR=/data/2026/uhadoop/teatime/raw bash scripts/stage_data.sh
set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(dirname "$HERE")
CFG=${CFG:-$REPO/config.json}
cd "$REPO" || exit 2
mkdir -p raw results

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

log "== preflight =="
for bin in python3 spark-submit hdfs; do
  command -v "$bin" >/dev/null || { log "falta '$bin' en PATH"; exit 2; }
done
python3 -V
python3 -c "import requests; print('requests', requests.__version__)" || exit 2

RAW_DIR=${GAIA_RAW_DIR:-$REPO/raw}
mkdir -p "$RAW_DIR" || { log "no puedo crear $RAW_DIR"; exit 2; }
log "datos crudos en: $RAW_DIR"

python3 - "$RAW_DIR" <<'PY' || exit 2
import shutil, sys
free = shutil.disk_usage(sys.argv[1]).free
print("espacio libre en {}: {:.1f} GiB".format(sys.argv[1], free / 2**30))
if free < 4 * 2**30:
    print("ERROR: se necesitan al menos 4 GiB libres (1,68 GiB de crudos + margen)")
    sys.exit(1)
PY

PROJ=$(python3 -c "import json;print(json.load(open('$CFG'))['hdfs']['project'])")
MERGED=$(python3 -c "import json;c=json.load(open('$CFG'));print(c['hdfs']['uri'].rstrip('/')+c['hdfs']['merged'])")
log "proyecto HDFS: $PROJ"
hdfs dfs -mkdir -p "$PROJ" || { log "no pude crear $PROJ en HDFS"; exit 2; }
hdfs dfs -ls "$PROJ" | head -10

if ! python3 src/download.py --config "$CFG" --check-only; then
  log "NO_INTERNET_HF: el nodo borde no alcanza huggingface.co"
  log "plan B: descargar en tu máquina y sincronizar raw/ (ver README)"
  exit 3
fi

log "== descarga =="
python3 src/download.py --config "$CFG" || { log "descarga incompleta"; exit 4; }

log "== unificación con Spark =="
# shellcheck source=/dev/null
source "$HERE/yarn_env.sh"
GAIA_MASTER='local[6]' GAIA_DRIVER_MEMORY=6g gaia_spark_conf
spark-submit "${GAIA_CONF[@]}" src/stage.py --config "$CFG" \
  2>&1 | tee results/stage.log | grep -vE '^[0-9]{2}/[0-9]{2}/[0-9]{2}' | tail -60
status=${PIPESTATUS[0]}
if [ "$status" -ne 0 ]; then
  log "stage.py falló (exit $status); revisa results/stage.log"
  exit 5
fi

log "== HDFS =="
hdfs dfs -ls "$PROJ" | head -10
hdfs dfs -du -h "$MERGED" 2>/dev/null | awk '{n+=1; b+=$1} END {if (n) printf "partes: %d\n", n}'
hdfs dfs -du -s -h "$MERGED" 2>/dev/null

log "== resumen =="
cat results/stage_summary.json
log "FIN OK"
