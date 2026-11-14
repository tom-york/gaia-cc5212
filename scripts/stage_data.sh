#!/usr/bin/env bash
# Staging de datos: descarga Norder=4/Dir=0, sube los crudos a HDFS, los unifica
# con Spark en YARN y deja el Parquet de 12 columnas en HDFS.
#
# Flujo: la descarga es red (nodo borde); el PUT a HDFS deja el crudo en el
# sistema distribuido (1,68 GiB, replicación 1) y la unificación corre en los
# ejecutores leyendo desde HDFS. El crudo queda en HDFS para poder reconstruir
# el Parquet sin volver a descargar.
#
# Códigos de salida: 2 entorno/HDFS, 3 sin internet, 4 descarga, 5 Spark, 6 PUT.
#
# Uso:  bash scripts/stage_data.sh
#       GAIA_RAW_DIR=/data/2026/uhadoop/teatime/raw bash scripts/stage_data.sh
#       GAIA_MASTER='local[6]' bash scripts/stage_data.sh    # plan B sin YARN
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

log "== subida de crudos a HDFS =="
HDFS_URI=$(python3 -c "import json;print(json.load(open('$CFG'))['hdfs']['uri'].rstrip('/'))")
HDFS_RAW="$PROJ/raw"
LOCAL_COUNT=$(ls -1 "$RAW_DIR"/*.parquet 2>/dev/null | wc -l)
[ "$LOCAL_COUNT" -gt 0 ] || { log "no hay parquet en $RAW_DIR"; exit 4; }
hdfs dfs -mkdir -p "$HDFS_RAW" || { log "no pude crear $HDFS_RAW"; exit 2; }
HDFS_COUNT=$(hdfs dfs -count "$HDFS_RAW" 2>/dev/null | awk '{print $2}')
HDFS_COUNT=${HDFS_COUNT:-0}
if [ "$HDFS_COUNT" -eq "$LOCAL_COUNT" ]; then
  log "raw ya está en HDFS ($HDFS_COUNT archivos); se omite la subida"
else
  log "subiendo $LOCAL_COUNT archivos con replicación 1"
  hdfs dfs -D dfs.replication=1 -put -f "$RAW_DIR"/*.parquet "$HDFS_RAW"/ \
    || { log "falló la subida a HDFS"; exit 6; }
fi
hdfs dfs -count -h "$HDFS_RAW"
HDFS_COUNT=$(hdfs dfs -count "$HDFS_RAW" | awk '{print $2}')
[ "$HDFS_COUNT" -eq "$LOCAL_COUNT" ] \
  || { log "mismatch: HDFS tiene $HDFS_COUNT archivos, local $LOCAL_COUNT"; exit 6; }

log "== unificación con Spark =="
# shellcheck source=/dev/null
source "$HERE/yarn_env.sh"
gaia_spark_conf
log "master: ${GAIA_MASTER} | executors: ${GAIA_NUM_EXECUTORS:-n/a} x ${GAIA_EXEC_MEMORY:-?}"
spark-submit "${GAIA_CONF[@]}" src/stage.py --config "$CFG" \
  --raw-uri "$HDFS_URI$HDFS_RAW" \
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
log "crudo local: $RAW_DIR (se puede borrar con rm -f $RAW_DIR/*.parquet)"
log "crudo HDFS: $HDFS_RAW | unificado: $MERGED"
log "FIN OK"
