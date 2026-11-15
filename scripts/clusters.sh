#!/usr/bin/env bash
set -euo pipefail

cd ~/teatime/gaia/gaia-cc5212

source scripts/yarn_env.sh
gaia_spark_conf

mkdir -p results

spark-submit "${GAIA_CONF[@]}" \
  --archives pylibs/pylibs.zip#pylibs \
  --conf spark.executorEnv.PYTHONPATH=./pylibs \
  --conf spark.yarn.appMasterEnv.PYTHONPATH=./pylibs \
  src/gaia_clusters.py 2>&1 | tee results/clusters_run.log | grep -vE '^[0-9]{2}/[0-9]{2}/[0-9]{2}'
