#!/usr/bin/env bash
# Flags de spark-submit validados en el cluster uhadoop (cc5212).
#
# Contexto: el smoke test inicial moría con exit 143 / exitCode -103 porque
# el ApplicationMaster superaba el límite de memoria VIRTUAL
# ("Container ... is running beyond virtual memory limits", vmem-pmem-ratio
# ~2,1). Con -Xmx chico, la JVM 11 reserva ~2,2 GB virtuales aunque use
# ~195 MB reales. La solución validada es subir la memoria física del AM y de
# los ejecutores (el techo virtual escala con ella) y recortar reservas de la
# JVM. No era el conector Cassandra.
#
# Uso:
#   source scripts/yarn_env.sh
#   gaia_spark_conf                      # llena el array GAIA_CONF
#   spark-submit "${GAIA_CONF[@]}" src/pipeline.py --config config.json
#
# Para correr sin YARN:
#   GAIA_MASTER='local[6]' gaia_spark_conf

GAIA_MASTER=${GAIA_MASTER:-yarn}
GAIA_DEPLOY=${GAIA_DEPLOY:-client}
GAIA_NUM_EXECUTORS=${GAIA_NUM_EXECUTORS:-8}
GAIA_EXEC_MEMORY=${GAIA_EXEC_MEMORY:-1536m}
GAIA_EXEC_OVERHEAD=${GAIA_EXEC_OVERHEAD:-512}
GAIA_DRIVER_MEMORY=${GAIA_DRIVER_MEMORY:-4g}
GAIA_SHUFFLE_PARTITIONS=${GAIA_SHUFFLE_PARTITIONS:-32}

_JVM_TWEAKS='-XX:MaxMetaspaceSize=256m -XX:ReservedCodeCacheSize=128m -Xss512k'

gaia_spark_conf() {
  GAIA_CONF=(--master "${GAIA_MASTER}" --driver-memory "${GAIA_DRIVER_MEMORY}")

  if [[ "${GAIA_MASTER}" == yarn* ]]; then
    GAIA_CONF+=(
      --deploy-mode "${GAIA_DEPLOY}"
      --num-executors "${GAIA_NUM_EXECUTORS}"
      --executor-cores 1
      --executor-memory "${GAIA_EXEC_MEMORY}"
      --conf "spark.executor.memoryOverhead=${GAIA_EXEC_OVERHEAD}"
      --conf "spark.yarn.am.memory=${GAIA_EXEC_MEMORY}"
      --conf "spark.yarn.am.memoryOverhead=${GAIA_EXEC_OVERHEAD}"
      --conf "spark.yarn.am.extraJavaOptions=${_JVM_TWEAKS}"
      --conf "spark.executor.extraJavaOptions=${_JVM_TWEAKS}"
    )
  else
    GAIA_CONF+=(--conf "spark.driver.extraJavaOptions=${_JVM_TWEAKS}")
  fi

  GAIA_CONF+=(
    --conf spark.jars.packages=
    --conf spark.ui.enabled=false
    --conf spark.hadoop.dfs.replication=1
    --conf "spark.sql.shuffle.partitions=${GAIA_SHUFFLE_PARTITIONS}"
  )

  # El driver en modo client corre en el nodo borde: no reservar de más.
  export PYSPARK_PYTHON=${PYSPARK_PYTHON:-python3}
  export PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON:-python3}
}
