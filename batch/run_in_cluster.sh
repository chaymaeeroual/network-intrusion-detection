#!/usr/bin/env bash
# Lance un script batch sur le cluster Spark, depuis le conteneur spark-master.
#   docker exec nid_spark_master bash /opt/tache_B_batch/run_in_cluster.sh <étape>
# Étapes : clean | indicators | pgtest | latency
# Variables utiles (passer avec docker exec -e VAR=valeur) :
#   RAW_HDFS_PATH    dossier ou fichier CSV dans HDFS (défaut hdfs://namenode:9000/data/raw/flows)
#   CLEAN_HDFS_PATH  Parquet nettoyé (défaut hdfs://namenode:9000/data/processed/flows_clean)
#   JDBC_PASSWORD    mot de passe PostgreSQL (OBLIGATOIRE pour indicators/pgtest/latency)
#   WRITE_POSTGRES   true pour que "indicators" écrive dans PostgreSQL
#   DRY_RUN=1        affiche la commande sans l'exécuter
set -euo pipefail
STEP="${1:?usage: run_in_cluster.sh clean|indicators|pgtest|latency}"
DIR="$(cd "$(dirname "$0")" && pwd)"

export RAW_HDFS_PATH="${RAW_HDFS_PATH:-hdfs://namenode:9000/data/raw/flows}"
export CLEAN_HDFS_PATH="${CLEAN_HDFS_PATH:-hdfs://namenode:9000/data/processed/flows_clean}"
export JDBC_URL="${JDBC_URL:-jdbc:postgresql://postgres:5432/intrusion_db}"
export JDBC_USER="${JDBC_USER:-intrusion_user}"

# Réglages imposés par le docker-compose : workers de 512 Mo / 1 cœur.
#  - sans --executor-memory 384m le job reste en attente (1 Go demandé par défaut) ;
#  - dfs.replication=2 pour que Spark écrive avec 2 réplicas comme HDFS ;
#  - spark.jars.ivy : l'utilisateur "spark" n'a pas de dossier home inscriptible.
OPTS=(--master spark://spark-master:7077
      --driver-memory 512m --executor-memory 512m --executor-cores 1
      --conf spark.hadoop.dfs.replication=2
      --conf spark.sql.shuffle.partitions=16
      --conf spark.jars.ivy=/tmp/.ivy2)
JDBC_PKG=(--packages org.postgresql:postgresql:42.7.7)

case "$STEP" in
  clean)      SCRIPT=clean_batch.py ;;
  indicators) SCRIPT=indicators.py;                 OPTS+=("${JDBC_PKG[@]}") ;;
  pgtest)     SCRIPT=write_indicators_postgres.py;  OPTS+=("${JDBC_PKG[@]}") ;;
  latency)    SCRIPT=measure_latency.py;            OPTS+=("${JDBC_PKG[@]}") ;;
  *) echo "Étape inconnue : $STEP" >&2; exit 1 ;;
esac

if [[ "$STEP" != "clean" && -z "${JDBC_PASSWORD:-}" ]]; then
  echo "ERREUR : JDBC_PASSWORD n'est pas défini (docker exec -e JDBC_PASSWORD=...)." >&2
  exit 1
fi

CMD=(/opt/spark/bin/spark-submit "${OPTS[@]}" "$DIR/$SCRIPT")
if [[ "${DRY_RUN:-0}" == "1" ]]; then
  printf '%q ' "${CMD[@]}"; echo
  exit 0
fi
exec "${CMD[@]}"
