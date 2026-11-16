#!/usr/bin/env bash
# Archive le modèle de C dans HDFS sous /models/<version> (S3), sans jamais écraser une version existante.
# À exécuter DANS le conteneur NameNode (l'image Spark n'a pas la commande hdfs) :
#   docker exec -i -e MODEL_SOURCE_HDFS=hdfs://namenode:9000/models/training/final_pipeline \
#       -e MODEL_VERSION=v1 nid_namenode bash -s < batch/version_model_hdfs.sh
set -euo pipefail
: "${MODEL_SOURCE_HDFS:?Définir MODEL_SOURCE_HDFS (dossier du PipelineModel produit par C)}"
MODEL_VERSION="${MODEL_VERSION:-v1}"
MODEL_ROOT="${MODEL_ROOT_HDFS:-hdfs://namenode:9000/models}"
DEST="${MODEL_ROOT}/${MODEL_VERSION}"

if ! hdfs dfs -test -e "${MODEL_SOURCE_HDFS}"; then
  echo "ERREUR : source introuvable : ${MODEL_SOURCE_HDFS}" >&2
  exit 3
fi
if hdfs dfs -test -e "${DEST}"; then
  echo "ERREUR : la version existe déjà : ${DEST} (choisir une autre MODEL_VERSION)" >&2
  exit 2
fi

echo "Copie ${MODEL_SOURCE_HDFS} -> ${DEST}"
hdfs dfs -mkdir -p "${MODEL_ROOT}"
hdfs dfs -cp "${MODEL_SOURCE_HDFS}" "${DEST}"
hdfs dfs -ls -R "${DEST}"
echo "Modèle archivé : ${DEST}"
