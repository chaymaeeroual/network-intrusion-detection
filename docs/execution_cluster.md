# Exécuter la tâche B sur le cluster Docker (Windows, invite de commandes)

Noms vérifiés dans le `docker-compose.yml` : conteneurs `nid_namenode`, `nid_spark_master`, `nid_postgres` ;
HDFS `hdfs://namenode:9000` ; Spark `spark://spark-master:7077` ; PostgreSQL `postgres:5432`
(base `intrusion_db`, utilisateur `intrusion_user`) ; UI HDFS http://localhost:9871 ; UI Spark http://localhost:8080.

> Les commandes `docker` ci-dessous n'ont pas pu être exécutées par l'auteur du code (pas de Docker disponible).
> Le code Python a, lui, été exécuté en local sur un CSV de test, y compris l'export vers un vrai PostgreSQL.

Se placer à la racine du dépôt (`network-intrusion-detection`) pour toutes les commandes.

## 0. Démarrer le cluster

```cmd
docker compose up -d
docker compose ps
```

8 conteneurs doivent être `Up` (le fichier `.env` avec `POSTGRES_PASSWORD` doit exister).

## 1. Mettre les CSV dans HDFS

Utiliser le dossier `TrafficLabelling` **sans espace final** (le zip contient un doublon avec espace : l'ignorer).

```cmd
docker cp "data\raw\labelled_flows\TrafficLabelling\." nid_namenode:/tmp/flows
docker exec nid_namenode bash -c "hdfs dfs -mkdir -p /data/raw/flows && hdfs dfs -put -f /tmp/flows/*.csv /data/raw/flows/ && rm -rf /tmp/flows"
docker exec nid_namenode hdfs dfs -ls -h /data/raw/flows
docker exec nid_namenode hdfs fsck /data/raw/flows -files -blocks
```

Attendu : 8 fichiers, et `fsck` sans « under replicated » (2 réplicas).

## 2. Copier les scripts dans le conteneur Spark

```cmd
docker exec nid_spark_master rm -rf /opt/tache_B_batch
docker cp batch nid_spark_master:/opt/tache_B_batch
```

À refaire après chaque modification d'un script.

## 3. Nettoyage (d'abord sur UN fichier)

Les workers n'ont que 512 Mo : tester d'abord sur un seul jour.

```cmd
docker exec -e RAW_HDFS_PATH=hdfs://namenode:9000/data/raw/flows/Wednesday-workingHours.pcap_ISCX.csv -e CLEAN_HDFS_PATH=hdfs://namenode:9000/data/processed/flows_test nid_spark_master bash /opt/tache_B_batch/run_in_cluster.sh clean
```

Chercher la ligne `CLEANING_REPORT=`. Si le job reste « WAITING » : voir « Problèmes fréquents ».
Quand ça marche, lancer sur tous les fichiers (c'est la valeur par défaut des chemins) :

```cmd
docker exec nid_spark_master bash /opt/tache_B_batch/run_in_cluster.sh clean > docs\clean_log.txt 2>&1
findstr CLEANING_REPORT docs\clean_log.txt > docs\cleaning_report.json
```

Reporter les chiffres dans `docs/dictionnaire_donnees.md` (section « Résultats mesurés »).

## 4. Indicateurs

Récupérer le mot de passe dans ton `.env` local (variable `POSTGRES_PASSWORD`), puis, sans écrire en base :

```cmd
docker exec -e JDBC_PASSWORD=TON_MOT_DE_PASSE nid_spark_master bash /opt/tache_B_batch/run_in_cluster.sh indicators
```

Test de connexion à PostgreSQL (crée la table `batch_connection_test`) :

```cmd
docker exec -e JDBC_PASSWORD=TON_MOT_DE_PASSE nid_spark_master bash /opt/tache_B_batch/run_in_cluster.sh pgtest
```

Créer les tables (une fois), puis écrire les indicateurs :

```cmd
type batch\sql\init_postgres.sql | docker exec -i nid_postgres psql -U intrusion_user -d intrusion_db
docker exec -e JDBC_PASSWORD=TON_MOT_DE_PASSE -e WRITE_POSTGRES=true nid_spark_master bash /opt/tache_B_batch/run_in_cluster.sh indicators
docker exec nid_postgres psql -U intrusion_user -d intrusion_db -c "SELECT * FROM batch_label_indicators ORDER BY flow_count DESC"
```

Ne jamais écrire le vrai mot de passe dans un fichier versionné.

## 5. Versionner le modèle de C (S3)

```cmd
docker exec -i -e MODEL_SOURCE_HDFS=hdfs://namenode:9000/models/training/final_pipeline -e MODEL_VERSION=v1 nid_namenode bash -s < batch\version_model_hdfs.sh
```

Le chemin source est à convenir avec C. Le script refuse d'écraser une version existante.

## 6. Latence (S4)

Quand A remplit `producer_timestamp` et `db_inserted_at` dans `ingested_events` :

```cmd
docker exec -e JDBC_PASSWORD=TON_MOT_DE_PASSE nid_spark_master bash /opt/tache_B_batch/run_in_cluster.sh latency
```

## Problèmes fréquents

| Symptôme | Cause / solution |
|---|---|
| `Initial job has not accepted any resources` | Mémoire demandée > 512 Mo par worker. Le script impose déjà `--executor-memory 384m` ; vérifier que les 2 workers sont `ALIVE` sur http://localhost:8080. |
| `java.lang.OutOfMemoryError` sur les 8 fichiers | Les workers sont trop petits pour 2,8 M de lignes. Lancer fichier par fichier (`CLEAN_HDFS_PATH` différent par jour), ou augmenter `--memory` des workers et `mem_limit` dans le compose avec A. |
| `FileNotFoundException ... .ivy2` | Déjà évité par `spark.jars.ivy=/tmp/.ivy2`. Le premier lancement télécharge le driver PostgreSQL : Internet nécessaire. |
| `password authentication failed` | `JDBC_PASSWORD` différent de `POSTGRES_PASSWORD`. Si tu as changé le mot de passe après le premier démarrage, le volume PostgreSQL garde l'ancien. |
| Labels avec `?` ou caractères bizarres | Relancer avec `-e CSV_ENCODING=ISO-8859-1`. |
| `Path does not exist` | Mauvais `RAW_HDFS_PATH` ; vérifier avec `hdfs dfs -ls /data/raw/flows`. |
