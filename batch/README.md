# Tâche B – Batch (Spark / HDFS / PostgreSQL)

| Fichier | Rôle |
|---|---|
| `batch/common.py` | Lecture CSV robuste (en-tête à doublon, espaces, Windows-1252), nettoyage |
| `batch/clean_batch.py` | CSV brut (HDFS) → Parquet propre (HDFS) + `CLEANING_REPORT` |
| `batch/indicators.py` | 6 indicateurs Spark SQL → tables PostgreSQL `batch_*` |
| `batch/write_indicators_postgres.py` | Test de connexion JDBC |
| `batch/version_model_hdfs.sh` | Archive le modèle de C dans `/models/<version>` (S3) |
| `batch/measure_latency.py` | Délai producteur → base (S4) |
| `batch/run_in_cluster.sh` | Lance un script avec les bons réglages Spark pour ce cluster |
| `batch/sql/init_postgres.sql` | Création des tables d'indicateurs |
| `docs/dictionnaire_donnees.md` | Dictionnaire de données (85 colonnes) |
| `docs/execution_cluster.md` | **Pas à pas pour tout lancer** |
| `docs/protocole_reprise_et_latence.md` | Protocole des tests S4 |

Commencer par `docs/execution_cluster.md`.

## Ce qui a été vérifié, et ce qui ne l'a pas été

Vérifié en local (Spark 3.5.7, PostgreSQL réel) sur un CSV de test de 312 lignes construit à partir de l'en-tête réel (85 colonnes, doublon `Fwd Header Length`, CRLF, Windows-1252, `Infinity`, `NaN`, doublons exacts) : nettoyage, 6 indicateurs, export JDBC relancé deux fois sans doublon ni perte de clé primaire, calcul de latence, logique du versionnage de modèle (avec un faux `hdfs`).

**Non vérifié** : exécution sur le cluster Docker réel (mémoire des workers de 512 Mo, HDFS réel, téléchargement du driver JDBC) et sur le dataset complet de 2,8 M de lignes.
