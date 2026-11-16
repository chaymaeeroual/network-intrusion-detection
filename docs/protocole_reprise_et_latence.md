# Protocole de reprise et mesure de latence – Tâche B

## A. Test d'arrêt / redémarrage du cluster (S4)

Ce test doit être réalisé avec l'équipe, car l'arrêt du cluster Docker affecte A, C et D.

1. Vérifier que les services Spark, HDFS, Kafka et PostgreSQL sont opérationnels.
2. Exécuter le nettoyage Batch et vérifier que le Parquet nettoyé existe dans HDFS.
3. Exécuter les indicateurs et vérifier les tables PostgreSQL.
4. Noter l'heure UTC, les chemins HDFS et le nombre de lignes avant l'arrêt.
5. Arrêter puis redémarrer le cluster selon le `docker-compose.yml` commun.
6. Vérifier les UIs Spark/HDFS, la présence des fichiers HDFS et la disponibilité de PostgreSQL.
7. Relancer `indicators.py` ; vérifier que les tables sont recalculées sans corruption.
8. Comparer les nombres de lignes et les indicateurs avant/après.

À consigner dans le rapport : commandes exactes, durée d'indisponibilité, présence des données, erreurs rencontrées et résultat du rejeu.

Exemple de vérification HDFS :
```bash
hdfs dfs -ls -R /data/processed/flows_clean
hdfs dfs -du -h /data/processed/flows_clean
```

Exemple de vérification PostgreSQL :
```sql
SELECT * FROM batch_label_indicators ORDER BY flow_count DESC;
```

Ne pas lancer `docker compose down -v` pendant ce test : l'option `-v` supprime les volumes persistants.

## B. Mesure du délai producteur → base

Le timestamp `Timestamp` du CSV représente le temps associé au flux réseau ; il ne mesure pas nécessairement le moment où le producteur Kafka a publié l'événement.

Pour mesurer correctement le délai de bout en bout, A doit transmettre un timestamp de publication (par exemple `producer_timestamp`, en UTC) et le point d'écriture en base doit enregistrer `db_inserted_at`. La latence est :

`latency_seconds = db_inserted_at - producer_timestamp`

Le script `batch/measure_latency.py` calcule le nombre d'événements, la moyenne, la médiane approximative (P50), le P95 et le maximum. Il nécessite une table `ingested_events` alimentée avec ces deux timestamps.

À documenter : unité, fuseau horaire UTC, nombre d'événements mesurés et fenêtre de mesure. Ne pas présenter une latence calculée depuis le timestamp du dataset comme une latence producteur → base.
