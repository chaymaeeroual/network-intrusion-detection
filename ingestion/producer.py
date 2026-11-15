import argparse
import json
import numbers
import os
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from glob import glob

import pandas as pd

# On importe le schéma partagé
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from schema import KAFKA_TOPIC, COLS_TO_EXCLUDE


def create_producer(bootstrap_servers):
    """Crée le producteur Kafka."""
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def list_csv_files(path):
    """Accepte un fichier CSV ou un dossier contenant des CSV (triés par nom)."""
    if os.path.isdir(path):
        files = sorted(glob(os.path.join(path, "*.csv")))
        if not files:
            raise FileNotFoundError(f"Aucun fichier .csv dans le dossier : {path}")
        return files
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Fichier introuvable : {path}")
    return [path]


def clean_value(val):
    """Rend la valeur compatible JSON : NaN / Infinity -> None, nombres -> float."""
    if val is None or (isinstance(val, float) and (val != val or val in (float("inf"), float("-inf")))):
        return None
    if isinstance(val, numbers.Number):
        return float(val)
    if pd.isna(val):
        return None
    return str(val)


def iter_rows(files, skip=0, chunksize=1000):
    """Lit les CSV par morceaux et rend les lignes une à une (en sautant les `skip` premières)."""
    skipped = 0
    for path in files:
        print(f"Lecture du fichier : {path}")
        # encoding_errors="replace" : certains fichiers CIC-IDS-2017 contiennent un octet invalide dans le label
        for chunk in pd.read_csv(path, chunksize=chunksize, low_memory=False, encoding_errors="replace"):
            # 1. NETTOYAGE : enlever les espaces parasites dans les noms de colonnes
            chunk.columns = chunk.columns.str.strip()
            for row in chunk.to_dict("records"):
                if skipped < skip:
                    skipped += 1
                    continue
                yield row


def process_and_send(files, producer, topic, events_per_sec=100, limit=None, skip=0):
    """Envoie les événements dans Kafka et affiche un résumé par label."""
    sent = 0
    labels = Counter()
    start_time = time.time()

    for row in iter_rows(files, skip):
        if limit is not None and sent >= limit:
            break

        # 2. ID unique et timestamp en UTC (les conteneurs Docker travaillent en UTC)
        event_id = str(uuid.uuid4())
        event_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        # 3. Label réel (BENIGN, DDoS, ...) ; le caractère invalide d'un fichier est remplacé par "-"
        label = str(row.get("Label", "UNKNOWN")).replace("�", "-")

        # 4. Features : toutes les colonnes SAUF celles exclues (identifiants, fuites de la cible)
        features = {c: clean_value(v) for c, v in row.items() if c not in COLS_TO_EXCLUDE}

        # 5. Envoi
        producer.send(topic, {"event_id": event_id, "event_ts": event_ts, "label": label, "features": features})
        sent += 1
        labels[label] += 1

        # 6. Débit (0 = illimité)
        if events_per_sec > 0:
            expected = sent / events_per_sec
            actual = time.time() - start_time
            if actual < expected:
                time.sleep(expected - actual)

        if sent % 1000 == 0:
            print(f"Progression : {sent} événements envoyés...")

    producer.flush()
    print(f"Terminé ! Total envoyé : {sent} événements.")
    for label, n in labels.most_common():
        print(f"  {label}: {n}")
    return sent


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Producteur Kafka pour le dataset CIC-IDS-2017")
    parser.add_argument("--file", type=str, required=True, help="Un CSV, ou un dossier contenant des CSV")
    parser.add_argument("--rate", type=int, default=100, help="Événements par seconde (0 pour illimité)")
    parser.add_argument("--limit", type=int, default=None, help="Nombre max d'événements à envoyer")
    parser.add_argument("--skip", type=int, default=0, help="Nombre de lignes à sauter au début")
    parser.add_argument("--topic", type=str, default=KAFKA_TOPIC, help="Topic Kafka")
    parser.add_argument("--broker", type=str, default="localhost:9094", help="Adresse de Kafka vue depuis l'hôte")
    args = parser.parse_args()

    try:
        csv_files = list_csv_files(args.file)
        producer = create_producer(args.broker)
        process_and_send(csv_files, producer, args.topic, args.rate, args.limit, args.skip)
    except Exception as e:
        print(f"Erreur : {e}")
        print("Vérifie que le cluster Docker (Kafka) est lancé et que le chemin du CSV est correct.")
        sys.exit(1)
