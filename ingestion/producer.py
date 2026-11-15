# ingestion/producer.py
import pandas as pd
import json
import time
import uuid
import argparse
import sys
import os
from datetime import datetime
from kafka import KafkaProducer

# On importe le schéma
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from schema import KAFKA_TOPIC, COLS_TO_EXCLUDE

def create_producer(bootstrap_servers):
    """Crée le producteur Kafka."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )

def process_and_send(file_path, producer, events_per_sec=100, limit=None):
    """Lit le CSV par morceaux et envoie dans Kafka."""
    print(f"Lecture du fichier : {file_path}")
    
    chunksize = 1000 # Lit 1000 lignes à la fois
    total_sent = 0
    start_time = time.time()

    # On lit le CSV par morceaux
    for chunk in pd.read_csv(file_path, chunksize=chunksize):
        
        # 1. NETTOYAGE : Enlever les espaces parasites dans les noms de colonnes
        chunk.columns = chunk.columns.str.strip()
        
        for index, row in chunk.iterrows():
            if limit and total_sent >= limit:
                break

            # 2. Créer un ID unique
            event_id = str(uuid.uuid4())
            
            # 3. Timestamp
            event_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 4. Récupérer le label (BENIGN, DDoS, etc.)
            # On cherche la colonne "Label" dans le CSV
            label = str(row.get('Label', 'UNKNOWN'))
            
            # 5. Extraire les features (toutes les colonnes SAUF celles exclues)
            features = {}
            for col in chunk.columns:
                if col not in COLS_TO_EXCLUDE:
                    val = row[col]
                    # Gérer Infinity / NaN pour que le JSON soit valide
                    if pd.isna(val) or val == float('inf') or val == float('-inf'):
                        features[col] = None
                    else:
                        features[col] = float(val) if isinstance(val, (int, float)) else str(val)

            # 6. Construire le message
            message = {
                "event_id": event_id,
                "event_ts": event_ts,
                "label": label,
                "features": features
            }

            # 7. Envoyer dans Kafka
            producer.send(KAFKA_TOPIC, message)
            total_sent += 1

            # 8. Gérer le débit (Rate Limiting)
            if events_per_sec > 0:
                expected_time = total_sent / events_per_sec
                actual_time = time.time() - start_time
                if actual_time < expected_time:
                    time.sleep(expected_time - actual_time)

            if total_sent % 100 == 0:
                print(f"Progression : {total_sent} événements envoyés...")

        if limit and total_sent >= limit:
            break

    producer.flush()
    print(f"Terminé ! Total envoyé : {total_sent} événements.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Producteur Kafka pour le dataset")
    parser.add_argument("--file", type=str, required=True, help="Chemin vers le CSV (ex: data/raw/mon_fichier.csv)")
    parser.add_argument("--rate", type=int, default=100, help="Événements par seconde (0 pour illimité)")
    parser.add_argument("--limit", type=int, default=None, help="Nombre max d'événements à envoyer")
    parser.add_argument("--broker", type=str, default="localhost:9092", help="Adresse de Kafka")
    
    args = parser.parse_args()

    try:
        producer = create_producer(args.broker)
        process_and_send(args.file, producer, args.rate, args.limit)
    except Exception as e:
        print(f"Erreur : {e}")
        print("Vérifie que ton cluster Docker (Kafka) est bien lancé.")