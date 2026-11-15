# Le nom du topic Kafka
KAFKA_TOPIC = "network-flows"
SCHEMA = {
    "event_id": "string",       # UUID unique généré par le producteur
    "event_ts": "string",       # Timestamp (ex: "2026-11-15 14:30:00")
    "label": "string",          # Le vrai label (BENIGN ou le type d'attaque)
    "features": "object"        # Un dictionnaire contenant toutes les colonnes du CSV
}

# Colonnes à exclure pour ne pas tricher (identifiants et fuites)
COLS_TO_EXCLUDE = [
    "Flow ID", 
    "Source IP", 
    "Destination IP", 
    "Timestamp", 
    "Label",
    " Label" # Parfois il y a un espace avant
]