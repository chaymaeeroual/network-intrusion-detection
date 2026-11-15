import streamlit as st
import pandas as pd
import psycopg2
import time
from datetime import datetime

st.set_page_config(page_title="Network Intrusion Dashboard", layout="wide")
st.title("🛡️ Détection d'Intrusion Réseau - Dashboard")
st.markdown("Visualisation en temps réel des prédictions.")

# Connexion à la base (remplace par les infos de ton .env si besoin)
def get_data():
    try:
        conn = psycopg2.connect(
            host="localhost", # ou 'postgres' si lancé dans Docker
            port="5433", 
            dbname="intrusion_db",
            user="intrusion_user",
            password="ton_mot_de_passe"
        )
        query = "SELECT * FROM intrusion_predictions ORDER BY processed_at DESC LIMIT 100;"
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        return None

df = get_data()

if df is not None and not df.empty:
    # Métriques
    total = len(df)
    attaques = len(df[df['prediction'] > 0.5])
    ratio = (attaques / total) * 100 if total > 0 else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("Événements récents", total)
    col2.metric("Attaques détectées", attaques)
    col3.metric("Ratio d'attaques", f"{ratio:.2f} %")

    # Tableau
    st.subheader("Derniers événements traités")
    st.dataframe(df)
else:
    st.warning("Aucune donnée trouvée. Vérifie la connexion à PostgreSQL ou attends que le modèle de D traite des données.")

# Actualisation auto (toutes les 5 sec)
time.sleep(5)
st.rerun()