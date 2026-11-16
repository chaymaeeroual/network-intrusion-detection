# Dictionnaire de données – Batch

## Source et schéma

Le schéma ci-dessous est construit à partir de l'en-tête communiqué pour le dataset de flux réseau. Les noms Spark sont normalisés en minuscules, les espaces deviennent `_`, `/` devient `_per_`, et les noms répétés reçoivent un suffixe (`_2`, etc.).

**Attention :** `Fwd Header Length` apparaît deux fois dans l'en-tête fourni. Le script conserve les deux colonnes et nomme la seconde `fwd_header_length_2`. Il faudra confirmer avec l'équipe si cette répétition existe réellement dans le CSV complet ou si elle provient d'une erreur d'export.

## Champs et rôles

| Position | Colonne source | Nom normalisé dans Spark | Type Parquet | Rôle / remarque |
|---:|---|---|---|---|
| 1 | `Flow ID` | `flow_id` | string | Identifiant technique, exclu des features |
| 2 | `Source IP` | `source_ip` | string | Identifiant réseau, exclu des features |
| 3 | `Source Port` | `source_port` | double | Port / protocole, variables réseau à évaluer selon le modèle |
| 4 | `Destination IP` | `destination_ip` | string | Identifiant réseau, exclu des features |
| 5 | `Destination Port` | `destination_port` | double | Port / protocole, variables réseau à évaluer selon le modèle |
| 6 | `Protocol` | `protocol` | double | Port / protocole, variables réseau à évaluer selon le modèle |
| 7 | `Timestamp` | `timestamp` | string | Date/heure, exclue des features |
| 8 | `Flow Duration` | `flow_duration` | double | Feature numérique candidate |
| 9 | `Total Fwd Packets` | `total_fwd_packets` | double | Feature numérique candidate |
| 10 | `Total Backward Packets` | `total_backward_packets` | double | Feature numérique candidate |
| 11 | `Total Length of Fwd Packets` | `total_length_of_fwd_packets` | double | Feature numérique candidate |
| 12 | `Total Length of Bwd Packets` | `total_length_of_bwd_packets` | double | Feature numérique candidate |
| 13 | `Fwd Packet Length Max` | `fwd_packet_length_max` | double | Feature numérique candidate |
| 14 | `Fwd Packet Length Min` | `fwd_packet_length_min` | double | Feature numérique candidate |
| 15 | `Fwd Packet Length Mean` | `fwd_packet_length_mean` | double | Feature numérique candidate |
| 16 | `Fwd Packet Length Std` | `fwd_packet_length_std` | double | Feature numérique candidate |
| 17 | `Bwd Packet Length Max` | `bwd_packet_length_max` | double | Feature numérique candidate |
| 18 | `Bwd Packet Length Min` | `bwd_packet_length_min` | double | Feature numérique candidate |
| 19 | `Bwd Packet Length Mean` | `bwd_packet_length_mean` | double | Feature numérique candidate |
| 20 | `Bwd Packet Length Std` | `bwd_packet_length_std` | double | Feature numérique candidate |
| 21 | `Flow Bytes/s` | `flow_bytes_per_s` | double | Feature numérique candidate |
| 22 | `Flow Packets/s` | `flow_packets_per_s` | double | Feature numérique candidate |
| 23 | `Flow IAT Mean` | `flow_iat_mean` | double | Feature numérique candidate |
| 24 | `Flow IAT Std` | `flow_iat_std` | double | Feature numérique candidate |
| 25 | `Flow IAT Max` | `flow_iat_max` | double | Feature numérique candidate |
| 26 | `Flow IAT Min` | `flow_iat_min` | double | Feature numérique candidate |
| 27 | `Fwd IAT Total` | `fwd_iat_total` | double | Feature numérique candidate |
| 28 | `Fwd IAT Mean` | `fwd_iat_mean` | double | Feature numérique candidate |
| 29 | `Fwd IAT Std` | `fwd_iat_std` | double | Feature numérique candidate |
| 30 | `Fwd IAT Max` | `fwd_iat_max` | double | Feature numérique candidate |
| 31 | `Fwd IAT Min` | `fwd_iat_min` | double | Feature numérique candidate |
| 32 | `Bwd IAT Total` | `bwd_iat_total` | double | Feature numérique candidate |
| 33 | `Bwd IAT Mean` | `bwd_iat_mean` | double | Feature numérique candidate |
| 34 | `Bwd IAT Std` | `bwd_iat_std` | double | Feature numérique candidate |
| 35 | `Bwd IAT Max` | `bwd_iat_max` | double | Feature numérique candidate |
| 36 | `Bwd IAT Min` | `bwd_iat_min` | double | Feature numérique candidate |
| 37 | `Fwd PSH Flags` | `fwd_psh_flags` | double | Feature numérique candidate |
| 38 | `Bwd PSH Flags` | `bwd_psh_flags` | double | Feature numérique candidate |
| 39 | `Fwd URG Flags` | `fwd_urg_flags` | double | Feature numérique candidate |
| 40 | `Bwd URG Flags` | `bwd_urg_flags` | double | Feature numérique candidate |
| 41 | `Fwd Header Length` | `fwd_header_length` | double | Feature numérique candidate |
| 42 | `Bwd Header Length` | `bwd_header_length` | double | Feature numérique candidate |
| 43 | `Fwd Packets/s` | `fwd_packets_per_s` | double | Feature numérique candidate |
| 44 | `Bwd Packets/s` | `bwd_packets_per_s` | double | Feature numérique candidate |
| 45 | `Min Packet Length` | `min_packet_length` | double | Feature numérique candidate |
| 46 | `Max Packet Length` | `max_packet_length` | double | Feature numérique candidate |
| 47 | `Packet Length Mean` | `packet_length_mean` | double | Feature numérique candidate |
| 48 | `Packet Length Std` | `packet_length_std` | double | Feature numérique candidate |
| 49 | `Packet Length Variance` | `packet_length_variance` | double | Feature numérique candidate |
| 50 | `FIN Flag Count` | `fin_flag_count` | double | Feature numérique candidate |
| 51 | `SYN Flag Count` | `syn_flag_count` | double | Feature numérique candidate |
| 52 | `RST Flag Count` | `rst_flag_count` | double | Feature numérique candidate |
| 53 | `PSH Flag Count` | `psh_flag_count` | double | Feature numérique candidate |
| 54 | `ACK Flag Count` | `ack_flag_count` | double | Feature numérique candidate |
| 55 | `URG Flag Count` | `urg_flag_count` | double | Feature numérique candidate |
| 56 | `CWE Flag Count` | `cwe_flag_count` | double | Feature numérique candidate |
| 57 | `ECE Flag Count` | `ece_flag_count` | double | Feature numérique candidate |
| 58 | `Down/Up Ratio` | `down_per_up_ratio` | double | Feature numérique candidate |
| 59 | `Average Packet Size` | `average_packet_size` | double | Feature numérique candidate |
| 60 | `Avg Fwd Segment Size` | `avg_fwd_segment_size` | double | Feature numérique candidate |
| 61 | `Avg Bwd Segment Size` | `avg_bwd_segment_size` | double | Feature numérique candidate |
| 62 | `Fwd Header Length` | `fwd_header_length_2` | double | Feature numérique candidate — doublon de l'en-tête, suffixé _2 |
| 63 | `Fwd Avg Bytes/Bulk` | `fwd_avg_bytes_per_bulk` | double | Feature numérique candidate |
| 64 | `Fwd Avg Packets/Bulk` | `fwd_avg_packets_per_bulk` | double | Feature numérique candidate |
| 65 | `Fwd Avg Bulk Rate` | `fwd_avg_bulk_rate` | double | Feature numérique candidate |
| 66 | `Bwd Avg Bytes/Bulk` | `bwd_avg_bytes_per_bulk` | double | Feature numérique candidate |
| 67 | `Bwd Avg Packets/Bulk` | `bwd_avg_packets_per_bulk` | double | Feature numérique candidate |
| 68 | `Bwd Avg Bulk Rate` | `bwd_avg_bulk_rate` | double | Feature numérique candidate |
| 69 | `Subflow Fwd Packets` | `subflow_fwd_packets` | double | Feature numérique candidate |
| 70 | `Subflow Fwd Bytes` | `subflow_fwd_bytes` | double | Feature numérique candidate |
| 71 | `Subflow Bwd Packets` | `subflow_bwd_packets` | double | Feature numérique candidate |
| 72 | `Subflow Bwd Bytes` | `subflow_bwd_bytes` | double | Feature numérique candidate |
| 73 | `Init_Win_bytes_forward` | `init_win_bytes_forward` | double | Feature numérique candidate |
| 74 | `Init_Win_bytes_backward` | `init_win_bytes_backward` | double | Feature numérique candidate |
| 75 | `act_data_pkt_fwd` | `act_data_pkt_fwd` | double | Feature numérique candidate |
| 76 | `min_seg_size_forward` | `min_seg_size_forward` | double | Feature numérique candidate |
| 77 | `Active Mean` | `active_mean` | double | Feature numérique candidate |
| 78 | `Active Std` | `active_std` | double | Feature numérique candidate |
| 79 | `Active Max` | `active_max` | double | Feature numérique candidate |
| 80 | `Active Min` | `active_min` | double | Feature numérique candidate |
| 81 | `Idle Mean` | `idle_mean` | double | Feature numérique candidate |
| 82 | `Idle Std` | `idle_std` | double | Feature numérique candidate |
| 83 | `Idle Max` | `idle_max` | double | Feature numérique candidate |
| 84 | `Idle Min` | `idle_min` | double | Feature numérique candidate |
| 85 | `Label` | `label` | string | Cible à prédire |

## Identifiants, temps et cible

- Identifiant de flux : `flow_id` (utile pour la traçabilité, à exclure des variables d'entraînement).
- Adresse source : `source_ip` (à exclure des features).
- Adresse destination : `destination_ip` (à exclure des features).
- Temps de l'événement : `timestamp` (à exclure des features ; conserver pour l'analyse temporelle).
- Cible : `label` (BENIGN ou catégorie d'attaque selon le dataset réel).

Les ports et le protocole sont des variables réseau. Leur inclusion dans les features doit être décidée par la personne C et documentée ; ils ne sont pas automatiquement des identifiants personnels du flux.

## Règles de qualité appliquées

1. Normalisation des noms de colonnes et résolution déterministe des doublons de noms.
2. Suppression des espaces en début/fin des valeurs textuelles.
3. Conversion des chaînes vides et des marqueurs `null` en valeurs nulles.
4. Conversion des colonnes autres que les identifiants, le timestamp et la cible en nombres.
5. Conversion des valeurs non numériques, `NaN`, `Infinity` et `-Infinity` en `NULL`.
6. Normalisation de `label` : tout caractère non ASCII devient `-` (le tiret long des labels « Web Attack – … » est l'octet 0x96 en Windows-1252), puis suppression des espaces et passage en majuscules (ex. `WEB ATTACK - BRUTE FORCE`, `DOS HULK`).
7. Suppression des doublons exacts de lignes.

Les ports et le protocole sont stockés en `double` dans le Parquet nettoyé ; les indicateurs PostgreSQL les convertissent en entiers (`BIGINT`).

Le script ne supprime pas arbitrairement les lignes contenant des valeurs nulles : elles restent disponibles pour les analyses, et leur traitement spécifique pourra être décidé avec la personne C.

## Limites et décisions à confirmer

- Le format réel du timestamp et son fuseau horaire doivent être confirmés.
- Le nombre de lignes, la distribution BENIGN/ATTACK, les taux de valeurs nulles et les doublons ne peuvent être calculés qu'à partir du CSV complet.
- La clé métier d'un flux n'a pas été confirmée. Le nettoyage utilise donc les doublons exacts, pas une déduplication sur `flow_id`.
- Le script lit les CSV en `windows-1252` par défaut (encodage connu des fichiers CIC-IDS-2017). À vérifier sur le vrai fichier du jeudi (Web Attacks) : le rapport doit afficher `WEB ATTACK - BRUTE FORCE`. Si les labels contiennent des `?` ou des caractères bizarres, relancer avec `-e CSV_ENCODING=ISO-8859-1`.
- Les colonnes de type identifiant et timestamp sont exclues des features ; la liste finale des features est validée par la personne C.

## Résultats mesurés sur le dataset complet (à remplir après `clean`)

Lancer l'étape `clean` (voir `docs/execution_cluster.md`) : elle affiche une ligne `CLEANING_REPORT={...}`. Reporter ici :

| Mesure | Valeur |
|---|---|
| Fichiers CSV chargés | _à remplir_ |
| Lignes brutes (`raw_rows`) | _à remplir_ |
| Doublons exacts supprimés (`removed_exact_duplicates`) | _à remplir_ |
| Lignes après nettoyage (`clean_rows`) | _à remplir_ |
| Colonnes avec `Infinity` / `NaN` / vides (`invalid_values_by_column`) | _à remplir_ (en pratique `flow_bytes_per_s` et `flow_packets_per_s`) |
| Distribution des labels (`label_distribution`) | _à remplir_ — calculer le % BENIGN / ATTACK |
| Format réel du `timestamp` et fuseau | _à confirmer sur 10 lignes_ |
