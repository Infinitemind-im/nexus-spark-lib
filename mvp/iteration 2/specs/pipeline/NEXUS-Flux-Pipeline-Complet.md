# NEXUS — Flux Pipeline Complet
**Salesforce → Kafka → Spark → Stores (Neo4j / Elasticsearch / TimescaleDB / Delta Lake)**
Mentis Consulting · Avril 2026 · Confidentiel

---

## Vue d'ensemble du flux

```
┌──────────────────────────────────────────────────────────────────────┐
│                    SALESFORCE (Source CRM)                           │
│         Streaming API (CometD)  ────  Backfill SOQL                 │
└──────────────────┬───────────────────────────┬────────────────────┘
                   │ Backfill                  │ CDC / Polling
                   ▼                           ▼
┌──────────────────────────────┐  ┌────────────────────────────────┐
│  Topic A                     │  │  Topic B                       │
│  m1.int.raw_records.legacy   │  │  m1.int.raw_records.cdc        │
│  (historique / backfill)     │  │  (temps réel)                  │
│  watermark : 24 h            │  │  watermark : 10 min            │
└──────────────┬───────────────┘  └──────────────┬─────────────────┘
               │                                  │
               │  premier arrivé = premier traité (streams indépendants)
               │                                  │
               ▼                                  ▼
   ┌───────────────────────────────────────────────────────────────┐
   │  nexus-spark-transformer   (spark/classifier/job.py)          │
   │  PySpark 3.5 Structured Streaming · micro-batch 500 ms        │
   │                                                               │
   │  ← Étape 1 — Classification  (sur métadonnées brutes)         │
   │       🔵 COLD      : fallback OU size > 128 Ko OU batch>500K  │
   │                      → ACK + DROP immédiat                    │
   │       ⚫ dead_letter : enveloppe invalide / champs manquants   │
   │       🔴 HOT        : mode=cdc ET age ≤ 24h OU freq_30j ≥ 3  │
   │       🟡 WARM       : non-HOT  ET age ≤ 90 jours              │
   │                                                               │
   │  ← Étape 2 — Normalisation  (uniquement HOT/WARM)             │
   │       • dates           →  ISO-8601                           │
   │       • montants        →  string → float                     │
   │       • booléens        →  "true"/"false" → bool              │
   │       • devises         →  FXService (ECB, Redis 24h)         │
   │                              original_currency + fx_rate      │
   │       • champs inconnus →  conservés dans source_extras       │
   │       • quality flags   →  null_rate, format_valid par champ  │
   │       • blocking_key    →  formule par entity type            │
   │                              (entity_blocking_rules)          │
   │       • CoercionError   →  dead_letter                        │
   │                                                               │
   │  ← Étape 3 — Résolution d'entité  (cdm_entity_id)            │
   │       Signal A — Déterministe                                 │
   │         exact match sur deterministic_id_columns              │
   │         (tax_id, domain, duns_number, ...)                    │
   │         → confidence 1.000 si match → court-circuit B+C      │
   │       Signal B — Probabiliste  (LSH sur blocking_key)         │
   │         Jaro-Winkler (noms) · Levenshtein (texte libre)       │
   │         seuils par tenant+entity type dans er_thresholds      │
   │         ≥ 0.92 → auto-apply · 0.75–0.92 → er_review_queue    │
   │       Signal C — Graphe  (si B en zone de revue)              │
   │         traversée Neo4j 1–2 hops                              │
   │         +0.05/voisin depth 1 · +0.02/voisin depth 2          │
   │         plafonné à +0.10 → si score croise 0.92 : auto-apply │
   │       → cdm_entity_id =                                       │
   │           gr:sha256(tenant_id||entity_type||blocking_key)     │
   │           tronqué 128 bits                                    │
   │       → upsert entity_resolution_index (PG)                  │
   │                                                               │
   │  ← Étape 4 — Golden Record Synthesis  (cdm_entity_id)        │
   │       • survivorship rules  (survivorship_rules)              │
   │           most_recent · source_priority · most_complete       │
   │           first_observed · manual_override                    │
   │       • 1 ligne par attribut gagnant →                        │
   │           golden_record_provenance                            │
   │           (source_system, source_record_id,                   │
   │            source_attr_value_hash, rule_kind)                 │
   │       • mise à jour golden_records_index                      │
   │       • aucune valeur métier stockée (Virtual CDM)            │
   │       • schema_snapshots mis à jour inline (cardinality/types) │
   └───────┬───────────────────────────────────────────────────────┘
           │
     ┌─────┼──────────┐
     ▼     ▼          ▼
  🔴 HOT  🟡 WARM   ⚫ dead_letter
     │     │              │
     │     │         m1.int.dead_letter
     │     ▼
     │  m1.int.transformed_records.warm
     │     │
     │  ┌──▼──────────────────────┐
     │  │       DELTA LAKE        │
     │  │  (batch > 500k : check- │
     │  │   point avant publish)  │
     │  └─────────────────────────┘
     │
     ├──────────────────────────────────────────────────────────┐
     │                         │                                │
     ▼                         ▼                                ▼
m1.int.transformed_     m1.int.transformed_     m1.int.transformed_
  records.hot.neo4j       records.hot.elastic     records.hot.timescale
     │                         │                                │
     └─────────────┬───────────┘                                │
                   │         ┌──────────────────────────────────┘
                   ▼         ▼
     ┌─────────────────────────────────────────────────────────────┐
     │                   nexus-m3-writer                           │
     │   consumer Kafka — lit les 3 topics HOT + warm              │
     │   écrit vers les stores                         │
     └───────────┬─────────────────┬────────────────┬─────────────┘
                 │                 │                │
                 ▼                 ▼                ▼
           ┌────────┐   ┌───────────────┐   ┌─────────────────┐
           │ NEO4J  │   │ELASTICSEARCH  │   │  TIMESCALEDB    │
           └────────┘   └───────────────┘   └─────────────────┘
```

---

## 1. Source — Salesforce CRM

**Rôle :** Point d'entrée des données métier. Salesforce est le système d'enregistrement (source of truth) pour les entités commerciales : comptes, contacts, opportunités, leads, tickets, produits, utilisateurs.

Le connecteur utilise **trois mécanismes de collecte complémentaires** pour garantir la complétude des données :

### 1.1 Streaming API — CometD (CDC temps réel)

- **Protocole :** CometD (Bayeux), implémenté en Python via `requests` + long-polling HTTP.
- **Canal écouté :** `/data/ChangeDataCapture` sur chaque objet Salesforce activé pour le CDC.
- **Événements reçus :** `CREATE`, `UPDATE`, `DELETE`, `UNDELETE` avec le payload complet de l'enregistrement modifié.
- **Latence :** quasi-temps réel, typiquement < 5 secondes après la modification dans Salesforce.
- **Usage :** alimente le **Topic B** (`m1.int.raw_records.cdc`) avec les événements CDC.

### 1.2 SOQL Polling (delta incrémental)

- **Mécanisme :** requête SOQL avec `WHERE LastModifiedDate > {checkpoint}`, exécutée toutes les N minutes.
- **Rôle :** filet de sécurité pour les enregistrements modifiés non captés par le Streaming API (ex. : modifications en masse via Salesforce Data Loader, imports API, triggers automatiques).
- **Checkpoint :** stocké localement (fichier JSON) — survit aux redémarrages du connecteur.
- **Usage :** alimente également le **Topic B** avec un `ingestion_mode: soql_poll`.

### 1.3 Backfill SOQL (historique complet)

- **Déclenchement :** mode `--mode full` au premier lancement, ou sur demande.
- **Mécanisme :** pagination SOQL complète sur tous les objets configurés, sans filtre de date — récupère l'intégralité de l'historique Salesforce.
- **Volume observé :** 2 232 enregistrements sur l'environnement de test (`acme-corp`).
- **Usage :** alimente le **Topic A** (`m1.int.raw_records.legacy`) avec un `ingestion_mode: legacy_batch`.

---

## 2. Connecteur Salesforce → Kafka

**Répertoire :** `connectors/salesforce/`
**Image Docker :** `python:3.11-slim`
**Réseau Docker :** `salesforce_default`

| Fichier | Rôle |
|---------|------|
| `main.py` | Point d'entrée CLI — orchestre Phase 1 (backfill) puis Phase 2 (CDC + polling) |
| `soql_backfill.py` | Pagination SOQL complète → Topic A |
| `soql_polling.py` | Delta incrémental planifié → Topic B |
| `streaming_cdc.py` | Écoute CometD temps réel → Topic B |
| `kafka_producer.py` | Producteur Kafka (`kafka-python`) — sérialisation JSON + clé `tenant_id:source_table:record_id` |
| `soql_client.py` | Client SOQL avec OAuth2 et gestion des tokens |
| `checkpoint.py` | Persistence du curseur de polling (fichier JSON local) |
| `models.py` | Dataclasses `RawRecord`, `KafkaMessage` |
| `config.py` | Lecture des variables d'environnement |

**Format du message produit (JSON) :**
```json
{
  "tenant_id": "acme-corp",
  "connector_id": "salesforce",
  "source_table": "account",
  "source_id": "001Dn00000XxxxxXXX",
  "op": "c",
  "ingestion_mode": "legacy_batch",
  "event_ts": "2026-04-27T10:00:00Z",
  "payload": { "...champs Salesforce bruts..." }
}
```

---

## 3. Kafka — Bus de messages

**Image :** `confluentinc/cp-kafka:7.6.1` en mode KRaft (sans ZooKeeper)
**CLUSTER_ID :** `xtzWnoRRRiacfFuGqx6T0Q`

### Topic A — `m1.int.raw_records.legacy`
- **Contenu :** enregistrements historiques issus du backfill SOQL
- **`ingestion_mode` :** `legacy_batch`
- **Volume :** 2 232 messages (environnement de test)
- **Rétention :** 7 jours (défaut Kafka)
- **Rôle :** permet de reconstruire l'état initial de la base CDM à partir de données existantes sans attendre du CDC

### Topic B — `m1.int.raw_records.cdc`
- **Contenu :** événements CDC et delta de polling temps réel
- **`ingestion_mode` :** `cdc` ou `soql_poll`
- **Rôle :** flux principal de mise à jour continue de la base CDM

---

## 4. Spark Classifier — Moteur de classification

**Fichier principal :** `spark/classifier/job.py`
**Moteur :** PySpark 3.5.8 Structured Streaming (`local[*]`)
**Image Docker :** `python:3.11-slim` + Java 21 + JARs Kafka Scala 2.12

> **Contrainte critique :** PySpark est épinglé à `>=3.5.0,<4.0.0` pour éviter l'incompatibilité Scala 2.13 de Spark 4.x avec les JARs Kafka compilés en Scala 2.12.

### 4.1 Lecture des topics (§2.3)

Spark ouvre **deux readers Structured Streaming indépendants** — un sur Topic A, un sur Topic B. Chaque message est traité dès son arrivée, sans attendre l'autre topic. Il n'y a **pas de `union()`** : les deux streams sont consommés en parallèle avec des watermarks adaptés à leur nature :

- **Topic A (legacy) :** watermark de 24 heures (données historiques potentiellement vieilles)
- **Topic B (CDC) :** watermark de 10 minutes (données fraîches)

### 4.2 Classification température (§2.2 — UDF Python)

Première étape de traitement, effectuée **sur les métadonnées brutes** (avant toute normalisation). Cela évite de consommer du CPU sur des records qui seront dropés.

La classification est effectuée par `TemperatureClassifier.classify()`, via une UDF Spark sérialisée. L'ordre d'évaluation est strict :

| Température | Condition | Action |
|-------------|-----------|--------|
| 🔵 **COLD** | Fallback OU size > 128 Ko OU batch > 500K lignes | ACK + DROP immédiat — aucune normalisation |
| ⚫ **dead_letter** | Enveloppe invalide / champs manquants | Routé vers `m1.int.dead_letter` |
| 🔴 **HOT** | `ingestion_mode == cdc` ET `age ≤ 24h` OU `fréquence_30j ≥ 3` | Continue vers normalisation |
| 🟡 **WARM** | Non-HOT ET `age ≤ 90 jours` | Continue vers normalisation |

### 4.3 Normalisation (§1.4)

Appliquée **uniquement aux records HOT et WARM**. Chaque message passe par `RecordNormalizer.normalize()` :
- Dates → ISO-8601
- Montants → `string → float`
- Booléens → `"true"/"false" → bool`
- Devises → `original_currency + fx_rate`
- Champs inconnus → conservés dans `source_extras`
- `CoercionError` → routé vers `dead_letter`

### 4.4 Résolution d'entité (§2.5)

Après normalisation, les données sont propres. Chaque record passe par `EntityResolver.resolve()` selon 3 signaux en cascade :

- **Signal A — Déterministe** : exact match sur `deterministic_id_columns` (tax_id, domain, duns_number). Si match → `confidence = 1.000`, court-circuit des signaux B et C.
- **Signal B — Probabiliste** : LSH sur `blocking_key`. Jaro-Winkler (noms), Levenshtein (texte libre). Seuils par tenant+entity type dans `er_thresholds`. Score ≥ 0.92 → auto-apply. Score 0.75–0.92 → `er_review_queue`.
- **Signal C — Graphe** (uniquement si B en zone de revue) : traversée Neo4j 1–2 hops. +0.05/voisin depth 1, +0.02/voisin depth 2, plafonné à +0.10. Si score corrigé ≥ 0.92 → auto-apply.

**Résultat :**
- `cdm_entity_id` = `gr:sha256(tenant_id || entity_type || blocking_key)` tronqué à 128 bits
- Upsert dans `entity_resolution_index` (PostgreSQL via `nexus-core`)

### 4.5 Publication par température (§2.4)

**Spark publie uniquement vers des topics Kafka — il n'écrit dans aucun store.** Le routing vers les stores est la responsabilité de `nexus-m3-writer` (§5).

Spark écrit vers **5 topics Kafka de sortie** :

| Topic | Contenu |
|-------|---------|
| `m1.int.transformed_records.hot.neo4j` | Enregistrements HOT — consommé par `nexus-m3-writer` → Neo4j |
| `m1.int.transformed_records.hot.elastic` | Enregistrements HOT — consommé par `nexus-m3-writer` → Elasticsearch |
| `m1.int.transformed_records.hot.timescale` | Enregistrements HOT — consommé par `nexus-m3-writer` → TimescaleDB |
| `m1.int.transformed_records.warm` | Enregistrements WARM — consommé par `nexus-m3-writer` → Delta Lake |
| `m1.int.dead_letter` | Messages en erreur (parsing / coercition / COLD drop) |

**Format du message enrichi (JSON) :**
```json
{
  "tenant_id": "acme-corp",
  "connector_id": "salesforce",
  "source_table": "account",
  "nexus_id": "uuid-v5-déterministe",
  "source_ref": "001Dn00000XxxxxXXX",
  "temperature_class": "hot",
  "reason_codes": ["cdc_event", "age_lt_24h"],
  "ingestion_mode": "cdc",
  "cdm_version": "2.0",
  "event_ts": "2026-04-27T10:00:00Z",
  "spark_job_id": "uuid-du-job",
  "transformation_ms": 12
}
```

---

## 5. M3 Writer — Routeur vers les Stores

**Répertoire :** `m3-writer/`
**Fichier principal :** `m3-writer/main.py`
**Image Docker :** `python:3.11-slim`

Le M3 Writer est un **consumer Kafka** qui souscrit aux 4 topics de sortie Spark et route chaque message vers le store approprié. Il fonctionne en **mode batch** : il accumule jusqu'à `M3_BATCH_SIZE` messages (défaut : 50) avant de flusher vers les stores, pour réduire la pression réseau.

**Topics consommés et routing :**

| Topic Kafka | Action |
|-------------|---------|
| `m1.int.transformed_records.hot.neo4j` | → **Neo4j** |
| `m1.int.transformed_records.hot.elastic` | → **Elasticsearch** |
| `m1.int.transformed_records.hot.timescale` | → **TimescaleDB** |
| `m1.int.transformed_records.warm` | → **Delta Lake** |

> COLD = ACK + DROP dans Spark directement — aucun topic COLD n'est publié, `nexus-m3-writer` n'intervient pas pour les records COLD.

**Principe de résilience :** une erreur sur un store HOT n'empêche pas les deux autres d'écrire. Chaque writer est enveloppé dans un `try/except` isolé — une panne Neo4j n'impacte ni Elasticsearch ni TimescaleDB.

---

## 6. COLD — Acknowledge + Drop

**Rôle :** les enregistrements COLD (age > 90 jours, non-fréquents) ne présentent pas d'intérêt immédiat pour les moteurs IA de NEXUS. Les écrire dans un store coûterait en ressources sans valeur ajoutée.

**Comportement :**
- **Spark** détecte la classification COLD dans `TemperatureClassifier.classify()`
- Spark log en DEBUG l'entité (`entity_id`, `tenant_id`, `source_table`)
- Spark commit l'offset Kafka — le message est acquitté définitivement
- **Aucun topic n'est publié pour les records COLD** — il n'existe pas de topic `m1.int.transformed_records.cold`
- **`nexus-m3-writer` n'intervient pas** pour les records COLD
- **Aucune écriture en base, aucun stockage**

> Les enregistrements COLD peuvent être reclassifiés HOT ou WARM lors d'une prochaine ingestion si l'entité est de nouveau modifiée dans Salesforce (le CDC remettra alors à jour sa température).

---

## 7. WARM → Delta Lake

**Fichier :** `m3-writer/writers/delta_writer.py`
**Technologie :** `deltalake` (Python pur, sans JVM) + `pyarrow`
**Chemin de stockage :** `/data/delta-lake/{tenant_id}/{source_table}/`

### Pourquoi Delta Lake ?

Delta Lake est un format de table transactionnel au-dessus de Parquet. Il apporte :
- **ACID transactions** — pas de corruption même en cas de crash pendant l'écriture
- **Schema evolution** — nouveaux champs Salesforce automatiquement intégrés (`schema_mode=merge`)
- **Time travel** — possibilité de relire les données à un instant T pour des retraitements
- **Partitionnement** — chaque partition = `tenant_id / source_table / date` pour des lectures analytiques efficaces

### Comportement d'écriture

- **Mode :** `append` — les données ne sont jamais écrasées, chaque batch Kafka est ajouté
- **Idempotence :** gérée par les offsets Kafka — si le M3 Writer redémarre, il reprend depuis le dernier offset committé sans re-écrire les messages déjà traités
- **Partitionnement :** `["tenant_id", "source_table", "_date_partition"]` (YYYY-MM-DD)
- **Schéma Arrow typé :** tous les champs sont fortement typés (pas de `text` générique)

### Champs stockés dans Delta Lake

| Champ | Type | Description |
|-------|------|-------------|
| `tenant_id` | string | Identifiant du tenant |
| `nexus_id` | string | ID CDM déterministe (UUID v5) |
| `source_table` | string | Objet Salesforce source (`account`, `contact`…) |
| `connector_id` | string | Identifiant du connecteur (`salesforce`) |
| `source_ref` | string | ID Salesforce original |
| `ingestion_mode` | string | `legacy_batch`, `cdc`, `soql_poll` |
| `temperature_class` | string | `warm` |
| `reason_codes` | string | JSON array sérialisé des raisons de classification |
| `cdm_version` | string | Version du CDM au moment de l'ingestion |
| `event_ts` | string | Timestamp de l'événement Salesforce |
| `written_at` | string | Timestamp d'écriture dans Delta Lake |
| `_date_partition` | string | YYYY-MM-DD — clé de partition |

> **Virtual CDM :** aucune valeur métier (`name`, `amount`, `email`…) n'est stockée dans Delta Lake. Seules les métadonnées de référence et de classification sont conservées.

---

## 8. HOT → Neo4j (Graph Store)

**Fichier :** `m3-writer/writers/neo4j_writer.py`
**Technologie :** Neo4j 5.x Community + driver Python officiel
**Port :** Bolt `7687`, Browser `7474`

### Rôle dans NEXUS

Neo4j stocke le **graphe de relations entre entités CDM**. Il permet à `nexus-m2-executor` de répondre à des requêtes de type :
- « Quels contacts sont liés à ce compte ? »
- « Quelle est la hiérarchie de cet utilisateur ? »
- « Quelles opportunités sont associées à ce produit ? »

### Modèle de données

```
(:CDMEntity {
    nexus_id: "uuid-v5",
    tenant_id:     "acme-corp",
    source_table:  "account",
    connector_id:  "salesforce",
    source_ref:    "001Dn00000XxxxxXXX",
    created_at:    "2026-04-27T10:00:00",
    updated_at:    "2026-04-27T10:00:00"
})-[:RELATES_TO {
    since:        "2026-04-27T10:00:00",
    connector_id: "salesforce"
}]->(:CDMEntity { nexus_id: "uuid-v5-lié", ... })
```

### Règle Virtual CDM — strictement appliquée

**Aucune valeur métier n'est écrite dans les nœuds Neo4j.** Pas de `name`, `email`, `amount`, `title`, `segment`. Les nœuds ne contiennent que des identifiants et métadonnées structurelles. Si un attaquant contourne OPA (le moteur de politiques d'accès), il ne récupère que des paires d'IDs opaques — aucune donnée exploitable.

### Idempotence

L'upsert utilise le pattern Cypher `MERGE ... ON CREATE SET ... ON MATCH SET ...` — réexécuter la même requête sur le même `nexus_id` ne crée pas de doublon et met à jour seulement `updated_at`.

---

## 9. HOT → Elasticsearch (Search Store)

**Fichier :** `m3-writer/writers/elastic_writer.py`
**Technologie :** Elasticsearch 8.x (single-node pour le développement)
**Port :** `9200`

### Rôle dans NEXUS

Elasticsearch stocke un **index de référence des entités HOT** pour permettre :
- La recherche full-text sur les identifiants et métadonnées
- La découverte rapide d'entités par `tenant_id`, `source_table`, `temperature_class`
- L'alimentation des dashboards de supervision (Kibana optionnel)

### Structure des index

Un index par combinaison `tenant_id + source_table` :
```
nexus-entities-acme-corp-account
nexus-entities-acme-corp-contact
nexus-entities-acme-corp-opportunity
...
```

Un **index template** est créé au démarrage du writer pour appliquer automatiquement le bon mapping sur tout nouvel index correspondant au pattern `nexus-entities-*`.

### Idempotence

L'indexation utilise `_id = nexus_id` — indexer deux fois le même `nexus_id` écrase le document existant sans créer de doublon.

### Champs indexés

| Champ | Type ES | Description |
|-------|---------|-------------|
| `nexus_id` | keyword | ID CDM (clé de document) |
| `tenant_id` | keyword | Tenant |
| `source_table` | keyword | Objet Salesforce |
| `connector_id` | keyword | Connecteur source |
| `source_ref` | keyword | ID Salesforce original |
| `temperature_class` | keyword | `hot` |
| `reason_codes` | keyword | Raisons de classification |
| `ingestion_mode` | keyword | Mode d'ingestion |
| `cdm_version` | keyword | Version CDM |
| `event_ts` | date | Timestamp événement |
| `indexed_at` | date | Timestamp d'indexation |

> **Virtual CDM :** identique à Neo4j — aucune valeur métier dans l'index.

---

## 10. HOT → TimescaleDB (Time-Series Store)

**Fichier :** `m3-writer/writers/timescale_writer.py`
**Technologie :** TimescaleDB (extension PostgreSQL) sur PostgreSQL 16
**Port :** `5432`

### Rôle dans NEXUS

TimescaleDB stocke les **métriques d'activité temporelles** des entités HOT. Il permet à `nexus-m2-executor` de répondre à des requêtes analytiques de type :
- « Combien d'événements HOT par jour sur le compte X sur les 30 derniers jours ? »
- « Quelle est la tendance d'activité de ce lead cette semaine ? »
- « Quelles entités ont eu le plus d'activité ce mois ? »

### Structure de la hypertable

```sql
CREATE TABLE business_metrics_raw (
    tenant_id        TEXT         NOT NULL,
    nexus_id    TEXT         NOT NULL,
    source_table     TEXT         NOT NULL,
    connector_id     TEXT         NOT NULL,
    metric_name      TEXT         NOT NULL DEFAULT 'entity_event',
    metric_value     DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    metric_ts        TIMESTAMPTZ  NOT NULL,   -- clé de partitionnement temporel
    temperature_class TEXT        NOT NULL,
    ingestion_mode   TEXT         NOT NULL,
    cdm_version      TEXT         NOT NULL,
    is_correction    BOOLEAN      NOT NULL DEFAULT FALSE,
    is_deletion      BOOLEAN      NOT NULL DEFAULT FALSE
);
-- Hypertable partitionnée automatiquement par metric_ts (chunks de 7 jours)
SELECT create_hypertable('business_metrics_raw', 'metric_ts');
```

### Idempotence

```sql
INSERT INTO business_metrics_raw (...)
VALUES (...)
ON CONFLICT (tenant_id, nexus_id, metric_ts)
WHERE NOT is_correction AND NOT is_deletion
DO NOTHING;
```

### Stratégie de suppression — Tombstone

Quand une entité est supprimée dans Salesforce, une **ligne de tombstone** est insérée (pas de DELETE) :
```
is_deletion = TRUE, metric_value = 0.0
```
Cela préserve l'historique complet et garantit l'auditabilité — aucune donnée n'est jamais physiquement effacée de la time-series.

---

## 11. Récapitulatif des services Docker

| Service | Image | Port(s) | Rôle |
|---------|-------|---------|------|
| `kafka` | `confluentinc/cp-kafka:7.6.1` | `9092` | Bus de messages central |
| `salesforce-connector` | `python:3.11-slim` | — | Ingestion Salesforce → Kafka |
| `spark-classifier` | `python:3.11-slim` + Java 21 | `4040` (Spark UI) | Classification température |
| `nexus-m3-writer` | `python:3.11-slim` | — | Routage Kafka → Stores |
| `nexus-neo4j` | `neo4j:5.18-community` | `7474`, `7687` | Graph Store |
| `nexus-elasticsearch` | `elasticsearch:8.13.4` | `9200` | Search Store |
| `nexus-timescaledb` | `timescale/timescaledb:latest-pg16` | `5432` | Time-Series Store |
| *(futur)* Delta Lake | volume local | — | Analytical Store (WARM) |

**Réseau :** tous les services partagent le réseau Docker `salesforce_default`.

---

## 12. Commandes de démarrage

```powershell
# 1. Démarrer Kafka + Connecteur Salesforce (backfill + CDC)
docker compose -f connectors/salesforce/docker-compose.yml up -d

# 2. Démarrer Spark Classifier
docker compose -f spark/docker-compose.yml up -d

# 3. Démarrer les Stores + M3 Writer
docker compose -f m3-writer/docker-compose.yml up -d

# 4. Vérifier les offsets Kafka (flux de données)
docker exec salesforce-kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --time -1

# 5. Vérifier Neo4j
# http://localhost:7474  →  MATCH (e:CDMEntity) RETURN e LIMIT 25

# 6. Vérifier Elasticsearch
# GET http://localhost:9200/nexus-entities-acme-corp-account/_count

# 7. Vérifier TimescaleDB
# psql -h localhost -U nexus -d nexus -c \
#   "SELECT source_table, COUNT(*) FROM business_metrics_raw GROUP BY 1 ORDER BY 2 DESC;"

# 8. Suivre les logs du M3 Writer
docker logs nexus-m3-writer -f
```

---

## 13. Garanties du système

| Propriété | Mécanisme |
|-----------|-----------|
| **Pas de perte de données** | `startingOffsets=earliest` sur Spark + `auto_offset_reset=earliest` sur M3 Writer |
| **Idempotence Neo4j** | `MERGE` Cypher — rejeu sans doublon |
| **Idempotence Elasticsearch** | `_id = nexus_id` — écrasement idempotent |
| **Idempotence TimescaleDB** | `ON CONFLICT DO NOTHING` |
| **Idempotence Delta Lake** | Offsets Kafka — pas de re-lecture après commit |
| **Isolation des pannes** | Chaque writer HOT est isolé dans un `try/except` indépendant |
| **Virtual CDM** | Aucune valeur métier dans aucun store (Neo4j, ES, TimescaleDB, Delta Lake) |
| **Audit complet** | Tombstone TimescaleDB — aucune suppression physique |
