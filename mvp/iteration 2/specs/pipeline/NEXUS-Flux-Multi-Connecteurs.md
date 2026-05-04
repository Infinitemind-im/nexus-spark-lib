# NEXUS — Flux Multi-Connecteurs : Salesforce + ServiceNow + Odoo + PostgreSQL
**Pipeline unifié : 4 sources → Kafka → Spark → Stores**
Mentis Consulting · Avril 2026 · Confidentiel

---

## Principe fondamental : un seul pipeline, N sources

La force de l'architecture NEXUS est que **l'ajout d'un nouveau connecteur ne nécessite aucune modification du pipeline central** (Spark, M3 Writer, stores). Chaque connecteur est un producteur Kafka autonome qui respecte le contrat d'enveloppe défini en §1.3 de l'INTEGRATION_SPEC.

**Les seuls prérequis pour brancher une nouvelle source :**
1. Implémenter l'enveloppe JSON standard (champs obligatoires)
2. Renseigner un `source_system` unique et un `connector_id` unique
3. Produire sur les topics existants `m1.int.raw_records.legacy` et/ou `m1.int.raw_records.cdc`

**Rien d'autre ne change.**

---

## Vue d'ensemble du flux 4 connecteurs

```
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│    SALESFORCE    │  │   SERVICENOW     │  │      ODOO        │  │   POSTGRESQL     │
│     (CRM)        │  │ (ITSM/Support)   │  │ (ERP/Comptable)  │  │  (Base données)  │
├──────────────────┤  ├──────────────────┤  ├──────────────────┤  ├──────────────────┤
│ • Streaming      │  │ • REST API Table │  │ • JSON-RPC       │  │ • Debezium WAL   │
│   CometD         │  │ • Polling increm.│  │ • Polling increm.│  │   (pgoutput)     │
│ • SOQL Polling   │  │ • Backfill REST  │  │ • Backfill RPC   │  │ • Snapshot init. │
│ • Backfill SOQL  │  │                  │  │                  │  │                  │
│                  │  │                  │  │                  │  │                  │
│ source_system:   │  │ source_system:   │  │ source_system:   │  │ source_system:   │
│  "salesforce"    │  │  "servicenow"    │  │  "odoo"          │  │  "postgres"      │
│ connector_id:    │  │ connector_id:    │  │ connector_id:    │  │ connector_id:    │
│  "sf-prod-01"    │  │  "snow-prod-01"  │  │  "odoo-prod-01"  │  │  "pg-prod-01"    │
└────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘
         │Backfill             │Backfill              │Backfill             │Snapshot
         │CDC                  │Polling               │Polling              │WAL CDC
         │                     │                      │                     │
         ▼                     ▼                      ▼                     ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  (tous les connecteurs produisent sur les DEUX topics indépendamment — aucune relation entre A et B)                     │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
         │Backfill / Snapshot  │                      │                     │ CDC / Polling / WAL
         ▼                     ▼                      ▼                     ▼
┌────────────────────────────────────────────┐    ┌────────────────────────────────────────────────┐
│   Topic A : m1.int.raw_records.legacy      │    │   Topic B : m1.int.raw_records.cdc             │
│   watermark 24h                            │    │   watermark 10min                              │
│                                            │    │                                                │
│  {source_system:"salesforce",              │    │  {source_system:"salesforce",                  │
│   source_table:"account", op:"c"...}       │    │   op:"u", ingestion_mode:"cdc"...}             │
│  {source_system:"servicenow",              │    │  {source_system:"servicenow",                  │
│   source_table:"incident", op:"c"...}      │    │   op:"c", ingestion_mode:"soql_poll"...}       │
│  {source_system:"odoo",                    │    │  {source_system:"odoo",                        │
│   source_table:"res.partner", op:"c"...}   │    │   op:"u", ingestion_mode:"soql_poll"...}       │
│  {source_system:"postgres",                │    │  {source_system:"postgres",                    │
│   source_table:"customers", op:"c"...}     │    │   op:"u", ingestion_mode:"cdc", lsn:...}       │
└───────────────────┬────────────────────────┘    └──────────────────────┬─────────────────────────┘
                    │                                                     │
                    │   Spark lit A et B simultanément — 2 readers        │
                    │   indépendants — aucun union() — watermarks         │
                    │   séparés — chaque message traité dès son arrivée   │
                    │                                                     │
                    └─────────────────────┬───────────────────────────────┘
                                          │
                    ┌─────────────────────▼─────────────────────────────────────────────┐
                    │   SPARK CLASSIFIER                spark/classifier/job.py    │
                    │   PySpark 3.5 Structured Streaming                           │
                    │   2 readers indépendants — pas de union() — watermarks séparés│
                    │                                                              │
                    │  ← Étape 1 — Classification  (sur métadonnées brutes)       │
                    │       🔵 COLD      : fallback OU size > 128 Ko OU batch>500K│
                    │                      → ACK + DROP immédiat                  │
                    │        │
                    │       🔴 HOT        : mode=cdc ET age ≤ 24h OU freq_30j ≥ 3│
                    │       🟡 WARM       : non-HOT ET age ≤ 90 jours             │
                    │                                                              │
                    │  ← Étape 2 — Normalisation  (uniquement HOT/WARM)           │
                    │       • dates           →  ISO-8601                         │
                    │       • montants        →  string → float                   │
                    │       • booléens        →  "true"/"false" → bool            │
                    │       • devises         →  original_currency + fx_rate      │
                    │           │
                    │                        │
                    │                                                              │
                    │  ← Étape 3 — Résolution d'entité  (cdm_entity_id)           │
                    │       Signal A — Déterministe                               │
                    │         exact match sur deterministic_id_columns            │
                    │         (tax_id, domain, duns_number, ...)                  │
                    │         → confidence 1.000 si match → court-circuit B+C    │
                    │       Signal B — Probabiliste  (LSH sur blocking_key)       │
                    │         Jaro-Winkler (noms) · Levenshtein (texte libre)     │
                    │         seuils par tenant+entity type dans er_thresholds    │
                    │         ≥ 0.92 → auto-apply · 0.75–0.92 → er_review_queue  │
                    │       Signal C — Graphe  (si B en zone de revue)            │
                    │         traversée Neo4j 1–2 hops                           │
                    │         +0.05/voisin depth 1 · +0.02/voisin depth 2        │
                    │         plafonné à +0.10 → si score croise 0.92 : auto-apply│
                    │       → cdm_entity_id =                                    │
                    │           gr:sha256(tenant_id||entity_type||blocking_key)  │
                    │           tronqué 128 bits                                 │
                    │       → upsert entity_resolution_index (PG)               │
                    │                                                             │
                    │  ← Étape 4 — Golden Record Synthesis  (cdm_entity_id)      │
                    │       • survivorship rules  (survivorship_rules)            │
                    │           most_recent · source_priority · most_complete     │
                    │           first_observed · manual_override                  │
                    │       • 1 ligne par attribut gagnant →                      │
                    │           golden_record_provenance                          │
                    │       • mise à jour golden_records_index                    │
                    │       • aucune valeur métier stockée (Virtual CDM)          │
                    └────────────┬──────────────────────────────────────────────────┘
                                 │
              ┌──────────────────┼──────────────┐
              ▼                  ▼               ▼
           🔴 HOT             🟡 WARM       ⚫ dead_letter
              │                  │               │
              │                  │          m1.int.dead_letter
              │
              ├──────────────────────────────────────────────────────┐
              │                         │                            │
              ▼                         ▼                            ▼
  m1.int.transformed_     m1.int.transformed_     m1.int.transformed_
   records.hot.neo4j       records.hot.elastic     records.hot.timescale
              │                         │                            │
              └──────────┬──────────────┘                            │
                         │         ┌────────────────────────────────┘
                         ▼         ▼
     ┌──────────────────────────────────────────────────────────────┐
     │                   nexus-m3-writer                            │
     │   consumer Kafka — lit les 3 topics HOT + warm               │
     │   écrit vers les stores (pas Spark)                          │
     └──────────┬──────────────────┬─────────────────┬─────────────┘
                │                  │                  │
                ▼                  ▼                  ▼
           ┌──────┐     ┌───────────────┐   ┌─────────────────┐
           │NEO4J │     │ELASTICSEARCH  │   │  TIMESCALEDB    │
           └──────┘     └───────────────┘   └─────────────────┘

  m1.int.transformed_records.warm → nexus-m3-writer → DELTA LAKE
```

---

## 1. Connecteur Salesforce — CRM

**Répertoire :** `connectors/salesforce/`
**`source_system` :** `salesforce`
**`connector_id` :** `sf-prod-01` (ou `sf-sandbox-01` pour le staging)

### Ce que Salesforce contient

Salesforce est le CRM principal. Il gère :
- **Account** — les comptes clients et partenaires
- **Contact** — les personnes de contact
- **Opportunity** — les opportunités commerciales et leur pipeline
- **Lead** — les prospects non encore qualifiés
- **Case** — les tickets de support client
- **User** — les utilisateurs Salesforce (commercial, support…)
- **Product2** — le catalogue produit

### Mécanismes de collecte

| Mécanisme | Protocole | Topic | `ingestion_mode` |
|-----------|-----------|-------|-----------------|
| **Backfill SOQL** | SOQL REST API, pagination | Topic A | `legacy_batch` |
| **SOQL Polling** | SOQL `WHERE LastModifiedDate > {checkpoint}` | Topic B | `soql_poll` |
| **Streaming CDC** | CometD (Bayeux), canal `/data/ChangeDataCapture` | Topic B | `cdc` |

### Exemple de message produit

```json
{
  "tenant_id": "acme-corp",
  "connector_id": "sf-prod-01",
  "source_system": "salesforce",
  "source_table": "opportunity",
  "source_record_id": "006Dn000000IZ3BIAW",
  "op": "u",
  "event_ts": "2026-04-27T10:00:00Z",
  "ingestion_mode": "cdc",
  "schema_version": "salesforce.opportunity.v12",
  "payload": {
    "Name": "ACME Q3 Deal",
    "Amount": 150000,
    "StageName": "Closed Won",
    "CloseDate": "2026-03-31",
    "AccountId": "001Dn00000ABC123"
  }
}
```

### Résolution d'entité dans NEXUS

L'`Opportunity` Salesforce est mappée vers l'entité CDM `transaction.opportunity`. Le `cdm_entity_id` est calculé par les 3 signaux de résolution (A→B→C) et stocké sous la forme `gr:sha256(tenant_id || entity_type || blocking_key)` tronqué à 128 bits. Upsert dans `entity_resolution_index` (PostgreSQL).

### Température attendue

- Opportunité modifiée aujourd'hui (`op=u`, CDC) → **HOT** → Neo4j + Elasticsearch + TimescaleDB
- Opportunité fermée il y a 30 jours (backfill) → **WARM** → Delta Lake
- Opportunité créée il y a 6 mois jamais retouchée → **COLD** → drop

---

## 2. Connecteur ServiceNow — ITSM

**Répertoire :** `connectors/servicenow/` *(à créer)*
**`source_system` :** `servicenow`
**`connector_id` :** `snow-prod-01`

### Ce que ServiceNow contient

ServiceNow est la plateforme ITSM (IT Service Management). Elle gère :
- **incident** — les incidents IT signalés par les utilisateurs ou alertes automatiques
- **change_request** — les demandes de changement d'infrastructure
- **problem** — les problèmes récurrents liés à plusieurs incidents
- **sc_request** — les demandes de service (onboarding, équipement…)
- **cmdb_ci** — le CMDB (inventaire des actifs IT : serveurs, applications…)
- **sys_user** — les utilisateurs ServiceNow

### Mécanismes de collecte

ServiceNow ne propose pas de CDC natif basé sur un WAL. La collecte se fait par :

| Mécanisme | Protocole | Topic | `ingestion_mode` |
|-----------|-----------|-------|-----------------|
| **Backfill REST** | `GET /api/now/table/{table}?sysparm_limit=1000&sysparm_offset=X` | Topic A | `legacy_batch` |
| **Polling incrémental** | `GET /api/now/table/{table}?sysparm_query=sys_updated_on>={checkpoint}` | Topic B | `soql_poll` |

> **Note :** ServiceNow propose aussi des webhooks (Business Rules + Scripted REST). Si activés, ils peuvent alimenter le Topic B avec une latence < 1s — identique au comportement CDC. `ingestion_mode` devient alors `webhook`.

### Exemple de message produit

```json
{
  "tenant_id": "acme-corp",
  "connector_id": "snow-prod-01",
  "source_system": "servicenow",
  "source_table": "incident",
  "source_record_id": "INC0012345",
  "op": "c",
  "event_ts": "2026-04-27T09:45:00Z",
  "ingestion_mode": "soql_poll",
  "schema_version": "servicenow.incident.v3",
  "payload": {
    "number": "INC0012345",
    "state": "2",
    "priority": "1",
    "category": "network",
    "caller_id": "USR0001234",
    "assigned_to": "USR0005678",
    "opened_at": "2026-04-27T09:30:00Z",
    "cmdb_ci": "CI00089"
  }
}
```

### Résolution d'entité et lien cross-source

ServiceNow partage des entités avec Salesforce :
- Un `caller_id` ServiceNow peut correspondre à un `Contact` Salesforce pour le même client
- Un `cmdb_ci` peut correspondre à un `Product2` Salesforce (serveur acheté)

**Spark résout ces liens via `entity_resolution_index`** : les deux enregistrements pointent vers le même `nexus_id` s'ils ont été précédemment réconciliés.

### Température attendue

- Incident P1 ouvert il y a 2 heures (polling, `event_age=2h`) → **HOT** → stocké dans les 3 stores
- Incident clôturé il y a 15 jours → **WARM** → Delta Lake (pour analyse tendances MTTR)
- Change request archivée depuis 4 mois → **COLD** → drop

---

## 3. Connecteur Odoo — ERP

**Répertoire :** `connectors/odoo/` *(à créer)*
**`source_system` :** `odoo`
**`connector_id` :** `odoo-prod-01`

### Ce que Odoo contient

Odoo est l'ERP open-source. Il gère :
- **res.partner** — les tiers (clients, fournisseurs, contacts) — équivalent du `Account` Salesforce
- **sale.order** — les commandes de vente
- **account.invoice** — les factures (entrantes et sortantes)
- **purchase.order** — les commandes d'achat
- **hr.employee** — les employés
- **product.product** — les produits (SKUs)
- **stock.move** — les mouvements de stock

### Mécanismes de collecte

Odoo expose une API JSON-RPC (`/web/dataset/call_kw`) et XML-RPC sur le port 8069. Il n'y a pas de CDC natif — la collecte est en polling :

| Mécanisme | Protocole | Topic | `ingestion_mode` |
|-----------|-----------|-------|-----------------|
| **Backfill RPC** | `execute_kw(model, 'search_read', [[]], {limit, offset})` | Topic A | `legacy_batch` |
| **Polling incrémental** | `execute_kw(model, 'search_read', [[[write_date,>=,checkpoint]]])` | Topic B | `soql_poll` |

> **Odoo ≥ 16** propose aussi un bus de messagerie interne (`bus.bus`) qui peut être exploité pour des notifications quasi-temps réel. `ingestion_mode` devient `odoo_bus` si activé.

### Exemple de message produit

```json
{
  "tenant_id": "acme-corp",
  "connector_id": "odoo-prod-01",
  "source_system": "odoo",
  "source_table": "sale.order",
  "source_record_id": "SO/2026/00542",
  "op": "u",
  "event_ts": "2026-04-27T11:30:00Z",
  "ingestion_mode": "soql_poll",
  "schema_version": "odoo.sale_order.v16",
  "payload": {
    "name": "SO/2026/00542",
    "state": "done",
    "amount_total": 48500.00,
    "currency_id": [1, "EUR"],
    "partner_id": [142, "ACME SA"],
    "date_order": "2026-04-20T08:00:00"
  }
}
```

### Résolution d'entité et lien cross-source

Le `res.partner` Odoo `142 / ACME SA` correspond très probablement au `Account` Salesforce `001Dn00000ABC123 / ACME SA`. **L'entity resolution Spark** va :
1. **Signal A** — chercher un exact match sur `deterministic_id_columns` (tax_id, domain, duns_number) entre les deux enregistrements
2. **Signal B** — si pas de match déterministe : LSH sur `blocking_key`, Jaro-Winkler sur les noms normalisés
3. **Signal C** — si B en zone de revue (0.75–0.92) : traversée Neo4j pour vérifier les voisins communs
4. Si résolution réussie → même `cdm_entity_id` (`gr:sha256(...)`) que le compte Salesforce — upsert `entity_resolution_index`
5. Si non résolu → nouveau `cdm_entity_id` distinct, marqué `resolution_pending=true` dans `er_review_queue`

### Normalisation devise (§1.5)

Odoo envoie les montants avec `currency_id`. Spark normalise :
- `original_currency: "EUR"`
- `original_amount: 48500.00`
- `fx_rate: 1.0` (si EUR est la devise de référence du tenant)
- `normalized_amount: 48500.00`

### Température attendue

- Commande validée aujourd'hui (polling, `event_age=1h`) → **HOT**
- Commande livrée la semaine dernière (`event_age=7d`) → **WARM** → Delta Lake
- Facture payée il y a 5 mois → **COLD** → drop

---

## 4. Ce qui se passe dans Spark avec 3 sources

```
Topic A (legacy)                    Topic B (cdc/poll)
─────────────────                   ──────────────────
sf-prod-01:   2 232 records         sf-prod-01:   CDC live
snow-prod-01: 15 000 records        snow-prod-01: polling 5min
odoo-prod-01:  8 500 records        odoo-prod-01: polling 15min
pg-prod-01:   12 000 records        pg-prod-01:   WAL CDC live
      │                                    │
      │  (premier arrivé = premier traité — indépendants)
      │                                    │
      ▼                                    ▼
              ┌───────────────────────────────────┐
              │ ÉTAPE 1 — CLASSIFICATION          │
              │ (sur métadonnées brutes)          │
              │  🔵 COLD / ⚫ dead_letter → éjectés│
              │  🔴 HOT  / 🟡 WARM  → continuent  │
              └────────────────┬──────────────────┘
                               │
              ┌────────────────▼──────────────────┐
              │ ÉTAPE 2 — NORMALISATION           │
              │ (uniquement HOT/WARM)             │
              │ • coercition types                │
              │ • normalisation devise            │
              │ • champs inconnus → source_extras │
              │ • CoercionError   → dead_letter   │
              └────────────────┬──────────────────┘
                               │
              ┌────────────────▼──────────────────┐
              │ ÉTAPE 3 — ENTITY RESOLUTION       │
              │ (3 signaux en cascade)            │
              │  Signal A — exact match           │
              │    deterministic_id_columns       │
              │    confidence 1.000 → court-circuit│
              │  Signal B — LSH probabiliste      │
              │    ≥ 0.92 → auto-apply            │
              │    0.75–0.92 → er_review_queue    │
              │  Signal C — Graphe Neo4j 1-2 hops │
              │    si B en zone de revue          │
              │  → cdm_entity_id = gr:sha256(...) │
              │  → upsert entity_resolution_index │
              └────────────────┬──────────────────┘
                               │
              ┌────────────────▼──────────────────┐
              │ ÉTAPE 4 — GOLDEN RECORD SYNTHESIS │
              │  survivorship rules               │
              │  golden_record_provenance         │
              │  golden_records_index             │
              │  Virtual CDM (aucune valeur métier)│
              └────────────────┬──────────────────┘
                               │
       ┌───────────────────────┼──────────────────────────┐
       ▼                       ▼                          ▼
    🔴 HOT                  🟡 WARM             🔵 COLD / ⚫ dead_letter
       │                       │                          │
       │                       │                     drop / m1.int.dead_letter
       │
       ├──→ m1.int.transformed_records.hot.neo4j   ┐
       ├──→ m1.int.transformed_records.hot.elastic  ├─→ nexus-m3-writer → stores
       └──→ m1.int.transformed_records.hot.timescale┘

       m1.int.transformed_records.warm → nexus-m3-writer → Delta Lake
```

---

## 5. Entity Resolution — le cœur du multi-connecteur

C'est ici que la magie opère. Trois sources parlent de la même réalité métier :

```
Salesforce Account "ACME SA"    → nexus_id: "a1b2c3d4-..."
ServiceNow caller "USR0001234"  → nexus_id: "a1b2c3d4-..."  ← MÊME ID
Odoo res.partner/142 "ACME SA"  → nexus_id: "a1b2c3d4-..."  ← MÊME ID
```

**Comment la résolution fonctionne (3 signaux en cascade) :**

1. **Signal A — Déterministe** : exact match sur `deterministic_id_columns` (tax_id, domain, duns_number). `confidence = 1.000` → court-circuit des signaux B et C. Résultat immédiat cross-source.
2. **Signal B — Probabiliste** : LSH sur `blocking_key`. Jaro-Winkler (noms), Levenshtein (texte libre). Score ≥ 0.92 → auto-apply. Score 0.75–0.92 → `er_review_queue`.
3. **Signal C — Graphe** (uniquement si B en zone de revue) : traversée Neo4j 1–2 hops. +0.05/voisin depth 1, +0.02/voisin depth 2, plafonné +0.10. Si score corrigé ≥ 0.92 → auto-apply.

**Résultat :** `cdm_entity_id` = `gr:sha256(tenant_id || entity_type || blocking_key)` tronqué 128 bits. Upsert dans `entity_resolution_index` (PostgreSQL via `nexus-core`).

**Dans Neo4j** : un seul nœud `(:CDMEntity)` avec des edges `(:SourceRef)` pointant vers les IDs sources de chaque connecteur.

**Politique de priorité en cas de conflit de valeur :**

| Priorité | Source | Raison |
|----------|--------|--------|
| 1 | CDC temps réel (`ingestion_mode=cdc`) | Plus fraîche |
| 2 | SOQL Polling (`ingestion_mode=soql_poll`) | Fraîche mais avec délai |
| 3 | Backfill (`ingestion_mode=legacy_batch`) | Historique, potentiellement obsolète |

Si deux sources de même priorité conflictent → `event_ts` le plus récent gagne.

---

## 6. Neo4j avec 3 sources — graphe enrichi

Avec 3 connecteurs, le graphe Neo4j devient beaucoup plus riche :

```cypher
// Entité unique résolue cross-source
(:CDMEntity {
    nexus_id: "a1b2c3d4",
    tenant_id: "acme-corp",
    source_table: "account",     // source primaire (Salesforce, priorité 1)
    connector_id: "sf-prod-01",
    updated_at: "2026-04-27T10:00:00"
})

// Relations cross-entités
(:CDMEntity {id:"a1b2c3d4"})-[:RELATES_TO]->(:CDMEntity {id:"b2c3d4e5"})
// ACME SA (compte) → ACME Q3 Deal (opportunité Salesforce)

(:CDMEntity {id:"a1b2c3d4"})-[:RELATES_TO]->(:CDMEntity {id:"c3d4e5f6"})
// ACME SA (compte) → INC0012345 (incident ServiceNow sur leur infra)

(:CDMEntity {id:"a1b2c3d4"})-[:RELATES_TO]->(:CDMEntity {id:"d4e5f6g7"})
// ACME SA (compte) → SO/2026/00542 (commande Odoo)
```

**Requête possible depuis `nexus-m2-executor` :**
```cypher
MATCH (c:CDMEntity {nexus_id: "a1b2c3d4"})-[:RELATES_TO]->(related)
RETURN related.nexus_id, related.source_table
// → Liste toutes les entités liées au client ACME SA,
//   quelle que soit leur source (SF, SNOW, Odoo)
```

---

## 7. TimescaleDB avec 3 sources — analyse temporelle cross-source

```sql
-- Activité du client ACME SA sur les 30 derniers jours, toutes sources
SELECT
    source_table,
    source_system,    -- champ ajouté dans le message enrichi
    DATE_TRUNC('day', metric_ts) AS jour,
    COUNT(*) AS nb_evenements
FROM business_metrics_raw
WHERE tenant_id = 'acme-corp'
  AND nexus_id = 'a1b2c3d4'
  AND metric_ts >= NOW() - INTERVAL '30 days'
GROUP BY 1, 2, 3
ORDER BY 3 DESC;

-- Résultat attendu :
-- opportunity | salesforce   | 2026-04-27 | 3
-- incident    | servicenow   | 2026-04-27 | 1
-- sale.order  | odoo         | 2026-04-26 | 2
-- ...
```

---

## 8. Delta Lake WARM avec 3 sources — archive analytique

```
/data/delta-lake/
└── acme-corp/
    ├── account/          ← Salesforce Accounts WARM
    │   └── _date_partition=2026-03-01/
    │       └── part-00000.parquet
    ├── incident/         ← ServiceNow Incidents WARM
    │   └── _date_partition=2026-04-10/
    │       └── part-00000.parquet
    ├── sale.order/       ← Odoo Sale Orders WARM
    │   └── _date_partition=2026-04-15/
    │       └── part-00000.parquet
    └── res.partner/      ← Odoo Partners WARM
        └── _date_partition=2026-03-15/
            └── part-00000.parquet
```

---

## 9. Comment ajouter un 4ème connecteur (ex: PostgreSQL)

Si demain on veut brancher une base PostgreSQL interne :

### Étapes

**1. Créer le connecteur**
```
connectors/postgres/
├── main.py           ← backfill + CDC via Debezium ou polling
├── requirements.txt
├── Dockerfile
└── docker-compose.yml
```

**2. Respecter l'enveloppe**
```json
{
  "source_system": "postgres",
  "connector_id": "pg-prod-01",
  "source_table": "public.customer",
  ...
}
```

**3. Démarrer le connecteur**
```powershell
docker compose -f connectors/postgres/docker-compose.yml up -d
```

**C'est tout. Spark, M3 Writer, Neo4j, Elasticsearch, TimescaleDB et Delta Lake traitent automatiquement les messages — aucune modification requise.**

---

## 10. Tableau récapitulatif — 3 connecteurs en production

| Attribut | Salesforce | ServiceNow | Odoo |
|----------|-----------|------------|------|
| `source_system` | `salesforce` | `servicenow` | `odoo` |
| `connector_id` | `sf-prod-01` | `snow-prod-01` | `odoo-prod-01` |
| Protocole CDC | CometD (Bayeux) | Polling REST / Webhook | Polling JSON-RPC |
| Latence CDC | < 5s | 1-5 min (polling) | 5-15 min (polling) |
| Objets principaux | Account, Contact, Opp, Case | incident, change, cmdb_ci | res.partner, sale.order, invoice |
| HOT typique | Opp modifiée aujourd'hui | Incident P1 < 24h | Commande validée < 24h |
| WARM typique | Compte actif < 90j | Incident clôturé < 90j | Facture < 90j |
| COLD typique | Lead archivé > 90j | Change archivée > 90j | Stock dormant > 90j |
| Démarrage | `connectors/salesforce/docker-compose.yml` | `connectors/servicenow/docker-compose.yml` | `connectors/odoo/docker-compose.yml` |
| Réseau Docker | `salesforce_default` | `salesforce_default` | `salesforce_default` |

---

## 11. Ce qui NE change pas avec N connecteurs

| Composant | Impact |
|-----------|--------|
| Topics Kafka A + B | **Inchangés** — tous les connecteurs y produisent |
| Spark Classifier | **Inchangé** — lit les mêmes topics, UDF identique |
| M3 Writer | **Inchangé** — route HOT/WARM/COLD de la même façon |
| Neo4j | **Inchangé** — MERGE idempotent quel que soit `source_system` |
| Elasticsearch | **Inchangé** — index template `nexus-entities-*` couvre toutes les sources |
| TimescaleDB | **Inchangé** — `business_metrics_raw` contient déjà `connector_id` |
| Delta Lake | **Inchangé** — partitions créées automatiquement à la première écriture |
| `INTEGRATION_SPEC.md` | **Aucune modification** des règles de classification |
