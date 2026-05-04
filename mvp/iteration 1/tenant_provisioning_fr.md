## Pourquoi le provisionnement est un prérequis absolu

NEXUS repose sur le principe qu'un tenant n'est pas une entité conceptuelle — c'est une ligne en base de données. Chaque service de la plateforme prend une décision à chaque message ou requête reçue : ce `tenant_id` est-il connu et actif ? Si la ligne n'existe pas, la réponse est toujours non, et la requête ou le message est rejeté.

Trois points d'application rendent cela structurel, pas seulement conventionnel :

**Kong** valide le JWT et extrait le `tenant_id` sous forme d'en-tête `X-Tenant-ID`. Si ce tenant n'existe pas dans `nexus_system.tenants`, le service en aval n'a aucun contexte valide pour fonctionner.

**PostgreSQL RLS** est limité à `current_setting('nexus.current_tenant_id')`. Le rôle base de données `nexus_app` n'est pas superutilisateur. Si aucune ligne tenant n'existe, `get_tenant_scoped_connection()` définira un contexte RLS correspondant à un `tenant_id` sans lignes correspondantes — chaque requête retourne silencieusement zéro résultats. Rien ne fonctionne mais rien n'échoue bruyamment non plus, ce qui est pire.

**Les consommateurs Kafka** appellent `is_active_tenant(tenant_id)` avant de traiter tout message. Un message arrivant pour un `tenant_id` dont le `status != 'active'` est rejeté au niveau du consommateur. Le message est commité et supprimé. Ni le pipeline M1, ni le CDM mapper, ni l'exécuteur de requêtes ne feront quoi que ce soit pour un tenant non provisionné.

La séquence n'est donc pas une convention — c'est une chaîne de dépendances strictes :

```
onboard_tenant.py → status = 'active'
                          ↓
        toute opération ultérieure devient possible
```

---

## Ce que fait concrètement le workflow de provisionnement

`onboard_tenant.py` est un script idempotent unique exécuté une seule fois par nouveau client. Il exécute cinq étapes dans un ordre strict, et ne définit `status = 'active'` qu'après que les cinq ont réussi :

**Étape 1 — Vérification initiale**

Lit `nexus_system.tenants` pour ce `tenant_id`. Si `status = 'active'` est déjà présent, se termine proprement — sans risque de double exécution. Sinon, insère une ligne avec `status = 'provisioning'`. Le tenant existe maintenant en base de données mais aucun service ne traitera quoi que ce soit pour lui.

**Étape 2 — Namespace de topics Kafka**

Crée les neuf topics dynamiques pour ce tenant :

```
{tid}.m1.semantic_interpretation_requested
{tid}.m1.sync_completed
{tid}.m2.semantic_interpretation_complete
{tid}.m2.agent_response_ready
{tid}.m2.workflow_trigger
{tid}.m2.knowledge_query
{tid}.m2.knowledge_query_result
{tid}.m4.mapping_approved
{tid}.m4.workflow_completed
```

Ce sont les canaux d'événements inter-services pour ce client spécifique. Sans eux, nexus-connector-worker n'a aucun topic sur lequel publier les événements de synchronisation, nexus-cdm-mapper n'a rien à consommer, nexus-governance-processor n'a aucun canal pour les mappings approuvés. L'intégralité du pipeline événementiel est cassée.

**Étape 3 — Version CDM initiale**

Insère la version CDM `1.0` pour ce tenant dans `nexus_system.cdm_versions`. C'est l'état de mapping de référence. Le CDM mapper vérifie quelle version CDM est active avant de proposer ou d'appliquer des mappings. Sans cet enregistrement, le mapper n'a pas de contexte de version et ne peut pas produire de propositions.

**Étape 4 — Registre RLS**

Insère une ligne dans `nexus_system.tenant_rls_registry`. C'est la liste d'autorité des identifiants tenant que les politiques RLS sont autorisées à traiter. Le cache `is_active_tenant()` dans `nexus_core` lit depuis cette table comme source de vérité.

**Étape 5 — Activation**

Définit `status = 'active'` et enregistre `activated_at`. C'est le verrou. Seulement après cette étape les consommateurs Kafka accepteront des messages pour ce tenant, Kong laissera passer les requêtes avec ce `X-Tenant-ID`, et les services effectueront un travail utile.

---

## L'ordre de dépendances entre tous les points d'entrée

```
                    onboard_tenant.py
                    (status = 'active')
                           │
          ┌────────────────┼────────────────────┐
          ▼                ▼                    ▼
    Connexion UI       JWT Kong            Consommateurs
    possible           validé              Kafka actifs
          │                │                    │
          ▼                ▼                    ▼
    Dashboard M6      nexus-connector-api  nexus-cdm-mapper
    affiche les       accepte POST         traite les événements
    données tenant    /connectors          pour ce tenant
```

Aucune des trois branches ne fonctionne sans que l'étape de provisionnement soit complétée en premier.

---

## Faut-il le faire avant l'interface utilisateur ?

Oui. La connexion à l'UI elle-même n'est pas bloquée — un utilisateur peut s'authentifier auprès d'Okta indépendamment du fait que son tenant soit provisionné, car Okta ne connaît pas les tenants NEXUS. Mais dès que l'utilisateur authentifié touche un endpoint API NEXUS :

- `GET /connectors` → retourne vide (RLS retourne zéro lignes, ce qui ressemble à l'absence de connecteurs plutôt qu'à une erreur)
- `POST /connectors` → le service écrit dans `nexus_system.connectors` avec `tenant_id = 'acme-corp'`, mais la publication Kafka échoue car le topic n'existe pas encore
- `GET /governance/proposals` → retourne vide
- Toute requête via nexus-query-api → l'exécuteur vérifie le statut tenant, le trouve inactif, rejette

Les modes d'échec sont majoritairement silencieux — les choses semblent fonctionner mais ne retournent aucune donnée — ce qui est plus difficile à diagnostiquer qu'une erreur explicite. Provisionner avant toute interaction avec l'UI élimine entièrement cette classe de problèmes de débogage.

---

## La séquence correcte pour un nouveau client

```
1. Configuration Okta (LEAD-02)
   Créer un utilisateur Okta avec l'attribut nexus_tenant_id = 'acme-corp'

2. onboard_tenant.py (LEAD-00)
   python scripts/onboard_tenant.py \
     --tenant-id acme-corp \
     --name "Acme Corporation" \
     --plan professional

   Vérification :
   SELECT status, activated_at FROM nexus_system.tenants
   WHERE tenant_id = 'acme-corp';
   -- Doit retourner : active | <timestamp>

3. Mettre à jour jwt_issuer dans la table tenants (LEAD-02)
   UPDATE nexus_system.tenants
   SET jwt_issuer = 'https://dev-XXXXXX.okta.com/oauth2/<server-id>'
   WHERE tenant_id = 'acme-corp';

4. Première connexion UI
   L'utilisateur se connecte via Okta → le JWT contient tenant_id = 'acme-corp'
   → Kong valide → X-Tenant-ID: acme-corp injecté
   → RLS scopé → l'UI se charge correctement

5. Enregistrement du connecteur
   L'admin enregistre le premier connecteur via l'UI d'administration
   → nexus-connector-api écrit en base
   → déclenche le DAG Airflow (ou événement Kafka en Itération 1)
   → le profilage de schéma commence
```

Les étapes 1 à 3 sont une configuration opérationnelle sans composant côté utilisateur. L'étape 4 est la première fois que quelqu'un touche l'UI. L'UI ne doit jamais être la première action.

---

## Une implication pratique pour l'UI d'administration M6

Le bouton « Intégrer un nouveau tenant » dans l'écran Tenants de l'UI d'administration ne doit pas appeler `onboard_tenant.py` directement via un endpoint API. La conception correcte est :

- L'UI collecte les informations du tenant et les soumet à nexus-connector-api
- nexus-connector-api valide et déclenche le script de provisionnement comme un DAG Airflow (ou un Job Kubernetes en Itération 1)
- L'UI interroge régulièrement le statut via `nexus_system.tenants.status`
- Ce n'est que lorsque `status = 'active'` que l'UI affiche le tenant comme prêt

Cela donne à l'administrateur une visibilité sur la progression du provisionnement et l'empêche de tenter d'enregistrer des connecteurs avant que le tenant soit entièrement initialisé. L'ensemble du workflow est observable depuis la même interface d'administration, sans nécessiter un accès SSH pour exécuter un script manuellement.