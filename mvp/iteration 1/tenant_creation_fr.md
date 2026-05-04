Il y a deux contextes d'exécution selon l'étape du projet.

---

## En développement local (Itération 1)

**Prérequis — variables d'environnement**

```bash
export POSTGRES_DSN="postgresql://nexus_app:<password>@localhost:5432/nexus_system"
export KAFKA_BOOTSTRAP="localhost:9092"
```

**Exécution directe**

```bash
python scripts/onboard_tenant.py \
  --tenant-id acme-corp \
  --name "Acme Corporation" \
  --plan professional
```

**Vérification immédiate**

```bash
# 1. Ligne tenant active
psql $POSTGRES_DSN -c \
  "SELECT tenant_id, status, activated_at FROM nexus_system.tenants;"

# 2. Les 9 topics Kafka créés
kubectl exec -it nexus-kafka-kafka-0 -n nexus-data -- \
  bin/kafka-topics.sh --bootstrap-server localhost:9092 --list \
  | grep acme-corp

# 3. Version CDM 1.0 présente
psql $POSTGRES_DSN -c \
  "SELECT * FROM nexus_system.cdm_versions WHERE tenant_id = 'acme-corp';"

# 4. Idempotence — relancer, doit se terminer sans erreur
python scripts/onboard_tenant.py \
  --tenant-id acme-corp \
  --name "Acme Corporation" \
  --plan professional
# Résultat attendu : "acme-corp already active — nothing to do"
```

---

## En production (Kubernetes)

Le script ne s'exécute pas directement — il tourne comme un **Kubernetes Job** déclenché par nexus-connector-api via l'API Kubernetes.

**Le Job manifest**

```yaml
# k8s/jobs/onboard-tenant-job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: onboard-tenant-acme-corp          # généré dynamiquement par l'API
  namespace: nexus-app
spec:
  ttlSecondsAfterFinished: 3600           # nettoyage automatique après 1h
  backoffLimit: 3                         # 3 tentatives en cas d'échec
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: onboard-tenant
          image: nexus-core:latest
          command:
            - python
            - scripts/onboard_tenant.py
            - --tenant-id
            - acme-corp                   # injecté par l'API
            - --name
            - "Acme Corporation"
            - --plan
            - professional
          env:
            - name: POSTGRES_DSN
              valueFrom:
                secretKeyRef:
                  name: nexus-postgres-credentials
                  key: dsn
            - name: KAFKA_BOOTSTRAP
              value: "nexus-kafka-kafka-bootstrap.nexus-data:9092"
```

**Comment nexus-connector-api déclenche le Job**

```python
# nexus_core/provisioning.py

from kubernetes import client, config

async def trigger_tenant_provisioning(
    tenant_id: str,
    tenant_name: str,
    plan: str
):
    config.load_incluster_config()   # dans le cluster
    batch_v1 = client.BatchV1Api()

    job = client.V1Job(
        metadata=client.V1ObjectMeta(
            name=f"onboard-tenant-{tenant_id}",
            namespace="nexus-app"
        ),
        spec=client.V1JobSpec(
            ttl_seconds_after_finished=3600,
            backoff_limit=3,
            template=client.V1PodTemplateSpec(
                spec=client.V1PodSpec(
                    restart_policy="OnFailure",
                    containers=[client.V1Container(
                        name="onboard-tenant",
                        image="nexus-core:latest",
                        command=[
                            "python", "scripts/onboard_tenant.py",
                            "--tenant-id",   tenant_id,
                            "--name",        tenant_name,
                            "--plan",        plan
                        ],
                        env=[
                            client.V1EnvVar(
                                name="POSTGRES_DSN",
                                value_from=client.V1EnvVarSource(
                                    secret_key_ref=client.V1SecretKeySelector(
                                        name="nexus-postgres-credentials",
                                        key="dsn"
                                    )
                                )
                            ),
                            client.V1EnvVar(
                                name="KAFKA_BOOTSTRAP",
                                value="nexus-kafka-kafka-bootstrap.nexus-data:9092"
                            )
                        ]
                    )]
                )
            )
        )
    )

    batch_v1.create_namespaced_job(namespace="nexus-app", body=job)
```

**Suivre la progression depuis l'UI admin**

L'UI interroge l'endpoint suivant toutes les 3 secondes jusqu'à ce que `status = 'active'` :

```http
GET /tenants/acme-corp/status
→ { "tenant_id": "acme-corp", "status": "provisioning", "progress": "kafka_topics" }
→ { "tenant_id": "acme-corp", "status": "active", "activated_at": "2026-03-26T..." }
```

**Surveiller le Job depuis kubectl**

```bash
# Statut du job
kubectl get jobs -n nexus-app | grep onboard-tenant-acme-corp

# Logs en temps réel
kubectl logs -n nexus-app \
  -l job-name=onboard-tenant-acme-corp \
  --follow

# En cas d'échec — voir pourquoi
kubectl describe job onboard-tenant-acme-corp -n nexus-app
```

---

## Résumé

| Contexte | Comment lancer | Qui déclenche |
|---|---|---|
| Local / dev | `python scripts/onboard_tenant.py --tenant-id ...` | Développeur manuellement |
| CI/CD (tests) | Même commande dans le pipeline GitHub Actions | Pipeline automatique |
| Production | Kubernetes Job créé par nexus-connector-api | Bouton « Intégrer tenant » dans l'UI admin |