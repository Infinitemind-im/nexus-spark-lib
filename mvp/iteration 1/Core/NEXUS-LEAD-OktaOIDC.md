# NEXUS — Task Plan: Okta OIDC Config + OIDC_ISSUER_URL
**Task ID: LEAD-02-C · Owner: Tech Lead · Days 6–8 of Week 1 (after nexus_core library)**
**Mentis Consulting · February 2026 · Confidential**

---

## What This Task Is and Why It Exists

NEXUS is enterprise middleware. Every enterprise it deploys into already has an Identity Provider — Okta, Azure AD, Google Workspace, Ping Identity. NEXUS must integrate with whichever IdP the client uses, not impose its own authentication system. Building a custom auth system would be a security liability, a maintenance burden, and a sales blocker.

This task establishes the identity foundation for all of Iteration 1:

- A running Okta developer org for development and demo use
- NEXUS registered as an OIDC application in that org
- `tenant_id` as a custom claim carried inside every JWT so Kong can extract and inject it as a header
- `OIDC_ISSUER_URL` wired into `nexus_core` and Kong — the single environment variable that makes NEXUS IdP-agnostic
- Test users and groups in Okta that all team members use during Iteration 1

Without this task, no team member can generate a valid JWT, Kong has no issuer to validate against, and every API call returns 401. The M6 Next.js login flow cannot be built. The acceptance tests in M4-01, M4-02, and M4-03 all depend on `$TEST_JWT` — that token comes from this task.

### The IdP-Agnostic Architecture

```
Client enterprise's IdP (Okta / Azure AD / Google Workspace / any OIDC)
    ↓  issues JWT with standard claims + NEXUS custom claim:
    {
      "sub":        "user-okta-id",          ← becomes X-User-ID
      "email":      "alice@acme.com",         ← becomes X-User-Email
      "tenant_id":  "acme-corp",             ← custom claim → X-Tenant-ID
      "exp":        1735689600
    }
    ↓  user sends JWT in Authorization: Bearer header
Kong (API Gateway) — configured with OIDC_ISSUER_URL
    ↓  validates JWT signature against IdP's JWKS endpoint
    ↓  verifies expiry, issuer, audience
    ↓  extracts claims, injects as HTTP headers:
    X-User-ID:    user-okta-id
    X-User-Email: alice@acme.com
    X-Tenant-ID:  acme-corp
    ↓  forwards request to application service
Application service (M2, M4, M6 API routes)
    ↓  reads nexus_core.identity.get_user_identity(headers)
    ↓  never sees the JWT, never decodes it
```

Swapping the IdP from the dev Okta org to a client's Azure AD is one environment variable change. No code changes anywhere.

---

## Scope

**In scope:**
- Okta developer org registration at `developer.okta.com`
- NEXUS OIDC application registered in Okta (Authorization Code flow + PKCE)
- `tenant_id` custom claim added to Okta JWT via an Authorization Server claim rule
- `OIDC_ISSUER_URL` environment variable defined in `nexus_core/oidc.py`
- Kong JWT plugin configured to validate against the Okta issuer's JWKS endpoint
- Kong header injection: `X-User-ID`, `X-User-Email`, `X-Tenant-ID`
- Test users and groups created in Okta (`test-alpha` tenant, `test-beta` tenant)
- Test JWT generation script for all team members
- Kubernetes Secret for Kong OIDC config
- Verification that M6 `next-auth` can use the same OIDC issuer URL

**Not in scope (Iteration 2):**
- Client-specific Okta orgs onboarding (documented pattern only)
- Azure AD and Google Workspace integration testing
- Okta Groups → NEXUS RBAC mapping (the `identity_mapping` table is seeded but not enforced)
- MFA enforcement
- Okta Workflows automation for user provisioning

---

## Dependencies

| Dependency | Owner | Must be done before |
|---|---|---|
| `nexus_core` library skeleton exists | Tech Lead | LEAD-02 (this task follows LEAD-02-B) |
| Kong deployed in `nexus-infra` namespace | Platform Team | P0-INFRA-05 |
| Kong JWT plugin globally enabled | Platform Team | P0-INFRA-05 |
| `nexus_system.tenants` table with `jwt_issuer` column | Tech Lead | LEAD-00-A |
| `nexus_system.identity_mapping` table created | Tech Lead | LEAD-02-G |

---

## Part 1 — Okta Developer Org Setup

### Step 1-A — Register an Okta Developer Account

```
1. Go to: https://developer.okta.com
2. Click "Sign up free"
3. Fill in:
   - First name / Last name: Ben (Tech Lead)
   - Email: nexus-dev@mentis-consulting.be   ← use a team email, not personal
   - Company: Mentis Consulting
4. Verify email → you receive your Okta org URL:
   https://dev-XXXXXXX.okta.com
   ↑ This is your OIDC_ISSUER_URL for Iteration 1
5. Save the org URL in the team password manager (1Password / Bitwarden)
```

### Step 1-B — Create a Custom Authorization Server

Okta's default authorization server does not support custom claims in the way NEXUS needs. Create a dedicated one.

```
In Okta Admin console:
Security → API → Authorization Servers → Add Authorization Server

Name:       nexus-dev
Audience:   api://nexus-dev
Description: NEXUS development and demo authorization server
```

Note the resulting **Issuer URI** — it will look like:
```
https://dev-XXXXXXX.okta.com/oauth2/nexus-dev-auth-server-id
```

This is the value of `OIDC_ISSUER_URL`. It differs from the org URL because it points to the specific authorization server, not the org root.

**Verify the JWKS endpoint is reachable:**
```bash
curl -s https://dev-XXXXXXX.okta.com/oauth2/<auth-server-id>/.well-known/openid-configuration \
  | jq '{issuer, jwks_uri, authorization_endpoint, token_endpoint}'
# Expected: all four fields populated
```

### Step 1-C — Add the tenant_id Custom Claim

This is the most important Okta configuration step. Without it, JWTs contain no `tenant_id`, Kong cannot inject `X-Tenant-ID`, and the entire multi-tenant isolation model breaks.

```
In the nexus-dev Authorization Server:
Claims tab → Add Claim

Claim 1:
  Name:              tenant_id
  Include in token:  Access Token
  Value type:        Expression
  Value:             user.nexus_tenant_id
  Include in:        Any scope

Claim 2:
  Name:              email
  Include in token:  Access Token
  Value type:        Expression
  Value:             user.email
  Include in:        Any scope
```

`user.nexus_tenant_id` is a custom Okta profile attribute defined in Step 1-D. It maps each Okta user to their NEXUS tenant.

### Step 1-D — Add nexus_tenant_id to Okta User Profile

```
Directory → Profile Editor → User (default)
→ Add Attribute

Data Type:    string
Display name: NEXUS Tenant ID
Variable:     nexus_tenant_id
Description:  Maps this user to their NEXUS tenant (e.g. 'acme-corp')
Attribute:    Required? No (system accounts won't have it)
```

### Step 1-E — Register NEXUS as an OIDC Application

```
Applications → Create App Integration
Sign-in method: OIDC — OpenID Connect
Application type: Web Application (for M6 Next.js server-side auth)

App name:         NEXUS Dev
Sign-in redirect URIs:
  http://localhost:3000/api/auth/callback/okta   ← local M6 dev
  https://nexus.internal/api/auth/callback/okta  ← deployed M6

Sign-out redirect URIs:
  http://localhost:3000
  https://nexus.internal

Assignments: Allow everyone in your organisation (for dev simplicity)
```

After creation, note:
- **Client ID** — used by M6 `next-auth`
- **Client Secret** — used by M6 `next-auth` and stored in Kubernetes Secret

```bash
# Add to Kubernetes Secret (used by M6)
kubectl create secret generic nexus-okta-credentials \
  --namespace nexus-app \
  --from-literal=client_id=<YOUR_CLIENT_ID> \
  --from-literal=client_secret=<YOUR_CLIENT_SECRET> \
  --from-literal=issuer_url=https://dev-XXXXXXX.okta.com/oauth2/<auth-server-id>
```

---

## Part 2 — Test Users and Groups

### Step 2-A — Create Test Groups

```
Directory → Groups → Add Group

Group 1: nexus-test-alpha
  Description: Test users for tenant test-alpha

Group 2: nexus-test-beta
  Description: Test users for tenant test-beta

Group 3: nexus-data-stewards
  Description: Users who can approve CDM proposals and mapping exceptions
```

### Step 2-B — Create Test Users

Create one user per tenant, plus a multi-tenant admin user for integration testing.

```
Directory → People → Add Person

User 1 (test-alpha tenant):
  First name:        Alice
  Last name:         Alpha
  Username:          alice@nexus-dev.mentis-consulting.be
  Password:          Set by admin (share in team password manager)
  nexus_tenant_id:   test-alpha
  Group:             nexus-test-alpha, nexus-data-stewards

User 2 (test-beta tenant):
  First name:        Bob
  Last name:         Beta
  Username:          bob@nexus-dev.mentis-consulting.be
  Password:          Set by admin
  nexus_tenant_id:   test-beta
  Group:             nexus-test-beta

User 3 (cross-tenant admin, for system-level tests only):
  First name:        Admin
  Last name:         Nexus
  Username:          admin@nexus-dev.mentis-consulting.be
  nexus_tenant_id:   __system__
  Group:             (none)
```

### Step 2-C — Provision Tenants for Test Users

After creating Okta users, run `onboard-tenant` so the `nexus_system.tenants` table recognises them:

```bash
onboard-tenant --tenant test-alpha --plan professional
onboard-tenant --tenant test-beta  --plan professional
```

And update the `jwt_issuer` column in the tenants table so NEXUS knows which IdP each tenant uses:

```sql
UPDATE nexus_system.tenants
SET jwt_issuer = 'https://dev-XXXXXXX.okta.com/oauth2/<auth-server-id>'
WHERE tenant_id IN ('test-alpha', 'test-beta');
```

---

## Part 3 — nexus_core OIDC Module

### Step 3-A — nexus_core/oidc.py

This module is the single source of truth for OIDC configuration in `nexus_core`. Every service that needs to know the issuer URL reads it from here — not from environment variables directly.

```python
# nexus_core/oidc.py

import os
import httpx
import logging
from functools import lru_cache
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class OIDCConfig:
    """
    OIDC configuration resolved from the issuer's discovery endpoint.
    Immutable and cached at startup — does not change at runtime.
    """
    issuer:                str
    jwks_uri:              str
    authorization_endpoint: str
    token_endpoint:         str
    userinfo_endpoint:      str

class OIDCConfigError(Exception):
    """Raised when OIDC configuration cannot be resolved."""

@lru_cache(maxsize=1)
def get_oidc_config() -> OIDCConfig:
    """
    Fetches and caches OIDC configuration from the issuer's discovery endpoint.
    Called once at service startup.

    The discovery endpoint is the OIDC standard:
    {OIDC_ISSUER_URL}/.well-known/openid-configuration

    This works for ANY OIDC-compliant IdP:
    - Okta:     https://dev-xxxxx.okta.com/oauth2/<server-id>/.well-known/openid-configuration
    - Azure AD: https://login.microsoftonline.com/{tenant}/v2.0/.well-known/openid-configuration
    - Google:   https://accounts.google.com/.well-known/openid-configuration
    """
    issuer_url = os.getenv("OIDC_ISSUER_URL")
    if not issuer_url:
        raise OIDCConfigError(
            "OIDC_ISSUER_URL environment variable is not set. "
            "Set it to your IdP's issuer URL before starting any NEXUS service. "
            "Example: OIDC_ISSUER_URL=https://dev-xxxxx.okta.com/oauth2/<server-id>"
        )

    discovery_url = f"{issuer_url.rstrip('/')}/.well-known/openid-configuration"
    logger.info(f"Fetching OIDC config from: {discovery_url}")

    try:
        response = httpx.get(discovery_url, timeout=10)
        response.raise_for_status()
        data = response.json()
    except httpx.HTTPError as e:
        raise OIDCConfigError(
            f"Failed to fetch OIDC discovery document from {discovery_url}: {e}"
        )

    required_fields = ["issuer", "jwks_uri", "authorization_endpoint",
                       "token_endpoint", "userinfo_endpoint"]
    missing = [f for f in required_fields if f not in data]
    if missing:
        raise OIDCConfigError(
            f"OIDC discovery document at {discovery_url} is missing required fields: {missing}"
        )

    config = OIDCConfig(
        issuer=data["issuer"],
        jwks_uri=data["jwks_uri"],
        authorization_endpoint=data["authorization_endpoint"],
        token_endpoint=data["token_endpoint"],
        userinfo_endpoint=data["userinfo_endpoint"],
    )
    logger.info(f"OIDC config loaded. Issuer: {config.issuer}, JWKS: {config.jwks_uri}")
    return config


def get_jwks_uri() -> str:
    """Shortcut — returns the JWKS URI for this IdP. Used by Kong configuration scripts."""
    return get_oidc_config().jwks_uri


def get_issuer() -> str:
    """Shortcut — returns the validated issuer string. Used for JWT claim validation."""
    return get_oidc_config().issuer
```

### Step 3-B — Environment Variable Documentation

Add to all service Kubernetes manifests and the team `.env.example`:

```bash
# .env.example (committed to repo — safe values only)

# ── Identity Provider ─────────────────────────────────────────────────────────
# The issuer URL of your OIDC-compliant IdP.
# This is the ONLY configuration change needed to swap IdP.
# Examples:
#   Okta developer org:  https://dev-XXXXXXX.okta.com/oauth2/<auth-server-id>
#   Okta production:     https://acme.okta.com/oauth2/<auth-server-id>
#   Azure AD:            https://login.microsoftonline.com/<tenant-id>/v2.0
#   Google Workspace:    https://accounts.google.com
OIDC_ISSUER_URL=https://dev-XXXXXXX.okta.com/oauth2/<auth-server-id>
```

Add to `nexus_core/oidc.py` validation at startup:

```python
def validate_env():
    """
    Called at startup of any service that depends on OIDC.
    Fails fast with a clear error rather than a 401 at first request.
    """
    try:
        config = get_oidc_config()
        logger.info(f"OIDC validated: issuer={config.issuer}")
    except OIDCConfigError as e:
        logger.critical(f"OIDC startup validation failed: {e}")
        raise SystemExit(1)
```

---

## Part 4 — Kong JWT Plugin Configuration

Kong validates JWTs and injects identity headers. This is the enforcement point — if Kong doesn't validate, nothing does.

### Step 4-A — Fetch JWKS URI from Okta Discovery Endpoint

```bash
# Run this once after Okta is configured. Output feeds Kong config.

JWKS_URI=$(python3 -c "
from nexus_core.oidc import get_jwks_uri
print(get_jwks_uri())
")
echo "JWKS URI: $JWKS_URI"
# Expected: https://dev-XXXXXXX.okta.com/oauth2/<server-id>/v1/keys
```

### Step 4-B — Configure Kong JWT Plugin with Okta JWKS

Update Kong's declarative config (`kong.yaml`) to point the JWT plugin at Okta's JWKS endpoint:

```yaml
# kong.yaml (deck sync)

_format_version: "3.0"

plugins:
  - name: jwt
    config:
      key_claim_name: kid            # Kong uses 'kid' from JWT header to select key from JWKS
      claims_to_verify:
        - exp                        # Validate token expiry
      secret_is_base64: false

  # Header injection plugin:
  # Extracts claims from the validated JWT payload and injects as HTTP headers.
  # Application services read these headers — they never touch the JWT.
  - name: request-transformer
    config:
      add:
        headers:
          - "X-User-ID:$(jwt_claims.sub)"
          - "X-User-Email:$(jwt_claims.email)"
          - "X-Tenant-ID:$(jwt_claims.tenant_id)"

  - name: rate-limiting
    config:
      minute: 100
      policy: header
      header_name: X-Tenant-ID
      limit_by: header

  - name: correlation-id
    config:
      header_name: X-Correlation-ID
      generator: uuid#counter
      echo_downstream: true

# Register the Okta public key with Kong
# Kong uses this to verify JWT signatures
consumers:
  - username: okta-nexus-dev
    jwt_secrets:
      - key: <KEY_ID_FROM_OKTA_JWKS>    # The 'kid' value from Okta's JWKS endpoint
        algorithm: RS256
        rsa_public_key: |               # Public key from JWKS — fetched below
          -----BEGIN PUBLIC KEY-----
          <BASE64_ENCODED_PUBLIC_KEY>
          -----END PUBLIC KEY-----
```

**Script to fetch and format Okta public key for Kong:**

```python
# scripts/fetch_okta_public_key.py
"""
Fetches the current RSA public key from Okta's JWKS endpoint
and formats it for Kong's JWT plugin configuration.

Usage:
  python scripts/fetch_okta_public_key.py
  # Outputs: kid, algorithm, and PEM-formatted public key ready for kong.yaml
"""

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers
from cryptography.hazmat.backends import default_backend
import base64, json

JWKS_URI = "https://dev-XXXXXXX.okta.com/oauth2/<server-id>/v1/keys"

def base64url_decode(data: str) -> bytes:
    padding = 4 - len(data) % 4
    return base64.urlsafe_b64decode(data + "=" * padding)

response = httpx.get(JWKS_URI)
keys = response.json()["keys"]

# Use the first active key (Okta may rotate — re-run this script on rotation)
key = keys[0]
n = int.from_bytes(base64url_decode(key["n"]), "big")
e = int.from_bytes(base64url_decode(key["e"]), "big")

public_key = RSAPublicNumbers(e, n).public_key(default_backend())
pem = public_key.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo,
).decode()

print(f"kid: {key['kid']}")
print(f"algorithm: {key['alg']}")
print(f"rsa_public_key:\n{pem}")
```

```bash
python scripts/fetch_okta_public_key.py
# Copy output into kong.yaml consumers section
deck sync --state kong.yaml
```

### Step 4-C — Verify Header Injection End-to-End

```bash
# Get a real JWT from Okta for user alice (test-alpha tenant)
# Using Okta CLI:
okta login
ACCESS_TOKEN=$(okta api /api/v1/authn \
  --method POST \
  --body '{"username":"alice@nexus-dev.mentis-consulting.be","password":"<pwd>"}' \
  | jq -r '.sessionToken')
# Exchange session token for access token via Okta OAuth flow
# (see Step 5 — JWT generation script for the full flow)

# Send request through Kong
curl -v https://api.nexus.internal/health \
  -H "Authorization: Bearer $ACCESS_TOKEN" 2>&1 | grep -E "X-User|X-Tenant"
# Expected:
# X-User-ID: 00uXXXXXXXXXXXX   (Okta's internal user ID — the 'sub' claim)
# X-User-Email: alice@nexus-dev.mentis-consulting.be
# X-Tenant-ID: test-alpha
```

---

## Part 5 — Test JWT Generation Script

Every team member needs to be able to generate a valid JWT for their test user. This script automates the OAuth 2.0 Authorization Code + PKCE flow from the command line.

```python
# scripts/get_test_jwt.py
"""
Generates an access token for a NEXUS test user via Okta PKCE flow.
Saves the token to a local file for use in curl/acceptance tests.

Usage:
  python scripts/get_test_jwt.py --user alice    # test-alpha tenant
  python scripts/get_test_jwt.py --user bob      # test-beta tenant

Output:
  Prints the access token and writes it to .nexus-test-token-{user}
  Use as: export TEST_JWT=$(cat .nexus-test-token-alice)
"""

import argparse, base64, hashlib, httpx, json, os, secrets, webbrowser
from urllib.parse import urlencode, urlparse, parse_qs
from http.server import HTTPServer, BaseHTTPRequestHandler

ISSUER_URL    = os.environ["OIDC_ISSUER_URL"]
CLIENT_ID     = os.environ["OKTA_CLIENT_ID"]    # No client secret needed for PKCE
REDIRECT_URI  = "http://localhost:8765/callback"
SCOPES        = "openid email profile"

USERS = {
    "alice": "alice@nexus-dev.mentis-consulting.be",
    "bob":   "bob@nexus-dev.mentis-consulting.be",
}

def pkce_pair():
    verifier  = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()
    ).rstrip(b"=").decode()
    return verifier, challenge

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", choices=list(USERS.keys()), required=True)
    args = parser.parse_args()

    config   = httpx.get(f"{ISSUER_URL}/.well-known/openid-configuration").json()
    verifier, challenge = pkce_pair()
    state    = secrets.token_urlsafe(16)

    auth_url = config["authorization_endpoint"] + "?" + urlencode({
        "client_id":             CLIENT_ID,
        "response_type":         "code",
        "scope":                 SCOPES,
        "redirect_uri":          REDIRECT_URI,
        "state":                 state,
        "code_challenge":        challenge,
        "code_challenge_method": "S256",
        "login_hint":            USERS[args.user],
    })

    # Capture callback on a local server
    code_holder = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            params = parse_qs(urlparse(self.path).query)
            code_holder["code"]  = params["code"][0]
            code_holder["state"] = params["state"][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"<h1>Auth complete. Return to terminal.</h1>")
        def log_message(self, *args): pass

    server = HTTPServer(("localhost", 8765), Handler)
    print(f"Opening browser for {args.user}...")
    webbrowser.open(auth_url)
    server.handle_request()

    assert code_holder.get("state") == state, "State mismatch — CSRF protection triggered"

    token_resp = httpx.post(config["token_endpoint"], data={
        "grant_type":    "authorization_code",
        "client_id":     CLIENT_ID,
        "redirect_uri":  REDIRECT_URI,
        "code":          code_holder["code"],
        "code_verifier": verifier,
    })
    token_resp.raise_for_status()
    token_data = token_resp.json()
    access_token = token_data["access_token"]

    # Save token
    token_file = f".nexus-test-token-{args.user}"
    with open(token_file, "w") as f:
        f.write(access_token)

    # Decode payload (no verification — just for display)
    payload = json.loads(
        base64.urlsafe_b64decode(access_token.split(".")[1] + "==")
    )

    print(f"\n✅ Token obtained for {args.user}")
    print(f"   sub:       {payload.get('sub')}")
    print(f"   email:     {payload.get('email')}")
    print(f"   tenant_id: {payload.get('tenant_id')}")
    print(f"   exp:       {payload.get('exp')}")
    print(f"\nSaved to: {token_file}")
    print(f"Usage:    export TEST_JWT=$(cat {token_file})")

if __name__ == "__main__":
    main()
```

Add to `.gitignore`:
```
.nexus-test-token-*
```

---

## Part 6 — Update nexus_core Package

### Step 6-A — Add oidc.py to package structure

```
nexus_core/
├── __init__.py
├── messaging.py
├── tenant.py
├── tenant_validator.py
├── provisioning.py
├── identity.py
├── oidc.py             ← NEW this task
├── topics.py
├── entities.py
├── schemas.py
├── errors.py
├── logging.py
├── db.py
└── cdm_registry.py
```

### Step 6-B — Startup Validation Pattern for All Services

Every service that runs behind Kong must validate OIDC config at startup. Add this to FastAPI lifespan events:

```python
# Pattern for every M2, M4, M6 FastAPI service

from contextlib import asynccontextmanager
from fastapi import FastAPI
from nexus_core.oidc import validate_env

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    validate_env()   # Fails fast if OIDC_ISSUER_URL is wrong or unreachable
    yield
    # Shutdown (nothing needed)

app = FastAPI(lifespan=lifespan)
```

### Step 6-C — Add to Kubernetes Secrets and all Service Manifests

```yaml
# k8s/nexus-oidc-secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: nexus-oidc-config
  namespace: nexus-app
type: Opaque
stringData:
  OIDC_ISSUER_URL:    "https://dev-XXXXXXX.okta.com/oauth2/<auth-server-id>"
  OKTA_CLIENT_ID:     "<client-id>"
  OKTA_CLIENT_SECRET: "<client-secret>"   # Used by M6 next-auth only
```

Reference in every service Deployment:

```yaml
# In every Deployment spec → containers → env
env:
  - name: OIDC_ISSUER_URL
    valueFrom:
      secretKeyRef:
        name: nexus-oidc-config
        key: OIDC_ISSUER_URL
```

---

## Part 7 — Onboarding Client Tenants (Reference Pattern)

This section documents the procedure for onboarding a real client tenant onto their own IdP. It is not executed in Iteration 1 but must be written and committed to the runbook.

```bash
# When a new enterprise client signs up:

# 1. Client provides their IdP issuer URL (OIDC discovery endpoint)
CLIENT_ISSUER_URL="https://acme.okta.com/oauth2/default"

# 2. Verify their OIDC discovery document is accessible and contains tenant_id claim
curl -s "$CLIENT_ISSUER_URL/.well-known/openid-configuration" | jq '{issuer, jwks_uri}'

# 3. Provision the tenant in NEXUS
onboard-tenant --tenant acme-corp --plan enterprise

# 4. Update the tenants table with their IdP
psql $POSTGRES_DSN -c "
    UPDATE nexus_system.tenants
    SET jwt_issuer = '$CLIENT_ISSUER_URL'
    WHERE tenant_id = 'acme-corp';
"

# 5. Fetch and register their public key with Kong
OIDC_ISSUER_URL=$CLIENT_ISSUER_URL python scripts/fetch_okta_public_key.py
# Update kong.yaml consumers section with new key, deck sync

# 6. Update OIDC_ISSUER_URL in their deployment namespace if using dedicated cluster
# (multi-tenant SaaS: all tenants share one Kong with multiple JWT consumers)
# (dedicated deployment: swap OIDC_ISSUER_URL env var in that deployment)
```

---

## Acceptance Test Sequence

Run in order. All of Part 1–6 must be complete before running these tests.

```bash
# ── 1. Verify Okta discovery endpoint is reachable ────────────────────────────

curl -s "$OIDC_ISSUER_URL/.well-known/openid-configuration" \
  | jq '{issuer, jwks_uri, token_endpoint}'
# Expected: all three fields populated with Okta URLs

# ── 2. Verify nexus_core OIDC config loads without error ─────────────────────

OIDC_ISSUER_URL=https://dev-XXXXXXX.okta.com/oauth2/<server-id> \
  python3 -c "
from nexus_core.oidc import get_oidc_config
config = get_oidc_config()
print(f'issuer:    {config.issuer}')
print(f'jwks_uri:  {config.jwks_uri}')
print('nexus_core OIDC: OK')
"

# ── 3. Generate test JWTs for all test users ──────────────────────────────────

python scripts/get_test_jwt.py --user alice
python scripts/get_test_jwt.py --user bob

export TEST_JWT=$(cat .nexus-test-token-alice)
export TEST_BETA_JWT=$(cat .nexus-test-token-bob)

# ── 4. Decode and verify JWT claims ──────────────────────────────────────────

python3 -c "
import base64, json, sys
token = open('.nexus-test-token-alice').read().strip()
payload = json.loads(base64.urlsafe_b64decode(token.split('.')[1] + '=='))
print(json.dumps(payload, indent=2))

assert 'sub' in payload,       'FAIL: missing sub claim'
assert 'email' in payload,     'FAIL: missing email claim'
assert 'tenant_id' in payload, 'FAIL: missing tenant_id custom claim'
assert payload['tenant_id'] == 'test-alpha', f'FAIL: wrong tenant_id: {payload[\"tenant_id\"]}'
print('JWT claims: OK')
"

# ── 5. Verify Kong validates JWT and injects headers ─────────────────────────

curl -sv https://api.nexus.internal/health \
  -H "Authorization: Bearer $TEST_JWT" 2>&1 \
  | grep -E "< X-User|< X-Tenant|HTTP/"
# Expected:
# HTTP/2 200
# < X-User-ID: 00uXXXXXXXXXXXX
# < X-User-Email: alice@nexus-dev.mentis-consulting.be
# < X-Tenant-ID: test-alpha

# ── 6. Verify Kong rejects request without JWT ───────────────────────────────

curl -sv https://api.nexus.internal/api/v1/governance/proposals 2>&1 | grep "HTTP/"
# Expected: HTTP/2 401

# ── 7. Verify Kong rejects expired/invalid JWT ───────────────────────────────

curl -sv https://api.nexus.internal/api/v1/governance/proposals \
  -H "Authorization: Bearer eyJhbGciOiJSUzI1NiJ9.INVALID.SIGNATURE" 2>&1 | grep "HTTP/"
# Expected: HTTP/2 401

# ── 8. Verify X-Tenant-ID in M4 API ─────────────────────────────────────────

curl -s https://api.nexus.internal/api/v1/governance/proposals \
  -H "Authorization: Bearer $TEST_JWT" | jq .
# Expected: HTTP 200, proposals array (empty is fine — tenant test-alpha exists)

# ── 9. Verify tenant isolation via X-Tenant-ID ───────────────────────────────

# Create a proposal for test-alpha
# Then query with test-beta JWT — must return empty
curl -s https://api.nexus.internal/api/v1/governance/proposals \
  -H "Authorization: Bearer $TEST_BETA_JWT" | jq '.total'
# Expected: 0

# ── 10. Verify nexus_core.identity extracts headers correctly ────────────────

python3 -c "
from nexus_core.identity import get_user_identity, IdentityMissingError

# Happy path
identity = get_user_identity({
    'X-User-ID':    '00uXXXXXXXXXXXX',
    'X-User-Email': 'alice@nexus-dev.mentis-consulting.be',
    'X-Tenant-ID':  'test-alpha',
})
assert identity.user_id == '00uXXXXXXXXXXXX'
assert identity.tenant_id == 'test-alpha'
print('identity extraction: OK')

# Missing header
try:
    get_user_identity({'X-User-ID': '123'})   # Missing X-Tenant-ID and X-User-Email
    print('FAIL: should have raised IdentityMissingError')
except IdentityMissingError:
    print('missing header rejection: OK')
"

# ── 11. Verify OIDC startup validation fails fast on bad issuer ───────────────

OIDC_ISSUER_URL=https://this-does-not-exist.example.com \
  python3 -c "
from nexus_core.oidc import validate_env
validate_env()
" 2>&1
# Expected: logs CRITICAL error, exits with SystemExit(1)

# ── 12. Verify swap to different issuer needs only env var change ─────────────

# Point to a different (but real) OIDC provider — no code change
OIDC_ISSUER_URL=https://accounts.google.com \
  python3 -c "
from nexus_core.oidc import get_oidc_config
config = get_oidc_config()
print(f'Issuer: {config.issuer}')
print('IdP swap: OK — same code, different issuer')
"
# Expected: loads Google's OIDC config successfully
```

---

## Acceptance Criteria

| # | Test | Expected Result |
|---|---|---|
| 1 | Okta discovery endpoint reachable | Returns `issuer`, `jwks_uri`, `token_endpoint` |
| 2 | `nexus_core.oidc.get_oidc_config()` | Returns `OIDCConfig` without error |
| 3 | JWT for alice contains `tenant_id: test-alpha` | Custom claim present in token payload |
| 4 | JWT for bob contains `tenant_id: test-beta` | Custom claim present in token payload |
| 5 | Kong validates JWT and injects `X-User-ID` | Header present in upstream request |
| 6 | Kong validates JWT and injects `X-Tenant-ID` | Value matches `tenant_id` claim in JWT |
| 7 | Kong validates JWT and injects `X-User-Email` | Value matches `email` claim in JWT |
| 8 | Request without JWT | Kong returns HTTP 401 |
| 9 | Request with invalid JWT | Kong returns HTTP 401 |
| 10 | `get_user_identity()` with all headers | Returns `NexusUserIdentity` correctly populated |
| 11 | `get_user_identity()` with missing header | Raises `IdentityMissingError` |
| 12 | `validate_env()` with wrong issuer URL | Exits process with `SystemExit(1)` |
| 13 | `OIDC_ISSUER_URL` swap to Google OIDC | `get_oidc_config()` loads without code changes |
| 14 | `test-alpha` JWT queries M4 governance API | HTTP 200, empty proposals |
| 15 | `test-beta` JWT cannot see `test-alpha` proposals | HTTP 200, total=0 |

---

## What Downstream Tasks Depend On

Once this task passes all acceptance criteria:

| Task | What it needs from this task |
|---|---|
| **P7-M6-01 (Okta login flow)** | `OKTA_CLIENT_ID`, `OKTA_CLIENT_SECRET`, `OIDC_ISSUER_URL` in Kubernetes Secret; `next-auth` Okta provider configured |
| **All M4 acceptance tests** | `$TEST_JWT` and `$TEST_BETA_JWT` from `get_test_jwt.py` |
| **All M2 acceptance tests** | Same test JWTs for HTTP API calls |
| **Multi-tenant isolation tests** | Two valid JWTs for different tenants |
| **Client onboarding (future)** | `fetch_okta_public_key.py` script and runbook |

---

## Key Design Decisions

**Why a custom Authorization Server, not Okta's default?** Okta's default authorization server (`/oauth2/default`) supports custom claims but is less flexible in production scenarios. A dedicated authorization server (`nexus-dev`) gives full control over claim rules, scopes, and policies without affecting other applications that may share the default server.

**Why is `tenant_id` a custom claim and not derived from the Okta group?** Groups are an authorisation concept — a user can be in multiple groups. `tenant_id` is a single-valued identity attribute. Using a custom profile attribute (`nexus_tenant_id`) makes the mapping explicit, unambiguous, and directly readable from the JWT without any lookup.

**Why does Kong register the public key statically, not via dynamic JWKS fetch?** Kong's JWT plugin in its standard form validates against pre-registered keys, not via live JWKS lookup. This is a deliberate security choice — it prevents a compromised Okta JWKS endpoint from being used to accept forged tokens. The tradeoff is that key rotation requires re-running `fetch_okta_public_key.py` and `deck sync`. Kong Enterprise supports JWKS-based dynamic validation; that is a future upgrade path.

**Why does `get_oidc_config()` use `@lru_cache` instead of a module-level variable?** `lru_cache` makes it testable — tests can clear the cache with `get_oidc_config.cache_clear()` and substitute a different `OIDC_ISSUER_URL`. A module-level variable set at import time is hard to override in tests.

---

*NEXUS Okta OIDC Config Task Plan · Mentis Consulting · February 2026 · Confidential*
