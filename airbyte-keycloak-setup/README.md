# airbyte-keycloak-setup

Micronaut Kotlin application that configures an Airbyte Keycloak realm
(clients, identity providers, default roles, users). It normally runs
once at bootstrap via the `keycloak-setup` Helm Job and exits. Its main
entry point is `io.airbyte.keycloak.setup.ApplicationKt`.

This module also carries Keycloak maintenance scripts under `scripts/`.

## sonar-webapp client backfill

`scripts/backfill_sonar_webapp_client.py` is a manually invoked operator
script that idempotently creates the `sonar-webapp` Keycloak client in
existing SSO realms that were provisioned before the SSO flow started
adding it.

The script:

- enumerates all realms directly from Keycloak using the admin API
  unless specific realms are supplied with `--realm`;
- supports targeted runs with repeated `--realm <realm-name>` flags;
- skips any realm that does not already have the `airbyte-webapp`
  client, so non-SSO realms do not receive an orphan client;
- skips any realm that already has the `sonar-webapp` client and logs
  drift without overwriting the existing client;
- defaults to dry-run mode;
- reports per-realm failures and continues processing the remaining
  realms.

The script requires a Keycloak admin bearer token. It does not use the
config database and does not run the normal realm setup application.

### Port-forward Keycloak

The chart exposes Keycloak as `<release>-airbyte-keycloak-svc` on port
`8180` (see `oss/charts/v2/airbyte/templates/airbyte-keycloak/service.yaml`
and the `keycloak.service.port` default). The existing Terraform docs use
`localhost:8081`, so the script defaults to `http://localhost:8081/auth`:

```bash
kubectl port-forward -n <keycloak-namespace> svc/<release>-airbyte-keycloak-svc 8081:8180
```

If your local forwarding uses a different port, pass
`--keycloak-base-url` or set `KEYCLOAK_BASE_URL`.

### Dry run

```bash
export KEYCLOAK_BEARER_TOKEN='<token>'
export AIRBYTE_URL='https://cloud.airbyte.com'
export AIRBYTE_AGENTS_URL='https://app.airbyte.ai'

uv run --script oss/airbyte-keycloak-setup/scripts/backfill_sonar_webapp_client.py
```

Equivalent explicit form:

```bash
uv run --script oss/airbyte-keycloak-setup/scripts/backfill_sonar_webapp_client.py \
  --bearer-token "$KEYCLOAK_BEARER_TOKEN" \
  --keycloak-base-url 'http://localhost:8081/auth' \
  --airbyte-url 'https://cloud.airbyte.com' \
  --sonar-webapp-url 'https://app.airbyte.ai' \
  --dry-run
```

To dry-run only a controlled spot-check set:

```bash
uv run --script oss/airbyte-keycloak-setup/scripts/backfill_sonar_webapp_client.py \
  --realm <realm-a> \
  --realm <realm-b>
```

### Apply

Run the dry run first and inspect the summary. To create missing clients:

```bash
uv run --script oss/airbyte-keycloak-setup/scripts/backfill_sonar_webapp_client.py --apply
```

To spot-check a controlled set before running every realm, pass
`--realm` one or more times. In targeted mode the script does not
enumerate all realms:

```bash
uv run --script oss/airbyte-keycloak-setup/scripts/backfill_sonar_webapp_client.py \
  --realm <realm-a> \
  --realm <realm-b> \
  --apply
```

You can also set `KEYCLOAK_BACKFILL_SONAR_WEBAPP_CLIENT_DRY_RUN=false`,
but `--apply` is preferred for one-off operator use because the mutation
is visible in the command.

### Configuration

The bearer token can be supplied as either `--bearer-token` or
`KEYCLOAK_BEARER_TOKEN`.

The Keycloak URL defaults to `http://localhost:8081/auth`. Override it
with `--keycloak-base-url` or `KEYCLOAK_BASE_URL`. If
`KEYCLOAK_BASE_URL` is unset, the script also honors the existing
Keycloak setup environment variables:

- `KEYCLOAK_PROTOCOL`
- `KEYCLOAK_INTERNAL_HOST`
- `KEYCLOAK_BASE_PATH`

The `sonar-webapp` client representation defaults to:

- `clientId`: `sonar-webapp`
- `name`: `Sonar Webapp`
- `baseUrl`: `AIRBYTE_AGENTS_URL`, falling back to `AIRBYTE_URL`
- `redirectUris`: `AIRBYTE_AGENTS_VALID_REDIRECT_URIS`, falling back to
  `<baseUrl>/*`
- `webOrigins`: `AIRBYTE_AGENTS_WEB_ORIGINS`, falling back to
  `<baseUrl>`

Use repeated `--redirect-uri` and `--web-origin` flags to provide
multiple values from the command line.

Use repeated `--realm` flags to restrict the run to specific Keycloak
realms. If no `--realm` is provided, the script enumerates and processes
all realms returned by Keycloak.
