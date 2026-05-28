# AGENTS.md — `oss/airbyte-keycloak-setup/`

Conventions for the Keycloak setup application. Read the root
[AGENTS.md](../../AGENTS.md) and [`oss/AGENTS.md`](../AGENTS.md) first.

## What this module is (and isn't)

This is a **Micronaut Kotlin application** that talks to a Keycloak
admin API to *configure* a realm — clients, identity providers,
default roles, users. It runs once at bootstrap and exits.

It is **not** a Keycloak SPI / extension JAR. Custom Keycloak Java
extensions would live elsewhere (none exist today).

The sibling module `oss/airbyte-keycloak/` is the **Keycloak Docker
image** itself — themes, password blacklists, entrypoint scripts. Do
not confuse the two:

- Need to add a client, realm role, or IDP → edit this module
  (`airbyte-keycloak-setup`).
- Need to change the Keycloak version, theme, or container scripts →
  edit `oss/airbyte-keycloak/`.

## Configurator pattern

The setup app is organized around `Configurator` classes, each
responsible for a slice of realm configuration:

- `WebClientConfigurator` — registers OIDC clients (webapp client,
  service clients, etc.) and their redirect URIs, scopes, and
  default client scopes.
- `IdentityProvidersConfigurator` — registers external IdPs
  (Google, OIDC, SAML brokers).
- `UserConfigurator` — seeds default users (typically used only for
  local dev).
- `ConfigurationMapService` — centralized lookup of configuration
  values passed in via `application.yml` / env vars.

Top-level orchestration lives in `KeycloakSetup.kt`. `KeycloakServer.kt`
manages server-lifecycle concerns and `KeycloakAdminClientProvider.kt`
exposes the admin client as a Micronaut bean.

## Adding things

- **New OIDC client** → add a method on `WebClientConfigurator` and
  invoke it from the configurator's main entry. Wire any required
  config keys through `ConfigurationMapService`. Update the realm
  exports / Helm values that pin client IDs and redirect URIs.
- **New identity provider** → add a method on
  `IdentityProvidersConfigurator`. External IdPs need credentials —
  source them from env vars, not hardcoded. Test against a local
  Keycloak first (see Local dev below).
- **New default role** → add a role mapping in the appropriate
  configurator (typically `WebClientConfigurator` for client-scoped
  roles, or a realm-level configurator for realm roles). Audit who
  receives the role by default — over-granting is a security finding.

## Build & test

- **Check the module**: `./gradlew :oss:airbyte-keycloak-setup:check`
- **Run the setup app locally**: see `infra/gcp/keycloak/local/`
  (Terraform + scripts for spinning up a local Keycloak that this
  setup app then configures).
- **Tests** live in `src/test/kotlin/io/airbyte/keycloak/setup/` —
  follow the existing patterns (JUnit 5 + MockK), mocking the
  Keycloak admin client.

## Conventions

- **Idempotency is mandatory.** The setup app may run multiple times
  against the same realm (bootloader retries, redeploys, etc.).
  Every configurator action must be a no-op if the target object
  already exists with matching configuration, and an update if it
  exists with drifted configuration.
- **No secrets in code.** Client secrets, IdP credentials, admin
  passwords come from env vars surfaced through
  `ConfigurationMapService`. Never commit them.
- **Don't reach across configurators.** If `WebClientConfigurator`
  needs something that `IdentityProvidersConfigurator` produces,
  thread it through `KeycloakSetup` rather than coupling the
  configurators directly.

## Cloud / OSS distinction

This module ships with both OSS and Cloud deployments — it's not
Cloud-specific. Cloud-only client registrations (e.g., for the
billing portal) usually still belong here, gated by a config flag
read from `ConfigurationMapService`. Ask before adding Cloud-only
logic outside the existing config-gated pattern.

## Common pitfalls

- "Realm doesn't exist" → the bootloader sequence didn't run, or
  the Keycloak server isn't healthy yet. Check
  `KeycloakAdminClientProvider` for the readiness check.
- Configurator runs but the change doesn't appear → confirm you're
  configuring the right realm (check `ConfigurationMapService`
  realm-name resolution). The setup app supports multiple realms in
  some deployments.
- Idempotency regression → almost always a configurator calling a
  "create" admin API instead of "create-or-update". Use
  existence-check-then-update.
