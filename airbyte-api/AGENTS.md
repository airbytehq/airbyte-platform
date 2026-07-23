# AGENTS.md — `oss/airbyte-api/`

OpenAPI is the source of truth for HTTP contracts. This directory holds
the YAML specs and the Gradle plumbing that generates JAX-RS server
interfaces and Kotlin client classes from them. Read the root
[AGENTS.md](../../AGENTS.md) and [`oss/AGENTS.md`](../AGENTS.md) first.

## Submodules

Canonical list is in repo-root `settings.gradle.kts`. As of writing:

- `:oss:airbyte-api:commons` — shared OpenAPI types
- `:oss:airbyte-api:server-api` — internal server API (the big one)
- `:oss:airbyte-api:public-api` — externally documented Airbyte API
- `:oss:airbyte-api:problems-api` — RFC 7807 problem details
- `:oss:airbyte-api:workload-api` — workload service API
- `:oss:airbyte-api:manifest-server-api` — connector manifest server API

Cloud-only APIs (e.g., `cloud-partner-api`) live under `cloud/` — see
[`cloud/AGENTS.md`](../../cloud/AGENTS.md).

## Spec locations

Each submodule's spec is `src/main/openapi/<name>.yaml`. For example:

- `server-api/src/main/openapi/api.yaml`, `config.yaml`,
  `api_sdk.yaml`, `api_terraform.yaml`, and per-resource
  documentation YAMLs
- `public-api/src/main/openapi/api.yaml`
- `workload-api/src/main/openapi/openapi.yaml`

The exact entry point used by codegen is set in each submodule's
`build.gradle.kts` (look for `val specFile = ...`). For `server-api`
it's currently `config.yaml`.

## Workflow for an API change

1. **Edit the YAML.** Treat it as the contract. Don't add an endpoint
   class in `airbyte-server` that doesn't have a corresponding
   operation in the spec.
2. **Build the submodule** — `./gradlew :oss:airbyte-api:server-api:build`.
   Gradle runs three codegen tasks automatically:
   - `generateApiServer` — JAX-RS Java interfaces
   - `genApiServer2` — Kotlin server interfaces
   - `genApiClient` — Kotlin client classes
   Generated output goes under `<submodule>/build/generated/api/...`
   and is wired into the source sets — you don't manage it manually.
3. **Implement the generated interface** in the consuming service
   (e.g., `airbyte-server/src/main/kotlin/io/airbyte/server/apis/`).
4. **Regenerate the frontend client**:
   `(cd oss/airbyte-webapp && pnpm generate-client)`. This updates
   `oss/airbyte-webapp/src/core/api/generated/`.
5. **Commit YAML + generated frontend output together** in the same
   PR. The backend stubs are not committed (they're in `build/`),
   but they must compile cleanly.
6. **Run `make check.oss`** before declaring done.
7. **If the change affects behavior visible to external API consumers,
   propagate it through the full chain: `server-api` →
   `public-api` spec → public SDKs.**
   Many changes to `server-api` (new enum values, new response
   fields, new endpoints) surface through the public API even when
   you only edit the internal spec. Ask yourself: "will an external
   caller see this value or need this endpoint?" If yes, update the
   `public-api` OpenAPI spec in this repo to match, then propagate
   to the Speakeasy-generated SDKs
   ([airbyte-api-python-sdk](https://github.com/airbytehq/airbyte-api-python-sdk),
   [airbyte-api-java-sdk](https://github.com/airbytehq/airbyte-api-java-sdk)).
   When the public spec and SDKs are not updated in lockstep,
   external consumers (Airflow/Dagster operators, Terraform
   provider, direct API callers) break at runtime — e.g. the
   `JobStatusEnum.QUEUED` incident where the server returned a new
   value the SDK didn't recognize.
   Open a tracking issue or PR on the SDK repos as part of the same
   change, and link it in your platform PR description.

## Conventions

- **Don't reformat the YAML files**. Spotless explicitly excludes
  them (see `server-api/build.gradle.kts` `airbyte.spotless.excludes`).
  Keeping the YAML diff small makes review tractable.
- **Avoid polymorphic discriminators** for fields that are wide
  unions — they currently break `kotlin-server`/`jaxrs-spec` codegen.
  When you need a polymorphic field, mark it as opaque
  (`JsonNode` via `schemaMappings` in the `build.gradle.kts`) and
  parse it into a sealed class downstream with Jackson
  `@JsonTypeInfo`. See existing entries in `schemaMappings`
  (`PrivateLinkServiceConfig`, `DeclarativeManifest`, etc.) for
  examples.
- **Generated Kotlin client uses Failsafe retry policies** —
  injected via post-codegen file rewrites (`updateApiClientWithFailsafe`,
  `updateDomainClientsWithFailsafe` in `server-api/build.gradle.kts`).
  Don't try to manage retry behavior at call sites; configure the
  policy at client construction.
- **One operation per route + verb**. Don't piggyback flags onto an
  existing operation to handle a new flow — define a new endpoint.

## Versioning

- `public-api` is externally documented and treated as a stable
  contract. Breaking changes require deprecation + announcement —
  ask before changing.
- **`server-api` changes that affect external behavior must flow to
  `public-api` and the SDKs.** `server-api` is internal, but many
  of its models (enums, response shapes) are returned through the
  public API. When you change something in `server-api` that
  external consumers will observe, update the `public-api` spec in
  the same PR, then ensure the Speakeasy-generated Python and Java
  SDKs (`airbyte-api` on PyPI, `com.airbyte:api` on Maven) are
  updated and released. A spec change that isn't reflected in a
  published SDK release causes runtime failures when the API
  returns values the SDK doesn't recognize. Treat the full
  `server-api` → `public-api` → SDK propagation as a mandatory
  follow-up, not an optional nice-to-have.
- `server-api`, `workload-api`, `problems-api`,
  `manifest-server-api` are internal; iterate freely, but still
  commit the YAML and regenerated frontend client atomically so the
  webapp stays in sync.

## Common pitfalls

- Frontend `src/core/api/generated/` is out of sync with the YAML →
  rerun `pnpm generate-client`. The `pretest` / `prebuild` scripts
  should normally handle this, but a stale install can skip it.
- Codegen task fails on a `schemaMappings` entry → you added a
  schema that needs to be mapped to `JsonNode`. Add it to all three
  `schemaMappings` blocks in the submodule's `build.gradle.kts`
  (server, server2, client).
- "Bean of type `XyzApi` not found" at server runtime → the
  generated interface is present but the handler implementing it
  isn't annotated `@Singleton` or isn't on the classpath.
