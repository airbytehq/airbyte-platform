# AGENTS.md — OSS (Kotlin / Micronaut Backend)

Conventions for the Kotlin + Micronaut services under `oss/` (everything
except `oss/airbyte-webapp/` — see its own AGENTS.md). Read the root
[AGENTS.md](../AGENTS.md) first.

## Stack

- **Kotlin** on **Micronaut** (with KSP annotation processors). Not
  Spring, not Java-first. Source in `src/main/kotlin/`, tests in
  `src/test/kotlin/`.
- **Gradle (Kotlin DSL)** with custom plugins:
  - `io.airbyte.gradle.jvm.app` — applications
  - `io.airbyte.gradle.jvm.lib` — libraries
  - `io.airbyte.gradle.docker` — Docker image build
  - `io.airbyte.gradle.publish` — artifact publishing
  - `io.airbyte.gradle.kube-reload` — local kube hot-reload
- **Dependency catalog**: `oss/deps.toml` (referenced as `libs.*` in
  `build.gradle.kts` files). Add new deps there, not inline.
- **Tests**: JUnit 5 + **MockK** (not Mockito) + AssertJ +
  `kotlin.test.runner.junit5` + junit-pioneer + mockwebserver for HTTP
  fakes. Testcontainers where infra is required.

## Module layout

Canonical list of modules is in the repo root `settings.gradle.kts`.
Common ones:

- `:oss:airbyte-server` — primary REST server
- `:oss:airbyte-workload-api-server` — workload API
- `:oss:airbyte-workers` — sync workers
- `:oss:airbyte-workload-launcher` — kube launcher
- `:oss:airbyte-bootloader` — startup migrations / setup
- `:oss:airbyte-cron` — scheduled jobs
- `:oss:airbyte-keycloak-setup` — see [its AGENTS.md](airbyte-keycloak-setup/AGENTS.md)
- `:oss:airbyte-api:*` — see [airbyte-api/AGENTS.md](airbyte-api/AGENTS.md)
- `:oss:airbyte-db:db-lib`, `:oss:airbyte-db:jooq` — see [airbyte-db/AGENTS.md](airbyte-db/AGENTS.md)
- `:oss:airbyte-domain:models`, `:oss:airbyte-domain:services` —
  domain layer (where data access *should* go)
- `:oss:airbyte-data` — data access. Two distinct patterns coexist:
  Micronaut Data interfaces under `data/repositories/` and hand-written
  jOOQ service implementations under `data/services/impls/jooq/`. The
  two never mix in the same class. **Prefer routing through
  `airbyte-domain` for new code** (there's an active migration away
  from direct `airbyte-data` dependencies — see the `TODO` in
  `airbyte-server/build.gradle.kts`)
- `:oss:airbyte-config:*` — config models, persistence, secrets, specs
- `:oss:airbyte-commons*` — cross-cutting utilities

## Service-internal layout

Inside an application module (e.g., `airbyte-server`), package layout
follows:

- `apis/` — JAX-RS endpoint classes (thin, delegate to handlers)
- `handlers/` — request orchestration, business logic
- `services/` — reusable domain logic
- `repositories/` — Micronaut Data repositories
- `helpers/` — pure utilities
- `auth/`, `config/` — cross-cutting concerns
- `Application.kt` — Micronaut entry point

Endpoints stay thin. Don't put business logic in `apis/` classes —
delegate to a handler or service.

## Dependency injection

- Use Micronaut DI: `@Singleton` on services, `@Inject` (or
  constructor injection — preferred) for dependencies.
- KSP processors generate bean factories at compile time. If a class
  isn't being discovered as a bean, check that KSP ran (it's wired
  via `ksp(...)` declarations in `build.gradle.kts`).
- Bean factories with `@Factory` for cases where the bean isn't a
  simple class (third-party clients, conditional beans).

## Architectural rules

- **Do not add new direct `airbyte-data` dependencies** to application
  modules. New data access goes through `airbyte-domain:services`. If
  you're tempted to add a `:oss:airbyte-data` import, ask first.
- **Multi-tenant safety**: see root AGENTS.md. Cloud services running
  in shared deployments must filter every org-scoped query by
  `organization_id`.
- **OpenAPI is the source of truth for HTTP contracts.** Edit
  `oss/airbyte-api/<submodule>/src/main/openapi/*.yaml`, let Gradle
  regenerate the JAX-RS interfaces, then implement them. Don't add
  endpoint classes that don't correspond to a YAML operation.

## Build & test commands

Always prefer scoped commands during iteration:

- **Format a module**: `./gradlew :oss:airbyte-server:spotlessApply`
- **Format everything (repo)**: `make format.oss`
- **Check a module (compile + test + spotless check)**:
  `./gradlew :oss:airbyte-server:check`
- **Run one test class**:
  `./gradlew :oss:airbyte-server:test --tests "*.MyHandlerTest"`
- **Full backend build**: `make build.oss` (delegates to
  `tools/bin/workflow/<env>/oss/build-all.sh`)
- **Full OSS tests**: `make check.oss`

Before declaring work done, run `make check.oss` (or at minimum
`:check` on every touched module).

## Logging

- Use `kotlin-logging` (`libs.kotlin.logging`) rather than direct slf4j.
- Lazy lambda form: `logger.info { "message $expensiveValue" }` so
  formatting is skipped when the level is disabled.

## Common pitfalls

- A new bean isn't picked up → check KSP ran on the module
  (`./gradlew :oss:<module>:kspKotlin --info`).
- A new endpoint returns 404 → check it's defined in the OpenAPI YAML
  *and* the handler implements the generated interface.
- A test passes locally but fails in CI → likely a Testcontainers
  Postgres version mismatch or a missing `@MicronautTest` annotation.
