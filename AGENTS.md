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
- `:oss:airbyte-api:*` — see [airbyte-api/AGENTS.md](airbyte-api/AGENTS.md)
- `:oss:airbyte-db:db-lib`, `:oss:airbyte-db:jooq` — see [airbyte-db/AGENTS.md](airbyte-db/AGENTS.md)
- `:oss:airbyte-domain:models`, `:oss:airbyte-domain:services` —
  domain layer (business rules and transaction boundaries; queries go
  through `airbyte-data` repositories)
- `:oss:airbyte-data` — data access. Two distinct patterns coexist:
  Micronaut Data interfaces under `data/repositories/` and hand-written
  jOOQ service implementations under `data/services/impls/jooq/`. The
  two never mix in the same class. **Prefer routing through
  `airbyte-domain` for new code** (there's an active migration away
  from direct `airbyte-data` dependencies — see the `TODO` in
  `airbyte-server/build.gradle.kts`)
- `:oss:airbyte-config:*` — config models, persistence, secrets, specs
- `:oss:airbyte-commons*` — cross-cutting utilities

## Layering: Controller → Domain Service → Data layer

New code follows three layers. Each layer calls only the one below it.

1. **Controller** (`oss/airbyte-server/.../apis/controllers/`)
   implements the generated OpenAPI interface. It owns the HTTP
   concerns: `@Secured`, `@ExecuteOn`, `@AuditLogging`, converting
   generated API models to and from `airbyte-domain:models` types,
   and turning domain exceptions into Problems. It contains no
   business logic and calls domain services only.
2. **Domain service** (`oss/airbyte-domain/services/`, package
   `io.airbyte.domain.services.<feature>`) owns the business rules,
   validation, and transaction boundaries (`@Transactional` or
   `TransactionOperations`). It takes and returns domain types
   (`io.airbyte.domain.models.<feature>`), never generated API
   models. It signals failures with domain exceptions defined in
   `airbyte-domain:models`.
3. **Data layer** (`oss/airbyte-data`) holds every database read and
   write. **All data access and all SQL goes through a repository**
   (a Micronaut Data interface under `data/repositories/`, with
   `@Query` for custom SQL). Domain services call repositories. They
   never hold a `DSLContext` or a `DataSource`, and never run SQL
   themselves.

**New code must not use handlers.** Don't add a class to `handlers/`
or to `airbyte-commons-server`, and don't route a new endpoint
through an existing handler.

**Reference implementation: SCIM configuration.**
`ScimConfigApiController`
(`oss/airbyte-server/src/main/kotlin/io/airbyte/server/apis/controllers/`)
calls `ScimConfigurationService`
(`oss/airbyte-domain/services/src/main/kotlin/io/airbyte/domain/services/scim/`).
That service uses `ScimConfigurationRepository` and
`OrganizationRepository` from `airbyte-data` inside a
`TransactionOperations` write. Copy its layering. Don't copy its
error mapping: it predates the Problems rule below and throws
`KnownException` subclasses.

**Existing code.** A change to an existing feature can go in the
handler or service that already owns it. Don't restructure that code
into the three layers unless the task asks for it. New endpoints and
new features always use the three layers.

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
- **SQL only in repositories; transactions only in domain services or
  persistence modules.** Only `airbyte-data` and
  `airbyte-config:config-persistence` may reference
  `javax.sql.DataSource`, `DSLContext`, or `DataSourceUnwrapper`, or
  call `connection.autoCommit`, `rollback()`, or `prepareStatement`.
  `airbyte-domain:services` may also use `@Transactional` or
  `TransactionOperations`, but calls repositories for every query.
  No other module opens a transaction. Never build a custom
  transaction handler, advisory lock, or other database mutex. See
  the root AGENTS.md transaction rule for alternatives to locking, and
  its data-layer rule for the exceptions (`@Factory` bean wiring,
  migrations). Existing usages in `airbyte-commons-server`
  handlers (`UserHandler`, `ResourceBootstrapHandler`,
  `DsrDeletionService`, and others) predate this rule. They are not a
  pattern to follow.
- **Multi-tenant safety**: see root AGENTS.md. Cloud services running
  in shared deployments must filter every org-scoped query by
  `organization_id`.
- **OpenAPI is the source of truth for HTTP contracts.** Edit
  `oss/airbyte-api/<submodule>/src/main/openapi/*.yaml`, let Gradle
  regenerate the JAX-RS interfaces, then implement them. Don't add
  endpoint classes that don't correspond to a YAML operation.
- **Config API routes are subject to change; Public API routes are
  final.** `server-api` (the Config API) is internal and may evolve
  freely. `public-api` routes are static and relied upon by external
  (non-Airbyte) users — see
  [airbyte-api/AGENTS.md](airbyte-api/AGENTS.md#versioning).
- **Decide the edition before you implement an endpoint.** Every
  controller in `oss/airbyte-server` ships in the community edition.
  If the feature is Cloud-only (billing, entitlement plan admin, Data
  Worker allocation, and similar), the OSS controller method must
  throw `ApiNotImplementedInOssProblem`, and the real implementation
  goes in a `cloud/airbyte-server-wrapped` controller that extends the
  OSS controller and carries `@Replaces`. The route stays in the
  shared OpenAPI YAML; only the controller body is edition-specific.
  See [`cloud/AGENTS.md`](../cloud/AGENTS.md) for the wrapping
  pattern, and PR #19515 for a reference migration of routes that were
  wrongly available in OSS. When the edition is unclear, ask before
  implementing.
- **Return API errors as Problems (RFC 9457), not `KnownException`.**
  Problems are defined in
  `oss/airbyte-api/problems-api/src/main/openapi/api-problems.yaml`.
  Each `<Name>ProblemResponse` schema (`x-implements:
  io.airbyte.api.problems.ProblemResponse`, `allOf` with
  `BaseProblemFields`) generates a throwable `<Name>Problem` in
  `io.airbyte.api.problems.throwable.generated`.
  `AbstractThrowableProblemHandler` turns it into the HTTP response.
  - Throw the problem whose HTTP status describes the failure:
    `BadRequestProblem` (400) for malformed input, `ForbiddenProblem`
    (403) when the caller lacks permission, `ResourceNotFoundProblem`
    (404) when the target doesn't exist, `StateConflictProblem` (409)
    when the current state blocks the request,
    `UnprocessableEntityProblem` (422) for well-formed input that
    fails a business rule, and `UnexpectedProblem` (500) only when
    the server itself failed. Never report a client error as a 500,
    or a server failure as a 4xx.
  - Prefer those generic problems to new ones, and pass context
    through `detail` or a typed `data` payload, for example
    `throw ResourceNotFoundProblem(ProblemResourceData().resourceId(id))`.
  - Add a new problem only when the client must branch on it. The
    schema's `default` values for `status`, `type`, and `title` are
    constants for that problem. Set `type` to
    `https://reference.airbyte.com/reference/errors#<slug>`. If the
    payload needs structure, add a `Problem<Name>Data` schema and
    reference it from `data`.
  - Treat an existing problem's `type` string as a contract. The
    webapp matches on it (`HttpProblem.isType`), so renaming it breaks
    error handling without a compile error.
  - `KnownException` subclasses still exist in older handlers. Don't
    add new ones.
- **Every controller method that blocks must declare `@ExecuteOn`.**
  Blocking means it touches the database, calls another service, or
  waits on Temporal, which covers nearly every endpoint. Without the
  annotation the method runs on a Netty event-loop thread, and one
  slow call stalls every request on that loop. Put it on each method,
  or on the class when every method uses the same pool. Use the
  constants in `AirbyteTaskExecutors`
  (`oss/airbyte-commons-server/.../scheduling/AirbyteTaskExecutors.kt`),
  not Micronaut's `TaskExecutors` directly. Pick the pool by the kind
  of work:
  - `IO`: the default for Config API controllers in
    `apis/controllers/` that read or write through domain services.
  - `PUBLIC_API`: public API endpoints under
    `apis/publicapi/controllers/`. The pool is small (5 threads by
    default), which caps how much server capacity public API traffic
    can use.
  - `SCHEDULER`: endpoints that start, run, or wait on connector jobs,
    such as `check_connection`, `discover_schema`, `sync`, `refresh`,
    and job create/cancel. These are slow, and a separate pool keeps
    them from using up `IO`.
  - `WEBHOOK`: inbound third-party webhooks, such as billing.
  - `HEALTH`: health-check endpoints only.
  - `WORKLOAD`: `airbyte-workload-api-server` only.
  - `DSR_DELETION` / `DSR_DELETION_HEARTBEAT`: background deletion
    work, not controllers.

  Pools are fixed-size and configured under `micronaut.executors` in
  each service's `src/main/resources/application.yml` (for example
  `oss/airbyte-server/src/main/resources/application.yml`). Each size
  comes from an env var such as `IO_TASK_EXECUTOR_THREADS`; the
  `application.yml` entry names it. Don't add a
  new pool unless an existing one clearly doesn't fit. A new pool
  needs a constant in `AirbyteTaskExecutors` and an entry in every
  `application.yml` that uses it, and its size must fit the database
  connection pool, because threads beyond the pool's connections just
  wait for one.

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

## Local entitlements overrides

To grant or deny entitlements in a local Cloud deployment:

1. Copy `oss/entitlements.yml.example` to `oss/entitlements.yml` (the
   latter is gitignored — it's your personal local config).
2. Fill in the ids you want to control. Valid ids are the `featureId`
   values in
   `oss/airbyte-commons-entitlements/src/main/kotlin/io/airbyte/commons/entitlements/models/EntitlementDefinitions.kt`.
   `true` grants, `false` explicitly denies (same as omitting it), and an
   integer grants a numeric entitlement that finite value (a `true` on a
   numeric entitlement grants it as unlimited). The plan is expressed as
   the `feature-plan-name` entitlement: a string value (matched against
   an `EntitlementPlan` id, enum name, or display name, e.g. `plus`)
   sets the plan.
3. Run `make deploy.cloud` — when the file exists, the deploy script
   mounts it into the server at `/etc/airbyte/entitlements.yml` and
   sets `STIGG_ENTITLEMENTS_FILE`. Without the file, nothing changes.
4. Restart the server after editing the file for changes to take
   effect.

## Common pitfalls

- A new bean isn't picked up → check KSP ran on the module
  (`./gradlew :oss:<module>:kspKotlin --info`).
- A new endpoint returns 404 → check it's defined in the OpenAPI YAML
  *and* the controller implements the generated interface.
- A test passes locally but fails in CI → likely a Testcontainers
  Postgres version mismatch or a missing `@MicronautTest` annotation.
