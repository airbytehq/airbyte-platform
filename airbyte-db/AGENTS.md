# AGENTS.md — `oss/airbyte-db/`

Conventions for database schemas, migrations, and jOOQ codegen. Read
the root [AGENTS.md](../../AGENTS.md) and [`oss/AGENTS.md`](../AGENTS.md)
first.

## Two databases

The platform uses **two logical PostgreSQL databases**, each managed
independently:

- **`configs_database`** — connectors, connections, workspaces,
  organizations, secrets references, etc. (the "control plane")
- **`jobs_database`** — sync job history, attempts, stream stats
  (the "operational plane")

They have separate schemas, separate Flyway migration histories, and
separate jOOQ generated classes. Never mix concerns across them.

## Module layout

- `:oss:airbyte-db:db-lib` — Flyway-driven migration code,
  bootstrap SQL, instance + JDBC helpers
- `:oss:airbyte-db:jooq` — jOOQ codegen consumer (generated record /
  table classes, kept in build outputs — do not edit by hand)

## File layout

- `db-lib/src/main/resources/init.sql` — initial bootstrap, runs once
  before Flyway takes over
- `db-lib/src/main/resources/configs_database/schema.sql` — current
  configs schema snapshot (regenerated, do not hand-edit)
- `db-lib/src/main/resources/jobs_database/schema.sql` — current jobs
  schema snapshot (regenerated, do not hand-edit)
- `db-lib/src/main/resources/migration_template.txt` — template for
  new Flyway migration classes
- `db-lib/src/main/kotlin/io/airbyte/db/instance/configs/migrations/` —
  **configs migrations** (Kotlin classes, not raw `.sql` files)
- `db-lib/src/main/kotlin/io/airbyte/db/instance/jobs/migrations/` —
  **jobs migrations** (Kotlin classes)

## Migration discipline

- **Migrations are Kotlin classes that extend Flyway's
  `org.flywaydb.core.api.migration.BaseJavaMigration`** and override
  `migrate(context: Context)`. They live in the `migrations/` package
  of the appropriate `instance/`. Use `migration_template.txt` as the
  starting point.
- **Class-name pattern**: `V<major>_<minor>_<patch>_<sequence>__<Description>`
  — current series is `V2_1_0_NNN` (e.g.,
  `V2_1_0_027__AddDefaultRoleToSsoConfig.kt`,
  `V2_1_0_001__AddStreamStatsAdditionalStats.kt`). Match the leading
  prefix of the most recent migration in the same `instance/` and
  bump the trailing sequence; `<sequence>` is a 3-digit counter for
  migrations within the same `<major>_<minor>_<patch>` series.
  Suppress ktlint's class-naming complaint with
  `@Suppress("ktlint:standard:class-naming")` on the class.
- **Keep migrations as simple as possible. Prefer raw SQL via
  `ctx.execute("...")` over jOOQ DSL builders.** Recent migrations
  (the `V2_*` series) write both DDL and data backfills as raw SQL
  strings, typically referencing table-name constants from
  `DatabaseConstants`. Example:
  ```kotlin
  ctx.execute("ALTER TABLE $SSO_CONFIG_TABLE ADD COLUMN default_role permission_type")
  ctx.execute(
    """
    UPDATE $PRIVATE_LINK_TABLE
    SET service_config = service_config || jsonb_build_object('type', service_type::text)
    WHERE NOT (service_config ?? 'type')
    """.trimIndent()
  )
  ```
  jOOQ DSL builders (`ctx.alterTable("t").addColumn(...).execute()`,
  `dropColumnIfExists(...)`) are allowed only when they genuinely
  buy something — e.g., a portable conditional drop. When in doubt,
  write raw SQL.
- **Do NOT use jOOQ-generated code (record/table classes) inside
  migrations.** They evolve with the schema — referencing them
  means old migrations stop compiling when the schema moves forward.
  Get a `DSLContext` from `org.jooq.impl.DSL.using(context.connection)`
  and use raw SQL (per above), not typed jOOQ DSL references.
- **One concern per migration.** Do not mix a schema change with a
  data backfill in the same revision — split them. Schema first,
  then a follow-up data migration. This makes rollback tractable.
- **Plan for rollback.** Schema migrations should be reversible
  (drop the column, drop the index). If you genuinely cannot reverse
  (a destructive data transform), document why in the migration
  class's KDoc.
- **Migrations go in their own PR.** Do not bundle a migration with
  unrelated code — they're reviewed and rolled back independently.
- **Never modify an existing migration that's been merged to
  `master`.** Add a new migration that fixes the prior one. Edits to
  applied migrations break Flyway's checksum validation across all
  environments.
- **Update the schema snapshot.** When your migration runs locally,
  it should regenerate `configs_database/schema.sql` /
  `jobs_database/schema.sql`. Commit the updated snapshot alongside
  the migration class.

## jOOQ codegen

- jOOQ classes are generated from the current database schema, not
  from `.sql` files. The codegen runs against a Testcontainers
  Postgres that has all current migrations applied.
- Generated classes live under `airbyte-db/jooq/build/generated/` and
  are added to the `:oss:airbyte-db:jooq` source set. **Do not edit
  generated classes by hand.**
- After adding a migration, `./gradlew :oss:airbyte-db:jooq:build`
  to refresh the generated classes. Commit the regenerated output
  alongside the migration.

## Local migration verification

Before committing a new migration:

```bash
# Run the migration against a local Postgres
./gradlew :oss:airbyte-db:db-lib:test --tests "*.MigrationsTest"

# Refresh jOOQ generated classes
./gradlew :oss:airbyte-db:jooq:build

# Repo-wide check
make check.oss
```

If you have a local deploy running (`make dev.up`), the bootloader
applies migrations on container start.

## Multi-tenant safety (Cloud)

Any new column / table that holds Cloud customer data **must** include
an `organization_id` column (or join through one). The query layer is
responsible for the filter, but the schema is responsible for making
it possible. See root [AGENTS.md](../../AGENTS.md) for the query-side
discipline.

## Common pitfalls

- "Migration checksum mismatch" → someone (maybe you) edited an
  applied migration. The fix is a *new* migration that corrects the
  prior one, not editing the old one.
- "Cannot drop column because dependent objects exist" → there's a
  view or foreign key referencing it. Drop those first in the same
  migration.
- jOOQ generated classes are stale in your IDE → run
  `./gradlew :oss:airbyte-db:jooq:build` and refresh.
