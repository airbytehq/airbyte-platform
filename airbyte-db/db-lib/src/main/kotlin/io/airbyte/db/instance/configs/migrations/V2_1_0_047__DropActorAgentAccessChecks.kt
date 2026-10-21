/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Drops the `actor_indexing_requires_agent_access` and `actor_backfill_requires_agent_access` checks added by
 * [V2_1_0_040__AddActorEnablements]. Fusion indexing and backfill moved to the connection, so the actor's legacy
 * `enable_indexing`/`enable_backfill` are no longer written, and these checks would otherwise reject revoking agent
 * access on actors that still have either flag set. The columns themselves are dropped by a later migration.
 *
 * Rollback is a new forward migration that re-adds both checks `NOT VALID`: rows written after this migration may
 * violate them, and `NOT VALID` enforces the checks on new writes without failing on those rows.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_047__DropActorAgentAccessChecks : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    ctx.execute(
      """
      ALTER TABLE actor
        DROP CONSTRAINT IF EXISTS actor_indexing_requires_agent_access,
        DROP CONSTRAINT IF EXISTS actor_backfill_requires_agent_access
      """.trimIndent(),
    )
  }
}
