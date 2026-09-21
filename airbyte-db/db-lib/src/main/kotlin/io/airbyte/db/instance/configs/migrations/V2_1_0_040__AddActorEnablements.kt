/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context

private val log = KotlinLogging.logger {}

/** Additive actor intent; application rollback leaves these columns intact. A later migration may drop them. */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_040__AddActorEnablements : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Existing rows trivially satisfy the constraints via the column defaults; NOT VALID skips a full scan of actor under
    // ACCESS EXCLUSIVE while the constraints are still enforced for all inserts/updates.
    context.connection.createStatement().use { statement ->
      statement.execute(
        """
        ALTER TABLE actor
          ADD COLUMN enable_agent_access boolean NOT NULL DEFAULT false,
          ADD COLUMN enable_indexing boolean NOT NULL DEFAULT false,
          ADD COLUMN enable_backfill boolean NOT NULL DEFAULT false,
          ADD COLUMN backfill_start_time timestamp with time zone,
          ADD CONSTRAINT actor_indexing_requires_agent_access CHECK (NOT enable_indexing OR enable_agent_access) NOT VALID,
          ADD CONSTRAINT actor_backfill_requires_agent_access CHECK (NOT enable_backfill OR enable_agent_access) NOT VALID,
          ADD CONSTRAINT actor_backfill_destination_only CHECK (actor_type = 'destination' OR (NOT enable_backfill AND backfill_start_time IS NULL)) NOT VALID
        """.trimIndent(),
      )
    }
  }
}
