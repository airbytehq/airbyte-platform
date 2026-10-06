/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context

private val log = KotlinLogging.logger {}

/**
 * Adds Fusion enablement to the connection table, starting with indexing. Existing actor-level
 * enablement columns are unaffected and not backfilled into this column; every connection starts
 * disabled. Additive column; application rollback leaves it unused. Kept to a single column so a
 * later migration can add further connection-level Fusion flags the same way.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_042__AddConnectionEnablements : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    context.connection.createStatement().use { statement ->
      statement.execute("ALTER TABLE connection ADD COLUMN enable_indexing boolean NOT NULL DEFAULT false")
    }
  }
}
