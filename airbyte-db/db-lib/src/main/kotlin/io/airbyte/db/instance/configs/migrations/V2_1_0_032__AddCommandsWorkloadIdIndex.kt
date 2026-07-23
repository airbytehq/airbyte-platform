/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Add an index on commands(workload_id) to support workload deletes.
 *
 * The commands table can be large in production. Without this index, deleting
 * workload rows can force Postgres to scan commands while enforcing the
 * commands_workload_id_fkey ON DELETE CASCADE relationship, causing DSR cleanup
 * queries to time out.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_032__AddCommandsWorkloadIdIndex : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    addCommandsWorkloadIdIndex(ctx)
  }

  // This prevents Flyway from automatically wrapping the migration in a transaction.
  // This is required because indexes cannot be created concurrently from within a transaction.
  override fun canExecuteInTransaction(): Boolean = false

  companion object {
    @JvmStatic
    fun addCommandsWorkloadIdIndex(ctx: DSLContext) {
      ctx.execute(
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS commands_workload_id_idx
        ON commands (workload_id)
        """.trimIndent(),
      )
    }
  }
}
