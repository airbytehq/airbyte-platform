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

@Suppress("ktlint:standard:class-naming")
class V1_8_1_004__AddConcurrentIndexToConnectionTimelineEventJobId : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx = DSL.using(context.connection)

    // Create concurrent partial index on job_id with IS NOT NULL condition
    // The partial index is required as not all events contain job IDs, and we want to optimize queries filtering by job_id
    addJobIdIndex(ctx)
  }

  // This prevents flyway from automatically wrapping the migration in a transaction.
  // This is important because indexes cannot be created concurrently (i.e. without locking) from within a transaction.
  override fun canExecuteInTransaction(): Boolean = false

  companion object {
    @JvmStatic
    fun addJobIdIndex(ctx: DSLContext) {
      ctx
        .query(
          """
          CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_connection_timeline_event_job_id
          ON connection_timeline_event (job_id) 
          WHERE job_id IS NOT NULL
          """.trimIndent(),
        ).execute()
    }
  }
}
