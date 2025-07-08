/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Add composite index on (status, scope, config_type) to the jobs table for improved query performance.
 */
@Suppress("ktlint:standard:class-naming")
class V1_1_0_003__AddJobsStatusScopeConfigTypeIndexes : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx: DSLContext = DSL.using(context.connection)

    ctx
      .query(
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS $JOBS_CONFIG_SCOPE_CREATED_AT_INDEX
        ON jobs(config_type, scope, created_at DESC)
        INCLUDE (status)
        """.trimIndent(),
      ).execute()

    ctx
      .query(
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS $JOBS_STATUS_CONFIG_SCOPE_INDEX
        ON jobs(status, config_type, scope)
        """.trimIndent(),
      ).execute()
  }

  // This prevents flyway from automatically wrapping the migration in a transaction.
  // This is important because indexes cannot be created concurrently (i.e. without locking) from within a transaction.
  override fun canExecuteInTransaction(): Boolean = false
}

private const val JOBS_CONFIG_SCOPE_CREATED_AT_INDEX = "jobs_config_scope_created_at_idx"
private const val JOBS_STATUS_CONFIG_SCOPE_INDEX = "jobs_status_config_scope_idx"
