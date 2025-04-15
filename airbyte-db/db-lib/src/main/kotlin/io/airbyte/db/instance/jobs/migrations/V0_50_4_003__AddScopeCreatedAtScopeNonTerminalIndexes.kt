/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_50_4_003__AddScopeCreatedAtScopeNonTerminalIndexes : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    // helps with the general sorting of jobs by latest per connection
    ctx
      .query("CREATE INDEX CONCURRENTLY IF NOT EXISTS scope_created_at_idx ON jobs(scope, created_at DESC)")
      .execute()

    // helps for looking for active jobs
    ctx
      .query(
        """
        CREATE INDEX CONCURRENTLY IF NOT EXISTS scope_non_terminal_status_idx ON jobs(scope, status) 
        WHERE status NOT IN ('failed', 'succeeded', 'cancelled')
        """.trimIndent(),
      ).execute()

    // remove other index, as these two are more performant
    ctx
      .query("DROP INDEX CONCURRENTLY $SCOPE_STATUS_CREATED_AT_INDEX_NAME")
      .execute()
  }

  // This prevents flyway from automatically wrapping the migration in a transaction.
  // This is important because indexes cannot be created concurrently (i.e. without locking) from
  // within a transaction.
  override fun canExecuteInTransaction(): Boolean = false
}
