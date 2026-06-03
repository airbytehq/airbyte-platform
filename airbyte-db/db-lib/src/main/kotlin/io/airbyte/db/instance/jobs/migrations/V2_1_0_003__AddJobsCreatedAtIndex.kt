/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Add index on jobs(created_at) to support the DbPrune cron job which queries
 * SELECT id FROM jobs WHERE created_at < ? LIMIT ?
 *
 * Without this index the query must seq scan the entire jobs table (~428 GB),
 * causing the nightly prune job to time out and leaving old data uncleaned.
 */
@Suppress("ktlint:standard:class-naming")
class V2_1_0_003__AddJobsCreatedAtIndex : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)

    ctx
      .query("CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_created_at_idx ON jobs(created_at)")
      .execute()
  }

  // This prevents flyway from automatically wrapping the migration in a transaction.
  // This is important because indexes cannot be created concurrently (i.e. without locking) from within a transaction.
  override fun canExecuteInTransaction(): Boolean = false
}
