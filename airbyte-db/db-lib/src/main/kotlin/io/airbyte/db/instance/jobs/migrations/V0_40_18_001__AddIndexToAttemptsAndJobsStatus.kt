/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

/**
 * Add index on job and attempt statuses.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_18_001__AddIndexToAttemptsAndJobsStatus : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    ctx.createIndexIfNotExists("attempts_status_idx").on(ATTEMPTS_TABLE, "status").execute()
    ctx.createIndexIfNotExists("jobs_status_idx").on(JOBS_TABLE, "status").execute()
  }
}

private const val ATTEMPTS_TABLE = "attempts"
private const val JOBS_TABLE = "jobs"
