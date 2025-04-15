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
 * Add indices to jobs table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_35_62_001__AddJobIndices : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    ctx.createIndexIfNotExists("jobs_config_type_idx").on("jobs", "config_type").execute()
    ctx.createIndexIfNotExists("jobs_scope_idx").on("jobs", "scope").execute()
  }
}
