/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.Table
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Adds metadata column in jobs table.
 */
@Suppress("ktlint:standard:class-naming")
class V1_1_0_000__AddMetadataInJobs : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    log.info { "Add metadata column" }
    addMetadataInJobs(ctx)

    log.info { "Completed migration: ${javaClass.simpleName}" }
  }

  private fun addMetadataInJobs(ctx: DSLContext) {
    val jobsTable: Table<*> = DSL.table("jobs")
    val metadata = DSL.field("metadata", SQLDataType.JSONB.nullable(true))

    ctx
      .alterTable(jobsTable)
      .addColumnIfNotExists(metadata)
      .execute()
  }
}
