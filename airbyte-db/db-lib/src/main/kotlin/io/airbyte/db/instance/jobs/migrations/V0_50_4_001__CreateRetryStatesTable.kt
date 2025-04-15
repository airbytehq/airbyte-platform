/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Adds the table to store retry state to be accessed by the connection manager workflow.
 */
@Suppress("ktlint:standard:class-naming")
class V0_50_4_001__CreateRetryStatesTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    log.info { "Creating table" }
    createRetryStatesTable(ctx)

    log.info { "Creating indices" }
    createIndices(ctx)

    log.info { "Completed migration: ${javaClass.simpleName}" }
  }
}

private fun createRetryStatesTable(ctx: DSLContext) {
  // metadata
  val id = DSL.field("id", SQLDataType.UUID.notNull())
  val connectionId = DSL.field("connection_id", SQLDataType.UUID.notNull())
  val jobId = DSL.field("job_id", SQLDataType.BIGINT.notNull())

  // row timestamps
  val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()))
  val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()))

  // values
  val successiveCompleteFailures = DSL.field("successive_complete_failures", SQLDataType.INTEGER.notNull())
  val totalCompleteFailures = DSL.field("total_complete_failures", SQLDataType.INTEGER.notNull())
  val successivePartialFailures = DSL.field("successive_partial_failures", SQLDataType.INTEGER.notNull())
  val totalPartialFailures = DSL.field("total_partial_failures", SQLDataType.INTEGER.notNull())

  ctx
    .createTableIfNotExists("retry_states")
    .columns(
      id,
      connectionId,
      jobId,
      createdAt,
      updatedAt,
      successiveCompleteFailures,
      totalCompleteFailures,
      successivePartialFailures,
      totalPartialFailures,
    ).constraints(
      DSL.primaryKey(id),
      DSL.foreignKey(jobId).references("jobs", "id").onDeleteCascade(),
      DSL.constraint("uniq_job_id").unique("job_id"),
    ).execute()
}

private fun createIndices(ctx: DSLContext) {
  ctx.createIndexIfNotExists("retry_state_connection_id_idx").on("retry_states", "connection_id").execute()
  ctx.createIndexIfNotExists("retry_state_job_id_idx").on("retry_states", "job_id").execute()
}
