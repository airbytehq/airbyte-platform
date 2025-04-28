/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.Catalog
import org.jooq.DSLContext
import org.jooq.EnumType
import org.jooq.Schema
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.impl.SchemaImpl

private val log = KotlinLogging.logger {}

/**
 * Adds stream_statuses table.
 */
@Suppress("ktlint:standard:class-naming")
class V0_43_2_001__CreateStreamStatusesTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    log.info { "Creating enums" }
    createStatusRunState(ctx)
    createStatusIncompleteRunCause(ctx)
    createStatusJobType(ctx)

    log.info { "Creating table" }
    createStreamStatusesTable(ctx)

    log.info { "Creating indices" }
    createIndices(ctx)

    log.info { "Completed migration: ${javaClass.simpleName}" }
  }

  private fun createStatusRunState(ctx: DSLContext) {
    ctx
      .createType("job_stream_status_run_state")
      .asEnum("pending", "running", "complete", "incomplete")
      .execute()
  }

  private fun createStatusIncompleteRunCause(ctx: DSLContext) {
    ctx
      .createType("job_stream_status_incomplete_run_cause")
      .asEnum("failed", "canceled")
      .execute()
  }

  private fun createStatusJobType(ctx: DSLContext) {
    ctx
      .createType("job_stream_status_job_type")
      .asEnum("sync", "reset")
      .execute()
  }

  private fun createStreamStatusesTable(ctx: DSLContext) {
    // metadata
    val id = DSL.field("id", SQLDataType.UUID.notNull())
    val workspaceId = DSL.field("workspace_id", SQLDataType.UUID.notNull())
    val connectionId = DSL.field("connection_id", SQLDataType.UUID.notNull())
    val jobId = DSL.field("job_id", SQLDataType.BIGINT.notNull())
    val attemptNo = DSL.field("attempt_number", SQLDataType.INTEGER.notNull())
    val streamNamespace = DSL.field("stream_namespace", SQLDataType.VARCHAR.notNull())
    val streamName = DSL.field("stream_name", SQLDataType.VARCHAR.notNull())
    val jobType = DSL.field("job_type", SQLDataType.VARCHAR.asEnumDataType(StatusJobType::class.java).notNull())

    // row timestamps
    val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()))
    val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull().defaultValue(DSL.currentOffsetDateTime()))

    // values
    val state = DSL.field("run_state", SQLDataType.VARCHAR.asEnumDataType(RunState::class.java).notNull())
    val incompleteCause = DSL.field("incomplete_run_cause", SQLDataType.VARCHAR.asEnumDataType(IncompleteRunCause::class.java).nullable(true))
    val transitionedAt = DSL.field("transitioned_at", SQLDataType.TIMESTAMPWITHTIMEZONE.notNull())

    ctx
      .createTableIfNotExists("stream_statuses")
      .columns(
        id,
        workspaceId,
        connectionId,
        jobId,
        streamNamespace,
        streamName,
        createdAt,
        updatedAt,
        attemptNo,
        jobType,
        state,
        incompleteCause,
        transitionedAt,
      ).constraints(
        DSL.primaryKey(id),
        DSL.foreignKey(jobId).references("jobs", "id").onDeleteCascade(),
      ).execute()
  }

  private fun createIndices(ctx: DSLContext) {
    ctx
      .createIndexIfNotExists("stream_status_connection_id_idx")
      .on("stream_statuses", "connection_id")
      .execute()
    ctx.createIndexIfNotExists("stream_status_job_id_idx").on("stream_statuses", "job_id").execute()
  }
}

private enum class RunState(
  private val literal: String,
) : EnumType {
  PENDING("pending"),
  RUNNING("running"),
  COMPLETE("complete"),
  INCOMPLETE("incomplete"),
  ;

  override fun getCatalog(): Catalog? = schema.catalog

  override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

  override fun getName(): String = "job_stream_status_run_state"

  override fun getLiteral(): String = literal
}

private enum class IncompleteRunCause(
  private val literal: String,
) : EnumType {
  FAILED("failed"),
  CANCELED("canceled"),
  ;

  override fun getCatalog(): Catalog? = schema.catalog

  override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

  override fun getName(): String = "job_stream_status_incomplete_run_cause"

  override fun getLiteral(): String = literal
}

private enum class StatusJobType(
  private val literal: String,
) : EnumType {
  SYNC("sync"),
  RESET("reset"),
  ;

  override fun getCatalog(): Catalog? = schema.catalog

  override fun getSchema(): Schema = SchemaImpl(DSL.name("public"), null)

  override fun getName(): String = "job_stream_status_job_type"

  override fun getLiteral(): String = literal
}
