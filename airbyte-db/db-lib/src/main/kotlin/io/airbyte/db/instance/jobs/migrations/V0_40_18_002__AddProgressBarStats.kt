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
 * The estimated columns contains the overall estimated records and bytes for an attempt.
 *
 *
 * The new stream_stats table contains the estimated and emitted records/bytes for an attempt at the
 * per-stream level. This lets us track per-stream stats as an attempt is in progress.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_18_002__AddProgressBarStats : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx = DSL.using(context.connection)
    addEstimatedColumnsToSyncStats(ctx)
    addStreamStatsTable(ctx)
  }
}

private fun addEstimatedColumnsToSyncStats(ctx: DSLContext) {
  ctx
    .alterTable("sync_stats")
    .add(
      DSL.field("estimated_records", SQLDataType.BIGINT.nullable(true)),
      DSL.field("estimated_bytes", SQLDataType.BIGINT.nullable(true)),
    ).execute()
}

private fun addStreamStatsTable(ctx: DSLContext) {
  // Metadata Columns
  val id = DSL.field("id", SQLDataType.UUID.nullable(false))
  val attemptId = DSL.field("attempt_id", SQLDataType.INTEGER.nullable(false))
  val streamNamespace = DSL.field("stream_namespace", SQLDataType.VARCHAR.nullable(false))
  val streamName = DSL.field("stream_name", SQLDataType.VARCHAR.nullable(false))

  // Stats Columns
  val recordsEmitted = DSL.field("records_emitted", SQLDataType.BIGINT.nullable(true))
  val bytesEmitted = DSL.field("bytes_emitted", SQLDataType.BIGINT.nullable(true))
  val estimatedRecords = DSL.field("estimated_records", SQLDataType.BIGINT.nullable(true))
  val estimatedBytes = DSL.field("estimated_bytes", SQLDataType.BIGINT.nullable(true))

  // Time Columns
  val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
  val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

  ctx
    .createTableIfNotExists("stream_stats")
    .columns(
      id,
      attemptId,
      streamNamespace,
      streamName,
      recordsEmitted,
      bytesEmitted,
      estimatedRecords,
      estimatedBytes,
      createdAt,
      updatedAt,
    ).constraints(
      DSL.primaryKey(id),
      DSL
        .foreignKey(attemptId)
        .references("attempts", "id")
        .onDeleteCascade(), // Prevent duplicate stat records of the same stream and attempt.
      DSL.unique("attempt_id", "stream_name"),
    ).execute()

  // Create an index on attempt_id, since all read queries on this table as of this migration will be
  // WHERE clauses on the attempt id.
  ctx.createIndex("index").on("stream_stats", "attempt_id").execute()
}
