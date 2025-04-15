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
 * Add sync stats table migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_3_001__CreateSyncStats : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    createSyncStatsTable(ctx)
  }
}

private fun createSyncStatsTable(ctx: DSLContext) {
  val id = DSL.field("id", SQLDataType.UUID.nullable(false))
  val attemptId = DSL.field("attempt_id", SQLDataType.INTEGER.nullable(false))
  val recordsEmitted = DSL.field("records_emitted", SQLDataType.BIGINT.nullable(true))
  val bytesEmitted = DSL.field("bytes_emitted", SQLDataType.BIGINT.nullable(true))
  val sourceStateMessagesEmitted = DSL.field("source_state_messages_emitted", SQLDataType.BIGINT.nullable(true))
  val destinationStateMessagesEmitted = DSL.field("destination_state_messages_emitted", SQLDataType.BIGINT.nullable(true))
  val recordsCommitted = DSL.field("records_committed", SQLDataType.BIGINT.nullable(true))
  val meanSecondsBeforeSourceStateMessageEmitted = DSL.field("mean_seconds_before_source_state_message_emitted", SQLDataType.BIGINT.nullable(true))
  val maxSecondsBeforeSourceStateMessageEmitted = DSL.field("max_seconds_before_source_state_message_emitted", SQLDataType.BIGINT.nullable(true))
  val meanSecondsBetweenStateMessageEmittedAndCommitted =
    DSL.field(
      "mean_seconds_between_state_message_emitted_and_committed",
      SQLDataType.BIGINT.nullable(true),
    )
  val maxSecondsBetweenStateMessageEmittedAndCommitted =
    DSL.field(
      "max_seconds_between_state_message_emitted_and_committed",
      SQLDataType.BIGINT.nullable(true),
    )
  val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
  val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

  ctx
    .createTableIfNotExists("sync_stats")
    .columns(
      id,
      attemptId,
      recordsEmitted,
      bytesEmitted,
      sourceStateMessagesEmitted,
      destinationStateMessagesEmitted,
      recordsCommitted,
      meanSecondsBeforeSourceStateMessageEmitted,
      maxSecondsBeforeSourceStateMessageEmitted,
      meanSecondsBetweenStateMessageEmittedAndCommitted,
      maxSecondsBetweenStateMessageEmittedAndCommitted,
      createdAt,
      updatedAt,
    ).constraints(
      DSL.primaryKey(id),
      DSL.foreignKey(attemptId).references("attempts", "id").onDeleteCascade(),
    ).execute()

  ctx.createIndex("attempt_id_idx").on("sync_stats", "attempt_id").execute()
}
