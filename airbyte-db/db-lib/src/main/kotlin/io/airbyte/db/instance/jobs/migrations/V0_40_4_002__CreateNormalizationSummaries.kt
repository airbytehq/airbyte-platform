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
 * Create normalization summaries migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_4_002__CreateNormalizationSummaries : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    // Warning: please do not use any jOOQ generated code to write a migration.
    // As database schema changes, the generated jOOQ code can be deprecated. So
    // old migration may not compile if there is any generated code.
    val ctx = DSL.using(context.connection)
    createNormalizationSummariesTable(ctx)
  }
}

private fun createNormalizationSummariesTable(ctx: DSLContext) {
  val id = DSL.field("id", SQLDataType.UUID.nullable(false))
  val attemptId = DSL.field("attempt_id", SQLDataType.BIGINT.nullable(false))
  val startTime = DSL.field("start_time", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true))
  val endTime = DSL.field("end_time", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(true))
  val failures = DSL.field("failures", SQLDataType.JSONB.nullable(true))
  val createdAt = DSL.field("created_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))
  val updatedAt = DSL.field("updated_at", SQLDataType.TIMESTAMPWITHTIMEZONE.nullable(false).defaultValue(DSL.currentOffsetDateTime()))

  ctx
    .createTableIfNotExists("normalization_summaries")
    .columns(id, attemptId, startTime, endTime, failures, createdAt, updatedAt)
    .constraints(DSL.primaryKey(id), DSL.foreignKey(attemptId).references("attempts", "id").onDeleteCascade())
    .execute()

  ctx.createIndex("normalization_summary_attempt_id_idx").on("normalization_summaries", "attempt_id").execute()
}
