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
 * Add Bytes Committed to SyncStats and StreamStats.
 */
@Suppress("ktlint:standard:class-naming")
class V0_42_0_001__AddBytesCommittedToStatsTables : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    updateSyncStats(ctx)
    updateStreamStats(ctx)
  }
}

private fun updateStreamStats(ctx: DSLContext) {
  val streamStats = "stream_stats"
  ctx
    .alterTable(streamStats)
    .addColumnIfNotExists(DSL.field("bytes_committed", SQLDataType.BIGINT.nullable(true)))
    .execute()
  ctx
    .alterTable(streamStats)
    .addColumnIfNotExists(DSL.field("records_committed", SQLDataType.BIGINT.nullable(true)))
    .execute()
}

private fun updateSyncStats(ctx: DSLContext) {
  val streamStats = "sync_stats"
  ctx
    .alterTable(streamStats)
    .addColumnIfNotExists(DSL.field("bytes_committed", SQLDataType.BIGINT.nullable(true)))
    .execute()
}
