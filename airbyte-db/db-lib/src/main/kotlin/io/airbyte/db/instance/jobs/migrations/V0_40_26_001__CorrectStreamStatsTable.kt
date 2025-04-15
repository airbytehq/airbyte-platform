/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

/**
 * Fix stream stats migration.
 */
@Suppress("ktlint:standard:class-naming")
class V0_40_26_001__CorrectStreamStatsTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    // This actually needs to be bigint to match the id column on the attempts table.
    val streamStats = "stream_stats"
    ctx
      .alterTable(streamStats)
      .alter("attempt_id")
      .set(SQLDataType.BIGINT.nullable(false))
      .execute()

    // Not all streams provide a namespace.
    ctx
      .alterTable(streamStats)
      .alter("stream_namespace")
      .set(SQLDataType.VARCHAR.nullable(true))
      .execute()

    // The constraint should also take into account the stream namespace. Drop the constraint and recreate it.
    ctx.alterTable(streamStats).dropUnique("stream_stats_attempt_id_stream_name_key").execute()
    ctx
      .alterTable(streamStats)
      .add(DSL.constraint("uniq_stream_attempt").unique("attempt_id", "stream_name", "stream_namespace"))
      .execute()
  }
}
