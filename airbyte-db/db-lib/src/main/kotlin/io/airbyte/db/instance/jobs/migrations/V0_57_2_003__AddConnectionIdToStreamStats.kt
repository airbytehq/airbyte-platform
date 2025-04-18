/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

@file:Suppress("ktlint:standard:filename")
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

@Suppress("ktlint:standard:class-naming")
class V0_57_2_003__AddConnectionIdToStreamStats : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    addConnectionIdColumn(ctx)
    log.info { "Migration finished!" }
  }
}

private val STREAM_STATS = DSL.table("stream_stats")
private val CONNECTION_ID = DSL.field("connection_id", SQLDataType.UUID)

fun addConnectionIdColumn(ctx: DSLContext) {
  log.info { "Adding connection_id column to stream_stats table" }

  ctx
    .alterTable(STREAM_STATS)
    .addColumnIfNotExists(CONNECTION_ID)
    .execute()
}
