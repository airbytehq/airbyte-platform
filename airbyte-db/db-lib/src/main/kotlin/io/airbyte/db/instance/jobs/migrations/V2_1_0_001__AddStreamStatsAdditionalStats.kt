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
class V2_1_0_001__AddStreamStatsAdditionalStats : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx: DSLContext = DSL.using(context.connection)

    addAdditionalStatsColumn(ctx)
  }

  companion object {
    fun addAdditionalStatsColumn(ctx: DSLContext) {
      ctx
        .alterTable("stream_stats")
        .addColumn("additional_stats", SQLDataType.JSONB.nullable(true))
        .execute()
    }
  }
}
