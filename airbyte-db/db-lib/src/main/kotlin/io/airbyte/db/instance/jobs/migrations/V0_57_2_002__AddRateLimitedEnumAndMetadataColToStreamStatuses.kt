/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.jobs.migrations

import io.airbyte.commons.annotation.InternalForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_57_2_002__AddRateLimitedEnumAndMetadataColToStreamStatuses : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    val ctx = DSL.using(context.connection)
    migrate(ctx)
  }

  companion object {
    @JvmStatic
    @InternalForTesting
    fun migrate(ctx: DSLContext) {
      ctx
        .alterTable("stream_statuses")
        .addColumnIfNotExists(DSL.field("metadata", SQLDataType.JSONB.nullable(true)))
        .execute()

      ctx.alterType("job_stream_status_run_state").addValue("rate_limited").execute()
    }
  }
}
