/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_1_1_030__BackfillFiltersUpdate : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    doMigration(ctx)
  }

  fun doMigration(ctx: DSLContext) {
    rebackfillCorrectFilters(ctx)
  }

  private fun rebackfillCorrectFilters(ctx: DSLContext) {
    log.info { "Backfilling connector_rollout.filters with corrected format..." }

    val correctedJson =
      """
      {
        "customerTierFilters": [
          {
            "name": "TIER",
            "value": ["TIER_2"],
            "operator": "IN"
          }
        ]
      }
      """.trimIndent()

    ctx
      .update(DSL.table("connector_rollout"))
      .set(DSL.field("filters"), DSL.inline(correctedJson, SQLDataType.JSONB))
      .execute()
  }
}
