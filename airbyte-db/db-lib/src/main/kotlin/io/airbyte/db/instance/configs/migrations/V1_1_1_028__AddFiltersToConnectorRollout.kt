/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.airbyte.config.CustomerTier
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_1_1_028__AddFiltersToConnectorRollout : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    doMigration(DSL.using(context.connection))
  }

  companion object {
    private const val CONNECTOR_ROLLOUT = "connector_rollout"
    private const val FILTERS = "filters"

    @JvmStatic
    @VisibleForTesting
    fun doMigration(ctx: DSLContext) {
      addFiltersToConnectorRollout(ctx)
      backfillFilters(ctx)
    }

    @JvmStatic
    fun addFiltersToConnectorRollout(ctx: DSLContext) {
      ctx
        .alterTable("connector_rollout")
        .addColumnIfNotExists(DSL.field(FILTERS, SQLDataType.JSONB))
        .execute()
    }

    @JvmStatic
    fun backfillFilters(ctx: DSLContext) {
      val json =
        """
        {
          "organization_customer_attributes": [
            {
              "name": "tier",
              "operator": "in",
              "values": ["${CustomerTier.TIER_2}"]
            }
          ]
        }
        """.trimIndent()

      ctx
        .update(DSL.table(CONNECTOR_ROLLOUT))
        .set(DSL.field(FILTERS), DSL.inline(json, SQLDataType.JSONB))
        .where(DSL.field(FILTERS).isNull)
        .execute()
    }
  }
}
