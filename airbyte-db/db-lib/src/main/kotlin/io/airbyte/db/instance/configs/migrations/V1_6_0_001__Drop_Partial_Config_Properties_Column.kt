/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import com.google.common.annotations.VisibleForTesting
import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.DSLContext
import org.jooq.impl.DSL

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V1_6_0_001__Drop_Partial_Config_Properties_Column : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)
    dropConfigurationColumn(ctx)
  }

  companion object {
    private const val PARTIAL_USER_CONFIG_TABLE_NAME: String = "partial_user_config"
    private const val PARTIAL_USER_CONFIG_CONFIGURATION_COLUMN_NAME: String = "partial_user_config_properties"

    @VisibleForTesting
    fun dropConfigurationColumn(ctx: DSLContext) {
      ctx
        .alterTable(PARTIAL_USER_CONFIG_TABLE_NAME)
        .dropColumnIfExists(PARTIAL_USER_CONFIG_CONFIGURATION_COLUMN_NAME)
        .execute()
    }
  }
}
