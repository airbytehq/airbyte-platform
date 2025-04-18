/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@Suppress("ktlint:standard:class-naming")
class V1_1_1_027__AddOriginIndexToScopedConfiguration : BaseJavaMigration() {
  @Throws(Exception::class)
  override fun migrate(context: Context) {
    LOGGER.info(
      "Running migration: {}",
      javaClass.simpleName,
    )

    val ctx = DSL.using(context.connection)

    ctx
      .createIndexIfNotExists(ORIGIN_INDEX_NAME)
      .on(DSL.table(SCOPED_CONFIGURATION_TABLE_NAME), DSL.field("origin"))
      .execute()
  }

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(V1_1_1_027__AddOriginIndexToScopedConfiguration::class.java)
    private const val SCOPED_CONFIGURATION_TABLE_NAME = "scoped_configuration"
    private const val ORIGIN_INDEX_NAME = "scoped_configuration_origin_idx"
  }
}
