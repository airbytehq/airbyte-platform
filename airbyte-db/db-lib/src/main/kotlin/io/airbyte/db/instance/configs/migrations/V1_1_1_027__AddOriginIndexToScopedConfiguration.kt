/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL

@Suppress("ktlint:standard:class-naming")
class V1_1_1_027__AddOriginIndexToScopedConfiguration : BaseJavaMigration() {
  @Throws(Exception::class)
  override fun migrate(context: Context) {
    log.info(
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
    private val log = KotlinLogging.logger {}
    private const val SCOPED_CONFIGURATION_TABLE_NAME = "scoped_configuration"
    private const val ORIGIN_INDEX_NAME = "scoped_configuration_origin_idx"
  }
}
