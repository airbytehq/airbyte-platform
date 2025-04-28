/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V0_50_33_013__AddTestingValuesColumnToConnectorBuilderProject : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }
    val ctx = DSL.using(context.connection)

    val testingValuesColumn = DSL.field(TESTING_VALUES_COLUMN_NAME, SQLDataType.JSONB.nullable(true))
    ctx
      .alterTable(CONNECTOR_BUILDER_PROJECT_TABLE)
      .addColumnIfNotExists(testingValuesColumn)
      .execute()
  }

  companion object {
    private const val CONNECTOR_BUILDER_PROJECT_TABLE = "connector_builder_project"
    private const val TESTING_VALUES_COLUMN_NAME = "testing_values"
  }
}
