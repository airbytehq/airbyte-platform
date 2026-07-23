/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V2_1_0_011__AddMaxResourceColumnsToDataWorkerUsage : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    alterTable(context)
  }

  private fun alterTable(context: Context) {
    val addColumns =
      """
      ALTER TABLE data_worker_usage
      ADD COLUMN max_source_cpu_request REAL NOT NULL DEFAULT 0.0,
      ADD COLUMN max_destination_cpu_request REAL NOT NULL DEFAULT 0.0,
      ADD COLUMN max_orchestrator_cpu_request REAL NOT NULL DEFAULT 0.0;
      """.trimIndent()

    context.connection.createStatement().use { statement ->
      statement.execute(addColumns)
    }
  }
}
