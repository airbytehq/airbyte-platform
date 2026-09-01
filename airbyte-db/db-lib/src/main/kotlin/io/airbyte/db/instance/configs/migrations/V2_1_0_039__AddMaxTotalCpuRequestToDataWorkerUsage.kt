/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V2_1_0_039__AddMaxTotalCpuRequestToDataWorkerUsage : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    alterTable(context)
  }

  private fun alterTable(context: Context) {
    val addColumn =
      """
      ALTER TABLE data_worker_usage
      ADD COLUMN max_total_cpu_request REAL;
      """.trimIndent()

    context.connection.createStatement().use { statement ->
      statement.execute(addColumn)
    }
  }
}
