/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context
import kotlin.jvm.javaClass
import kotlin.text.trimIndent
import kotlin.use

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V2_1_0_017__CreateDataWorkerUsageReservationTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    createTable(context)
    createIndexes(context)
  }

  private fun createTable(context: Context) {
    val createDataWorkerUsageReservationTableStatement =
      """
      CREATE TABLE data_worker_usage_reservation(
        job_id BIGINT PRIMARY KEY,
        organization_id UUID NOT NULL,
        workspace_id UUID NOT NULL,
        dataplane_group_id UUID NOT NULL,
        source_cpu_request REAL NOT NULL,
        destination_cpu_request REAL NOT NULL,
        orchestrator_cpu_request REAL NOT NULL,
        used_on_demand_capacity BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      """.trimIndent()

    context.connection.createStatement().use { statement ->
      statement.execute(createDataWorkerUsageReservationTableStatement)
    }
  }

  private fun createIndexes(context: Context) {
    val createOrganizationIdIndex =
      """
      CREATE INDEX data_worker_usage_reservation_organization_id_idx
      ON data_worker_usage_reservation(organization_id);
      """.trimIndent()

    context.connection.createStatement().use { statement ->
      statement.execute(createOrganizationIdIndex)
    }
  }
}
