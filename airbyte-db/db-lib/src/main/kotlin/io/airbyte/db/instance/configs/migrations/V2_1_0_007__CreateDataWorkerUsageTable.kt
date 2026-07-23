/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V2_1_0_007__CreateDataWorkerUsageTable : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    createTable(context)
    createIndexes(context)
  }

  private fun createTable(context: Context) {
    val createDataWorkerUsageTableStatement =
      """
      CREATE TABLE data_worker_usage(
        job_id BIGINT PRIMARY KEY,
        organization_id UUID NOT NULL,
        workspace_id UUID NOT NULL,
        dataplane_group_id UUID NOT NULL,
        source_cpu_request REAL NOT NULL,
        destination_cpu_request REAL NOT NULL,
        orchestrator_cpu_request REAL NOT NULL,
        job_start TIMESTAMPTZ NOT NULL,
        job_end TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      """.trimIndent()

    context.connection.createStatement().use { statement ->
      statement.execute(createDataWorkerUsageTableStatement)
    }
  }

  private fun createIndexes(context: Context) {
    val createOrganizationJobStartIndex =
      """
      CREATE INDEX data_worker_usage_organization_id_job_start_idx
      ON data_worker_usage(organization_id, job_start);
      """.trimIndent()

    val createJobIdIndex =
      """
      CREATE INDEX data_worker_usage_job_id_idx
      ON data_worker_usage(job_id);
      """.trimIndent()

    val jobEndIsNullIndex =
      """
      CREATE INDEX data_worker_usage_job_end_is_null_idx
      ON data_worker_usage(job_end)
      WHERE job_end IS NULL;
      """.trimIndent()

    context.connection.createStatement().use { statement ->
      statement.execute(createOrganizationJobStartIndex)
      statement.execute(createJobIdIndex)
      statement.execute(jobEndIsNullIndex)
    }
  }
}
