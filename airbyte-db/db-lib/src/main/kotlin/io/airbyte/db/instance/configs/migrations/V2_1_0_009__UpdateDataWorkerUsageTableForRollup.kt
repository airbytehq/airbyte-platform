/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.db.instance.configs.migrations

import io.github.oshai.kotlinlogging.KotlinLogging
import org.flywaydb.core.api.migration.BaseJavaMigration
import org.flywaydb.core.api.migration.Context

private val log = KotlinLogging.logger {}

@Suppress("ktlint:standard:class-naming")
class V2_1_0_009__UpdateDataWorkerUsageTableForRollup : BaseJavaMigration() {
  override fun migrate(context: Context) {
    log.info { "Running migration: ${javaClass.simpleName}" }

    alterTable(context)
  }

  private fun alterTable(context: Context) {
    val dropJobId =
      """
      ALTER TABLE data_worker_usage
      DROP COLUMN job_id;
      """.trimIndent()

    val updateJobStartColumnName =
      """
      ALTER TABLE data_worker_usage
      RENAME COLUMN job_start TO bucket_start;
      """.trimIndent()

    val dropJobEndColumn =
      """
      ALTER TABLE data_worker_usage
      DROP COLUMN job_end;
      """.trimIndent()

    val dropOrgIdBucketIndex =
      """
      DROP INDEX IF EXISTS data_worker_usage_organization_id_job_start_idx 
      """.trimIndent()

    val workspaceIdBucketStartDataplaneGroupIdPrimaryKey =
      """
      ALTER TABLE data_worker_usage
      ADD PRIMARY KEY (organization_id, bucket_start, workspace_id, dataplane_group_id);
      """.trimIndent()

    context.connection.createStatement().use { statement ->
      statement.execute(dropJobId)
      statement.execute(updateJobStartColumnName)
      statement.execute(dropJobEndColumn)
      statement.execute(dropOrgIdBucketIndex)
      statement.execute(workspaceIdBucketStartDataplaneGroupIdPrimaryKey)
    }
  }
}
