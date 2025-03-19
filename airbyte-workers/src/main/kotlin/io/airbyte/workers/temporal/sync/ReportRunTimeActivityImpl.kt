/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workers.temporal.activities.ReportRunTimeActivityInput
import jakarta.inject.Singleton

@Singleton
class ReportRunTimeActivityImpl(
  private val metricClient: MetricClient,
) : ReportRunTimeActivity {
  override fun reportRunTime(input: ReportRunTimeActivityInput) {
    val runTimeRefresh = input.refreshSchemaEndTime - input.startTime
    val runTimeReplication = input.replicationEndTime - input.refreshSchemaEndTime
    val totalWorkflowRunTime = input.replicationEndTime - input.startTime

    val connectionTag = MetricAttribute(MetricTags.CONNECTION_ID, input.connectionId.toString())
    val sourceDefinitionTag = MetricAttribute(MetricTags.SOURCE_DEFINITION_ID, input.sourceDefinitionId.toString())

    metricClient.count(
      metric = OssMetricsRegistry.DISCOVER_CATALOG_RUN_TIME,
      value = runTimeRefresh,
      attributes =
        arrayOf(
          connectionTag,
          sourceDefinitionTag,
        ),
    )
    metricClient.count(
      metric = OssMetricsRegistry.REPLICATION_RUN_TIME,
      value = runTimeReplication,
      attributes =
        arrayOf(
          connectionTag,
          sourceDefinitionTag,
        ),
    )
    metricClient.count(
      metric = OssMetricsRegistry.SYNC_TOTAL_TIME,
      value = totalWorkflowRunTime,
      attributes =
        arrayOf(
          connectionTag,
          sourceDefinitionTag,
        ),
    )
  }
}
