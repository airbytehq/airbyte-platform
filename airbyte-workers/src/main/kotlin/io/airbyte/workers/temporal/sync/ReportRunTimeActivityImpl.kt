/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.workers.temporal.sync

import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workers.temporal.activities.ReportRunTimeActivityInput
import jakarta.inject.Singleton

@Singleton
class ReportRunTimeActivityImpl(private val metricClient: MetricClient) : ReportRunTimeActivity {
  override fun reportRunTime(input: ReportRunTimeActivityInput) {
    val runTimeRefresh = input.refreshSchemaEndTime - input.startTime
    val runTimeReplication = input.replicationEndTime - input.refreshSchemaEndTime
    val totalWorkflowRunTime = input.replicationEndTime - input.startTime

    val connectionTag = MetricAttribute(MetricTags.CONNECTION_ID, input.connectionId.toString())
    val sourceDefinitionTag = MetricAttribute(MetricTags.SOURCE_DEFINITION_ID, input.sourceDefinitionId.toString())

    if (input.shouldRefreshSchema) {
      metricClient.count(
        OssMetricsRegistry.DISCOVER_CATALOG_RUN_TIME,
        runTimeRefresh,
        connectionTag,
        sourceDefinitionTag,
      )
    }
    metricClient.count(OssMetricsRegistry.REPLICATION_RUN_TIME, runTimeReplication, connectionTag, sourceDefinitionTag)
    metricClient.count(OssMetricsRegistry.SYNC_TOTAL_TIME, totalWorkflowRunTime, connectionTag, sourceDefinitionTag)
  }
}
