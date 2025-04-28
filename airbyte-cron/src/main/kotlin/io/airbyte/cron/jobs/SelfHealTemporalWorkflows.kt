/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs

import datadog.trace.api.Trace
import io.airbyte.commons.temporal.TemporalClient
import io.airbyte.cron.SCHEDULED_TRACE_OPERATION_NAME
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.scheduling.annotation.Scheduled
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import jakarta.inject.Singleton

private val log = KotlinLogging.logger {}

@Singleton
class SelfHealTemporalWorkflows(
  private val temporalClient: TemporalClient,
  private val metricClient: MetricClient,
) {
  init {
    log.debug { "Creating temporal self-healing" }
  }

  @Trace(operationName = SCHEDULED_TRACE_OPERATION_NAME)
  @Scheduled(fixedRate = "10s")
  fun cleanTemporal() {
    metricClient.count(
      metric = OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE,
      attributes = arrayOf(MetricAttribute(MetricTags.CRON_TYPE, "self_heal_temporal")),
    )

    temporalClient
      .restartClosedWorkflowByStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED)
      .also { metricClient.count(metric = OssMetricsRegistry.WORKFLOWS_HEALED, value = it.toLong()) }
  }
}
