/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cron.jobs;

import static io.airbyte.cron.MicronautCronRunner.SCHEDULED_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.commons.temporal.TemporalClient;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.micronaut.scheduling.annotation.Scheduled;
import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Temporal cleaner. Resets failed workflow executions.
 */
@Singleton
public class SelfHealTemporalWorkflows {

  private static final Logger log = LoggerFactory.getLogger(SelfHealTemporalWorkflows.class);

  private final TemporalClient temporalClient;
  private final MetricClient metricClient;

  public SelfHealTemporalWorkflows(final TemporalClient temporalClient, final MetricClient metricClient) {
    log.debug("Creating temporal self-healing");
    this.temporalClient = temporalClient;
    this.metricClient = metricClient;
  }

  @Trace(operationName = SCHEDULED_TRACE_OPERATION_NAME)
  @Scheduled(fixedRate = "10s")
  void cleanTemporal() {
    metricClient.count(OssMetricsRegistry.CRON_JOB_RUN_BY_CRON_TYPE, 1, new MetricAttribute(MetricTags.CRON_TYPE, "self_heal_temporal"));
    final var numRestarted = temporalClient.restartClosedWorkflowByStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED);
    metricClient.count(OssMetricsRegistry.WORKFLOWS_HEALED, numRestarted);
  }

}
