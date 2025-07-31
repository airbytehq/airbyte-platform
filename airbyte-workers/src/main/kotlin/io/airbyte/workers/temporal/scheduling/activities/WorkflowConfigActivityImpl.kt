/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import datadog.trace.api.Trace
import io.airbyte.commons.micronaut.EnvConstants
import io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.time.Duration

/**
 * Implementation of the [WorkflowConfigActivity] that is managed by the application framework
 * and therefore has access to the configuration loaded by the framework.
 */
@Singleton
@Requires(env = [EnvConstants.CONTROL_PLANE])
class WorkflowConfigActivityImpl(
  @param:Property(
    name = "airbyte.workflow.failure.restart-delay",
    defaultValue = "600",
  ) private val workflowRestartDelaySeconds: Long,
) : WorkflowConfigActivity {
  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun getWorkflowRestartDelaySeconds(): Duration = Duration.ofSeconds(workflowRestartDelaySeconds)
}
