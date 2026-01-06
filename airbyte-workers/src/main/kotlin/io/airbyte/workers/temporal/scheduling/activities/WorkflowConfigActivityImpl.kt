/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.commons.micronaut.EnvConstants
import io.airbyte.micronaut.runtime.AirbyteWorkflowConfig
import io.micronaut.context.annotation.Requires
import io.opentelemetry.instrumentation.annotations.WithSpan
import jakarta.inject.Singleton
import java.time.Duration

/**
 * Implementation of the [WorkflowConfigActivity] that is managed by the application framework
 * and therefore has access to the configuration loaded by the framework.
 */
@Singleton
@Requires(env = [EnvConstants.CONTROL_PLANE])
class WorkflowConfigActivityImpl(
  private val airbyteWorkflowConfig: AirbyteWorkflowConfig,
) : WorkflowConfigActivity {
  @WithSpan
  override fun getWorkflowRestartDelaySeconds(): Duration = Duration.ofSeconds(airbyteWorkflowConfig.failure.restartDelay)
}
