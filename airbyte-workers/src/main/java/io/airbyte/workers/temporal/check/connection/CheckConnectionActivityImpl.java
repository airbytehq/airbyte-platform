/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.check.connection;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.WorkloadCheckFrequencyInSeconds;
import io.airbyte.featureflag.Workspace;
import io.airbyte.workers.commands.CheckCommand;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.models.CheckConnectionInput;
import io.airbyte.workers.sync.WorkloadClient;
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.activity.ActivityOptions;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.UUID;

/**
 * Check connection activity temporal implementation for the control plane.
 */
@Singleton
@SuppressWarnings("PMD.ExceptionAsFlowControl")
public class CheckConnectionActivityImpl implements CheckConnectionActivity {

  private final WorkloadClient workloadClient;
  private final FeatureFlagClient featureFlagClient;
  private final ActivityOptions activityOptions;
  private final CheckCommand checkCommand;

  public CheckConnectionActivityImpl(
                                     final FeatureFlagClient featureFlagClient,
                                     final WorkloadClient workloadClient,
                                     final CheckCommand checkCommand,
                                     @Named("checkActivityOptions") final ActivityOptions activityOptions) {
    this.workloadClient = workloadClient;
    this.featureFlagClient = featureFlagClient;
    this.checkCommand = checkCommand;
    this.activityOptions = activityOptions;
  }

  @Override
  public Duration getCheckConnectionTimeout() {
    return activityOptions.getStartToCloseTimeout();
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput runWithWorkload(final CheckConnectionInput input) throws WorkerException {
    final UUID workspaceId = input.getCheckConnectionInput().getActorContext().getWorkspaceId();
    final WorkloadCreateRequest workloadCreateRequest = checkCommand.buildWorkloadCreateRequest(input, null);

    final int checkFrequencyInSeconds =
        featureFlagClient.intVariation(WorkloadCheckFrequencyInSeconds.INSTANCE, new Workspace(workspaceId));

    final ActivityExecutionContext context = Activity.getExecutionContext();

    workloadClient.runWorkloadWithCancellationHeartbeat(workloadCreateRequest, checkFrequencyInSeconds, context);

    return checkCommand.getOutput(workloadCreateRequest.getWorkloadId());
  }

}
