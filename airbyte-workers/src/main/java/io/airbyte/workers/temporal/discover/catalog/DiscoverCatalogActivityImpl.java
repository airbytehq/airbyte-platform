/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.discover.catalog;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;

import com.google.common.annotations.VisibleForTesting;
import datadog.trace.api.Trace;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.WorkloadCheckFrequencyInSeconds;
import io.airbyte.featureflag.Workspace;
import io.airbyte.workers.commands.DiscoverCommand;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.models.DiscoverCatalogInput;
import io.airbyte.workers.sync.WorkloadClient;
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import jakarta.inject.Singleton;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * DiscoverCatalogActivityImpl.
 */
@Singleton
@Slf4j
@SuppressWarnings("PMD.ExceptionAsFlowControl")
public class DiscoverCatalogActivityImpl implements DiscoverCatalogActivity {

  private final FeatureFlagClient featureFlagClient;
  private final WorkloadClient workloadClient;
  private final DiscoverCommand discoverCommand;

  public DiscoverCatalogActivityImpl(
                                     final FeatureFlagClient featureFlagClient,
                                     final WorkloadClient workloadClient,
                                     final DiscoverCommand discoverCommand) {
    this.featureFlagClient = featureFlagClient;
    this.workloadClient = workloadClient;
    this.discoverCommand = discoverCommand;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput runWithWorkload(final DiscoverCatalogInput input) throws WorkerException {
    final UUID workspaceId = input.getDiscoverCatalogInput().getActorContext().getWorkspaceId();
    final WorkloadCreateRequest workloadCreateRequest = discoverCommand.buildWorkloadCreateRequest(input, null);

    final int checkFrequencyInSeconds =
        featureFlagClient.intVariation(WorkloadCheckFrequencyInSeconds.INSTANCE, new Workspace(workspaceId));

    final ActivityExecutionContext context = getActivityContext();

    workloadClient.runWorkloadWithCancellationHeartbeat(workloadCreateRequest, checkFrequencyInSeconds, context);

    return discoverCommand.getOutput(workloadCreateRequest.getWorkloadId());
  }

  @VisibleForTesting
  ActivityExecutionContext getActivityContext() {
    return Activity.getExecutionContext();
  }

}
