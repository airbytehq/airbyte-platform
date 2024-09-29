/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.discover.catalog;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;

import com.google.common.annotations.VisibleForTesting;
import datadog.trace.api.Trace;
import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.Geography;
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.logging.LogClientManager;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.ConnectorJobOutput.OutputType;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.WorkloadCheckFrequencyInSeconds;
import io.airbyte.featureflag.Workspace;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.models.DiscoverCatalogInput;
import io.airbyte.workers.pod.Metadata;
import io.airbyte.workers.sync.WorkloadClient;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadLabel;
import io.airbyte.workload.api.client.model.generated.WorkloadPriority;
import io.airbyte.workload.api.client.model.generated.WorkloadType;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * DiscoverCatalogActivityImpl.
 */
@Singleton
@Slf4j
@SuppressWarnings("PMD.ExceptionAsFlowControl")
public class DiscoverCatalogActivityImpl implements DiscoverCatalogActivity {

  static final long DISCOVER_CATALOG_SNAP_DURATION = Duration.ofMinutes(15).toMillis();

  private final Path workspaceRoot;
  private final AirbyteApiClient airbyteApiClient;
  private final FeatureFlagClient featureFlagClient;
  private final WorkloadClient workloadClient;
  private final WorkloadIdGenerator workloadIdGenerator;
  private final LogClientManager logClientManager;

  public DiscoverCatalogActivityImpl(
                                     @Named("workspaceRoot") final Path workspaceRoot,
                                     final AirbyteApiClient airbyteApiClient,
                                     final FeatureFlagClient featureFlagClient,
                                     final WorkloadClient workloadClient,
                                     final WorkloadIdGenerator workloadIdGenerator,
                                     final LogClientManager logClientManager) {
    this.workspaceRoot = workspaceRoot;
    this.airbyteApiClient = airbyteApiClient;
    this.featureFlagClient = featureFlagClient;
    this.workloadClient = workloadClient;
    this.workloadIdGenerator = workloadIdGenerator;
    this.logClientManager = logClientManager;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput runWithWorkload(final DiscoverCatalogInput input) throws WorkerException {
    final String jobId = input.getJobRunConfig().getJobId();
    final int attemptNumber = input.getJobRunConfig().getAttemptId() == null ? 0 : Math.toIntExact(input.getJobRunConfig().getAttemptId());
    final String workloadId = input.getDiscoverCatalogInput().getManual()
        ? workloadIdGenerator.generateDiscoverWorkloadId(input.getDiscoverCatalogInput().getActorContext().getActorDefinitionId(), jobId,
            attemptNumber)
        : workloadIdGenerator.generateDiscoverWorkloadIdV2WithSnap(
            input.getDiscoverCatalogInput().getActorContext().getActorId(),
            System.currentTimeMillis(),
            DISCOVER_CATALOG_SNAP_DURATION);

    final String serializedInput = Jsons.serialize(input);

    final UUID workspaceId = input.getDiscoverCatalogInput().getActorContext().getWorkspaceId();
    final Geography geo = getGeography(Optional.ofNullable(input.getLauncherConfig().getConnectionId()),
        Optional.ofNullable(workspaceId));

    final WorkloadCreateRequest workloadCreateRequest = new WorkloadCreateRequest(
        workloadId,
        List.of(new WorkloadLabel(Metadata.JOB_LABEL_KEY, jobId),
            new WorkloadLabel(Metadata.ATTEMPT_LABEL_KEY, String.valueOf(attemptNumber)),
            new WorkloadLabel(Metadata.WORKSPACE_LABEL_KEY, workspaceId.toString()),
            new WorkloadLabel(Metadata.ACTOR_TYPE, String.valueOf(ActorType.SOURCE.toString()))),
        serializedInput,
        logClientManager.fullLogPath(TemporalUtils.getJobRoot(workspaceRoot, jobId, attemptNumber)),
        geo.getValue(),
        WorkloadType.DISCOVER,
        Objects.requireNonNull(WorkloadPriority.Companion.decode(input.getLauncherConfig().getPriority().toString())),
        null,
        null,
        null);

    final int checkFrequencyInSeconds =
        featureFlagClient.intVariation(WorkloadCheckFrequencyInSeconds.INSTANCE, new Workspace(workspaceId));

    final ActivityExecutionContext context = getActivityContext();

    workloadClient.runWorkloadWithCancellationHeartbeat(workloadCreateRequest, checkFrequencyInSeconds, context);

    return workloadClient.getConnectorJobOutput(
        workloadId,
        failureReason -> new ConnectorJobOutput()
            .withOutputType(OutputType.DISCOVER_CATALOG_ID)
            .withDiscoverCatalogId(null)
            .withFailureReason(failureReason));
  }

  @VisibleForTesting
  ActivityExecutionContext getActivityContext() {
    return Activity.getExecutionContext();
  }

  @VisibleForTesting
  Geography getGeography(final Optional<UUID> maybeConnectionId, final Optional<UUID> maybeWorkspaceId) throws WorkerException {
    try {
      return maybeConnectionId
          .map(connectionId -> {
            try {
              return airbyteApiClient.getConnectionApi().getConnection(new ConnectionIdRequestBody(connectionId)).getGeography();
            } catch (final IOException e) {
              throw new RuntimeException(e);
            }
          }).orElse(
              maybeWorkspaceId.map(
                  workspaceId -> {
                    try {
                      return airbyteApiClient.getWorkspaceApi().getWorkspace(new WorkspaceIdRequestBody(workspaceId, false))
                          .getDefaultGeography();
                    } catch (final IOException e) {
                      throw new RuntimeException(e);
                    }
                  })
                  .orElse(Geography.AUTO));
    } catch (final Exception e) {
      throw new WorkerException("Unable to find geography of connection " + maybeConnectionId, e);
    }
  }

}
