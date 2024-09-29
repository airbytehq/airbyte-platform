/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.check.connection;

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
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.ConnectorJobOutput.OutputType;
import io.airbyte.config.StandardCheckConnectionOutput;
import io.airbyte.config.StandardCheckConnectionOutput.Status;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.WorkloadCheckFrequencyInSeconds;
import io.airbyte.featureflag.Workspace;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.models.CheckConnectionInput;
import io.airbyte.workers.pod.Metadata;
import io.airbyte.workers.sync.WorkloadClient;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadLabel;
import io.airbyte.workload.api.client.model.generated.WorkloadPriority;
import io.airbyte.workload.api.client.model.generated.WorkloadType;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityExecutionContext;
import io.temporal.activity.ActivityOptions;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * Check connection activity temporal implementation for the control plane.
 */
@Singleton
@Slf4j
@SuppressWarnings("PMD.ExceptionAsFlowControl")
public class CheckConnectionActivityImpl implements CheckConnectionActivity {

  private final Path workspaceRoot;
  private final AirbyteApiClient airbyteApiClient;
  private final WorkloadClient workloadClient;
  private final WorkloadIdGenerator workloadIdGenerator;
  private final FeatureFlagClient featureFlagClient;
  private final MetricClient metricClient;
  private final ActivityOptions activityOptions;
  private final LogClientManager logClientManager;

  public CheckConnectionActivityImpl(
                                     @Named("workspaceRoot") final Path workspaceRoot,
                                     final AirbyteApiClient airbyteApiClient,
                                     final FeatureFlagClient featureFlagClient,
                                     final WorkloadClient workloadClient,
                                     final WorkloadIdGenerator workloadIdGenerator,
                                     final MetricClient metricClient,
                                     @Named("checkActivityOptions") final ActivityOptions activityOptions,
                                     final LogClientManager logClientManager) {
    this.workspaceRoot = workspaceRoot;
    this.airbyteApiClient = airbyteApiClient;
    this.workloadClient = workloadClient;
    this.workloadIdGenerator = workloadIdGenerator;
    this.featureFlagClient = featureFlagClient;
    this.metricClient = metricClient;
    this.activityOptions = activityOptions;
    this.logClientManager = logClientManager;
  }

  @Override
  public Duration getCheckConnectionTimeout() {
    return activityOptions.getStartToCloseTimeout();
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public ConnectorJobOutput runWithWorkload(final CheckConnectionInput input) throws WorkerException {
    final String jobId = input.getJobRunConfig().getJobId();
    final int attemptNumber = input.getJobRunConfig().getAttemptId() == null ? 0 : Math.toIntExact(input.getJobRunConfig().getAttemptId());
    final String workloadId =
        workloadIdGenerator.generateCheckWorkloadId(input.getCheckConnectionInput().getActorContext().getActorDefinitionId(), jobId,
            attemptNumber);
    final String serializedInput = Jsons.serialize(input);

    final UUID workspaceId = input.getCheckConnectionInput().getActorContext().getWorkspaceId();
    final Geography geo = getGeography(Optional.ofNullable(input.getLauncherConfig().getConnectionId()),
        Optional.ofNullable(workspaceId));

    final WorkloadCreateRequest workloadCreateRequest = new WorkloadCreateRequest(
        workloadId,
        List.of(new WorkloadLabel(Metadata.JOB_LABEL_KEY, jobId),
            new WorkloadLabel(Metadata.ATTEMPT_LABEL_KEY, String.valueOf(attemptNumber)),
            new WorkloadLabel(Metadata.WORKSPACE_LABEL_KEY, workspaceId.toString()),
            new WorkloadLabel(Metadata.ACTOR_TYPE, String.valueOf(input.getCheckConnectionInput().getActorType().toString()))),
        serializedInput,
        logClientManager.fullLogPath(TemporalUtils.getJobRoot(workspaceRoot, jobId, attemptNumber)),
        geo.getValue(),
        WorkloadType.CHECK,
        WorkloadPriority.Companion.decode(input.getLauncherConfig().getPriority().toString()),
        null,
        null,
        null);

    final int checkFrequencyInSeconds =
        featureFlagClient.intVariation(WorkloadCheckFrequencyInSeconds.INSTANCE, new Workspace(workspaceId));

    final ActivityExecutionContext context = Activity.getExecutionContext();

    workloadClient.runWorkloadWithCancellationHeartbeat(workloadCreateRequest, checkFrequencyInSeconds, context);

    final var output = workloadClient.getConnectorJobOutput(
        workloadId,
        failureReason -> new ConnectorJobOutput()
            .withOutputType(OutputType.CHECK_CONNECTION)
            .withCheckConnection(
                new StandardCheckConnectionOutput()
                    .withStatus(Status.FAILED)
                    .withMessage(failureReason.getExternalMessage()))
            .withFailureReason(failureReason));

    metricClient.count(
        OssMetricsRegistry.SIDECAR_CHECK,
        1,
        new MetricAttribute(MetricTags.STATUS, output.getCheckConnection().getStatus() == Status.FAILED ? "failed" : "success"));
    return output;
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
