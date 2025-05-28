/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.sync;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.logging.LogClientManager;
import io.airbyte.config.ConnectionContext;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.Destination;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.Source;
import io.airbyte.featureflag.WorkloadHeartbeatTimeout;
import io.airbyte.featureflag.WorkloadPollingInterval;
import io.airbyte.featureflag.Workspace;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.exception.WorkloadLauncherException;
import io.airbyte.workers.exception.WorkloadMonitorException;
import io.airbyte.workers.internal.exception.DestinationException;
import io.airbyte.workers.internal.exception.SourceException;
import io.airbyte.workers.models.ReplicationActivityInput;
import io.airbyte.workers.pod.Metadata;
import io.airbyte.workers.workload.DataplaneGroupResolver;
import io.airbyte.workers.workload.WorkloadConstants;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workers.workload.WorkloadOutputWriter;
import io.airbyte.workers.workload.exception.DocStoreAccessException;
import io.airbyte.workload.api.client.WorkloadApiClient;
import io.airbyte.workload.api.client.model.generated.Workload;
import io.airbyte.workload.api.client.model.generated.WorkloadCancelRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadLabel;
import io.airbyte.workload.api.client.model.generated.WorkloadPriority;
import io.airbyte.workload.api.client.model.generated.WorkloadStatus;
import io.airbyte.workload.api.client.model.generated.WorkloadType;
import io.micronaut.http.HttpStatus;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.function.Function;
import org.openapitools.client.infrastructure.ServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker implementation that uses workload API instead of starting kube pods directly.
 */
public class WorkloadApiWorker {

  private static final int HTTP_CONFLICT_CODE = HttpStatus.CONFLICT.getCode();
  private static final String DESTINATION = "destination";
  private static final String SOURCE = "source";
  private static final String WORKLOAD_LAUNCHER = "workload-launcher";

  private static final Set<String> WORKLOAD_MONITOR = Set.of("workload-monitor-start", "workload-monitor-claim", "workload-monitor-heartbeat");

  private static final Logger log = LoggerFactory.getLogger(WorkloadApiWorker.class);
  private static final Set<WorkloadStatus> TERMINAL_STATUSES = Set.of(WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS);
  private final WorkloadOutputWriter workloadOutputWriter;
  private final WorkloadApiClient workloadApiClient;
  private final WorkloadClient workloadClient;
  private final WorkloadIdGenerator workloadIdGenerator;
  private final ReplicationActivityInput input;
  private final FeatureFlagClient featureFlagClient;
  private final LogClientManager logClientManager;
  private final DataplaneGroupResolver dataplaneGroupResolver;

  private String workloadId = null;

  public WorkloadApiWorker(final WorkloadOutputWriter workloadOutputWriter,
                           final WorkloadApiClient workloadApiClient,
                           final WorkloadClient workloadClient,
                           final WorkloadIdGenerator workloadIdGenerator,
                           final ReplicationActivityInput input,
                           final FeatureFlagClient featureFlagClient,
                           final LogClientManager logClientManager,
                           final DataplaneGroupResolver dataplaneGroupResolver) {
    this.workloadOutputWriter = workloadOutputWriter;
    this.workloadApiClient = workloadApiClient;
    this.workloadClient = workloadClient;
    this.workloadIdGenerator = workloadIdGenerator;
    this.input = input;
    this.featureFlagClient = featureFlagClient;
    this.logClientManager = logClientManager;
    this.dataplaneGroupResolver = dataplaneGroupResolver;
  }

  // TODO Migrate test before deleting to ensure we do not lose coverage
  @Deprecated
  public ReplicationOutput run(final ReplicationInput replicationInput, final Path jobRoot) throws WorkerException {
    final String workloadId = createWorkload(replicationInput, jobRoot);
    waitForWorkload(workloadId);
    return getOutput(workloadId);
  }

  public String createWorkload(final ReplicationInput replicationInput, final Path jobRoot) throws WorkerException {
    final String serializedInput = Jsons.serialize(input);
    workloadId = workloadIdGenerator.generateSyncWorkloadId(replicationInput.getConnectionId(),
        Long.parseLong(replicationInput.getJobRunConfig().getJobId()),
        replicationInput.getJobRunConfig().getAttemptId().intValue());

    final ConnectionContext context = replicationInput.getConnectionContext();
    final var organizationId = context != null ? context.getOrganizationId() : null;
    final var workspaceId = context != null ? context.getWorkspaceId() : replicationInput.getWorkspaceId();
    final var connectionId = context != null ? context.getConnectionId() : replicationInput.getConnectionId();

    final String dataplaneGroup = dataplaneGroupResolver.resolveForSync(
        organizationId,
        workspaceId,
        connectionId);

    log.info("Creating workload {}", workloadId);

    final WorkloadCreateRequest workloadCreateRequest = new WorkloadCreateRequest(
        workloadId,
        List.of(
            // This list copied from KubeProcess#getLabels() without docker image labels which we populate from
            // the launcher
            new WorkloadLabel(Metadata.CONNECTION_ID_LABEL_KEY, replicationInput.getConnectionId().toString()),
            new WorkloadLabel(Metadata.JOB_LABEL_KEY, replicationInput.getJobRunConfig().getJobId()),
            new WorkloadLabel(Metadata.ATTEMPT_LABEL_KEY, replicationInput.getJobRunConfig().getAttemptId().toString()),
            new WorkloadLabel(Metadata.WORKSPACE_LABEL_KEY, replicationInput.getWorkspaceId().toString()),
            new WorkloadLabel(Metadata.WORKER_POD_LABEL_KEY, Metadata.WORKER_POD_LABEL_VALUE)),
        serializedInput,
        logClientManager.fullLogPath(jobRoot),
        WorkloadType.SYNC,
        WorkloadPriority.DEFAULT,
        workspaceId,
        organizationId,
        replicationInput.getConnectionId().toString(),
        null,
        replicationInput.getSignalInput(),
        dataplaneGroup);

    // Create the workload
    try {
      workloadClient.createWorkload(workloadCreateRequest);
    } catch (final ServerException e) {
      if (e.getStatusCode() != HTTP_CONFLICT_CODE) {
        throw e;
      } else {
        log.info("Workload {} has already been created, reconnecting...", workloadId);
      }
    }
    return workloadId;
  }

  public boolean isWorkloadTerminal(final String workloadId) {
    // TODO handle error
    final Workload workload = getWorkload(workloadId);
    return workload.getStatus() != null && TERMINAL_STATUSES.contains(workload.getStatus());
  }

  public void waitForWorkload(final String workloadId) {
    // Wait until workload reaches a terminal status
    // TODO merge this with WorkloadApiHelper.waitForWorkload. The only difference currently is the
    // progress log.
    int i = 0;
    final Duration sleepInterval = Duration.ofSeconds(featureFlagClient.intVariation(WorkloadPollingInterval.INSTANCE, getFeatureFlagContext()));
    Workload workload;
    while (true) {
      workload = getWorkload(workloadId);

      if (workload.getStatus() != null) {
        if (TERMINAL_STATUSES.contains(workload.getStatus())) {
          log.info("Workload {} has returned a terminal status of {}.  Fetching output...", workloadId, workload.getStatus());
          break;
        }

        if (i % 5 == 0) {
          i++;
          // Since syncs are mostly in a running state this can spam logs
          // while providing no actionable information
          if (workload.getStatus() != WorkloadStatus.RUNNING) {
            log.info("Workload {} is {}", workloadId, workload.getStatus());
          }
        }
        i++;
      }
      sleep(sleepInterval.toMillis());
    }

    if (workload.getStatus() == WorkloadStatus.CANCELLED) {
      throw new CancellationException("Replication cancelled by " + workload.getTerminationSource());
    }
  }

  public void cancelWorkload(final String workloadId) throws IOException {
    callWithRetry(() -> {
      workloadApiClient.getWorkloadApi().workloadCancel(new WorkloadCancelRequest(
          workloadId, WorkloadConstants.WORKLOAD_CANCELLED_BY_USER_REASON, "WorkloadApiWorker"));
      return true;
    });
  }

  public ReplicationOutput getOutput(final String workloadId) throws WorkerException {
    final Workload workload = getWorkload(workloadId);
    final ReplicationOutput output;
    try {
      output = getReplicationOutput(workloadId);
    } catch (final Exception e) {
      throwFallbackError(workload, e);
      throw new WorkerException("Failed to read replication output", e);
    }
    if (output == null) {
      // If we fail to read the output, fallback to throwing an exception based on the status of the
      // workload
      throwFallbackError(workload, null);
      throw new WorkerException("Replication output is empty");
    }
    return output;
  }

  private void throwFallbackError(final Workload workload, final Exception e) throws WorkerException {
    if (workload.getStatus() == WorkloadStatus.FAILURE) {
      if (SOURCE.equals(workload.getTerminationSource())) {
        throw new SourceException(workload.getTerminationReason(), e);
      } else if (DESTINATION.equals(workload.getTerminationSource())) {
        throw new DestinationException(workload.getTerminationReason(), e);
      } else if (WORKLOAD_LAUNCHER.equals(workload.getTerminationSource())) {
        throw new WorkloadLauncherException(workload.getTerminationReason());
      } else if (workload.getTerminationSource() != null && WORKLOAD_MONITOR.contains(workload.getTerminationSource())) {
        throw new WorkloadMonitorException(workload.getTerminationReason());
      } else {
        throw new WorkerException(workload.getTerminationReason(), e);
      }
    }
  }

  public void cancel() {
    try {
      if (workloadId != null) {
        cancelWorkload(workloadId);
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private ReplicationOutput getReplicationOutput(final String workloadId) {
    final Optional<ReplicationOutput> output;

    output = fetchReplicationOutput(workloadId, (location) -> {
      try {
        return workloadOutputWriter.readSyncOutput(location);
      } catch (final DocStoreAccessException e) {
        throw new RuntimeException(e);
      }
    });

    log.debug("Replication output for workload {} : {}", workloadId, output.orElse(null));
    return output.orElse(null);
  }

  private Optional<ReplicationOutput> fetchReplicationOutput(final String location,
                                                             final Function<String, Optional<ReplicationOutput>> replicationFetcher) {
    return replicationFetcher.apply(location);
  }

  private Context getFeatureFlagContext() {
    return new Multi(List.of(
        new Workspace(input.getWorkspaceId()),
        new Connection(input.getConnectionId()),
        new Source(input.getSourceId()),
        new Destination(input.getDestinationId())));
  }

  private Workload getWorkload(final String workloadId) {
    return callWithRetry(() -> workloadApiClient.getWorkloadApi().workloadGet(workloadId));
  }

  /**
   * This method is aiming to mimic the behavior of the heartbeat which only fails after its timeout
   * is reach. This allows to be more resilient to a workloadApi downtime
   *
   * @param workloadApiCall A supplier calling the API
   * @return the result of the API call
   */
  private <T> T callWithRetry(CheckedSupplier<T> workloadApiCall) {
    Duration timeoutDuration = Duration.ofMinutes(featureFlagClient.intVariation(WorkloadHeartbeatTimeout.INSTANCE, getFeatureFlagContext()));
    return Failsafe.with(RetryPolicy.builder()
        .withDelay(Duration.ofSeconds(30))
        .withMaxDuration(timeoutDuration)
        .build()).get(workloadApiCall);
  }

  private void sleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

}
