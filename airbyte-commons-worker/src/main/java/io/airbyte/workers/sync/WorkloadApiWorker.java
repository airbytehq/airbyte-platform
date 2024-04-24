/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.sync;

import static io.airbyte.config.helpers.LogClientSingleton.fullLogPath;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.WorkloadApiClient;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.Geography;
import io.airbyte.commons.json.Jsons;
import io.airbyte.config.ReplicationOutput;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.Destination;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.Source;
import io.airbyte.featureflag.WorkloadPollingInterval;
import io.airbyte.featureflag.Workspace;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.Worker;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.internal.exception.DestinationException;
import io.airbyte.workers.internal.exception.SourceException;
import io.airbyte.workers.models.ReplicationActivityInput;
import io.airbyte.workers.process.Metadata;
import io.airbyte.workers.workload.JobOutputDocStore;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workers.workload.exception.DocStoreAccessException;
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
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.function.Function;
import org.openapitools.client.infrastructure.ServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker implementation that uses workload API instead of starting kube pods directly.
 */
public class WorkloadApiWorker implements Worker<ReplicationInput, ReplicationOutput> {

  private static final int HTTP_CONFLICT_CODE = HttpStatus.CONFLICT.getCode();
  private static final String DESTINATION = "destination";
  private static final String SOURCE = "source";

  private static final Logger log = LoggerFactory.getLogger(WorkloadApiWorker.class);
  private static final Set<WorkloadStatus> TERMINAL_STATUSES = Set.of(WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS);
  private final JobOutputDocStore jobOutputDocStore;
  private final AirbyteApiClient apiClient;
  private final WorkloadApiClient workloadApiClient;
  private final WorkloadClient workloadClient;
  private final WorkloadIdGenerator workloadIdGenerator;
  private final ReplicationActivityInput input;
  private final FeatureFlagClient featureFlagClient;

  private String workloadId = null;

  public WorkloadApiWorker(final JobOutputDocStore jobOutputDocStore,
                           final AirbyteApiClient apiClient,
                           final WorkloadApiClient workloadApiClient,
                           final WorkloadClient workloadClient,
                           final WorkloadIdGenerator workloadIdGenerator,
                           final ReplicationActivityInput input,
                           final FeatureFlagClient featureFlagClient) {
    this.jobOutputDocStore = jobOutputDocStore;
    this.apiClient = apiClient;
    this.workloadApiClient = workloadApiClient;
    this.workloadClient = workloadClient;
    this.workloadIdGenerator = workloadIdGenerator;
    this.input = input;
    this.featureFlagClient = featureFlagClient;
  }

  @Override
  @SuppressWarnings("PMD.AssignmentInOperand")
  public ReplicationOutput run(final ReplicationInput replicationInput, final Path jobRoot) throws WorkerException {
    final String serializedInput = Jsons.serialize(input);
    workloadId = workloadIdGenerator.generateSyncWorkloadId(replicationInput.getConnectionId(),
        Long.parseLong(replicationInput.getJobRunConfig().getJobId()),
        replicationInput.getJobRunConfig().getAttemptId().intValue());

    log.info("Creating workload {}", workloadId);

    // Ideally, this should be passed down to avoid the extra API call.
    final Geography geo = getGeography(replicationInput.getConnectionId());

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
        fullLogPath(jobRoot),
        geo.getValue(),
        WorkloadType.SYNC,
        WorkloadPriority.DEFAULT,
        replicationInput.getConnectionId().toString(),
        null);

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
          log.info("Workload {} is {}", workloadId, workload.getStatus());
        }
        i++;
      }
      sleep(sleepInterval.toMillis());
    }

    if (workload.getStatus() == WorkloadStatus.CANCELLED) {
      throw new CancellationException("Replication cancelled by " + workload.getTerminationSource());
    }

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
      } else {
        throw new WorkerException(workload.getTerminationReason(), e);
      }
    }
  }

  @Override
  public void cancel() {
    try {
      if (workloadId != null) {
        workloadApiClient.getWorkloadApi().workloadCancel(new WorkloadCancelRequest(workloadId, "user requested", "WorkloadApiWorker"));
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Geography getGeography(final UUID connectionId) throws WorkerException {
    try {
      return apiClient.getConnectionApi().getConnection(new ConnectionIdRequestBody().connectionId(connectionId)).getGeography();
    } catch (final ApiException e) {
      throw new WorkerException("Unable to find geography of connection " + connectionId, e);
    }
  }

  private ReplicationOutput getReplicationOutput(final String workloadId) {
    final Optional<ReplicationOutput> output;

    output = fetchReplicationOutput(workloadId, (location) -> {
      try {
        return jobOutputDocStore.readSyncOutput(location);
      } catch (final DocStoreAccessException e) {
        throw new RuntimeException(e);
      }
    });

    log.info("Replication output for workload {} : {}", workloadId, output.orElse(null));
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
    try {
      return workloadApiClient.getWorkloadApi().workloadGet(workloadId);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
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
