/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.sync;

import static io.airbyte.config.helpers.LogClientSingleton.fullLogPath;

import io.airbyte.api.client.AirbyteApiClient;
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
import io.airbyte.featureflag.WorkloadHeartbeatRate;
import io.airbyte.featureflag.WorkloadPollingInterval;
import io.airbyte.featureflag.Workspace;
import io.airbyte.persistence.job.models.ReplicationInput;
import io.airbyte.workers.Worker;
import io.airbyte.workers.exception.WorkerException;
import io.airbyte.workers.internal.exception.DestinationException;
import io.airbyte.workers.internal.exception.SourceException;
import io.airbyte.workers.models.ReplicationActivityInput;
import io.airbyte.workers.orchestrator.OrchestratorNameGenerator;
import io.airbyte.workers.process.Metadata;
import io.airbyte.workers.storage.DocumentStoreClient;
import io.airbyte.workers.workload.JobOutputDocStore;
import io.airbyte.workers.workload.WorkloadIdGenerator;
import io.airbyte.workers.workload.exception.DocStoreAccessException;
import io.airbyte.workload.api.client.generated.WorkloadApi;
import io.airbyte.workload.api.client.model.generated.Workload;
import io.airbyte.workload.api.client.model.generated.WorkloadCancelRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadCreateRequest;
import io.airbyte.workload.api.client.model.generated.WorkloadLabel;
import io.airbyte.workload.api.client.model.generated.WorkloadStatus;
import io.airbyte.workload.api.client.model.generated.WorkloadType;
import io.micronaut.http.HttpStatus;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.function.Function;
import org.openapitools.client.infrastructure.ClientException;
import org.openapitools.client.infrastructure.ServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker implementation that uses workload API instead of starting kube pods directly.
 */
public class WorkloadApiWorker implements Worker<ReplicationInput, ReplicationOutput> {

  private static final int HTTP_CONFLICT_CODE = 409;
  private static final String DESTINATION = "destination";
  private static final String SOURCE = "source";

  private static final Logger log = LoggerFactory.getLogger(WorkloadApiWorker.class);
  private static final Set<WorkloadStatus> TERMINAL_STATUSES = Set.of(WorkloadStatus.CANCELLED, WorkloadStatus.FAILURE, WorkloadStatus.SUCCESS);
  private final DocumentStoreClient documentStoreClient;
  private final OrchestratorNameGenerator orchestratorNameGenerator;
  private final JobOutputDocStore jobOutputDocStore;
  private final AirbyteApiClient apiClient;
  private final WorkloadApi workloadApi;
  private final WorkloadIdGenerator workloadIdGenerator;
  private final ReplicationActivityInput input;
  private final FeatureFlagClient featureFlagClient;
  private final boolean useOutputDocStore;

  private String workloadId = null;

  public WorkloadApiWorker(final DocumentStoreClient documentStoreClient,
                           final OrchestratorNameGenerator orchestratorNameGenerator,
                           final JobOutputDocStore jobOutputDocStore,
                           final AirbyteApiClient apiClient,
                           final WorkloadApi workloadApi,
                           final WorkloadIdGenerator workloadIdGenerator,
                           final ReplicationActivityInput input,
                           final FeatureFlagClient featureFlagClient,
                           final boolean useOutputDocStore) {
    this.documentStoreClient = documentStoreClient;
    this.orchestratorNameGenerator = orchestratorNameGenerator;
    this.jobOutputDocStore = jobOutputDocStore;
    this.apiClient = apiClient;
    this.workloadApi = workloadApi;
    this.workloadIdGenerator = workloadIdGenerator;
    this.input = input;
    this.featureFlagClient = featureFlagClient;
    this.useOutputDocStore = useOutputDocStore;
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

    // Create the workload
    try {
      createWorkload(new WorkloadCreateRequest(
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
          replicationInput.getConnectionId().toString(),
          WorkloadType.SYNC));
    } catch (final ServerException e) {
      if (e.getStatusCode() != HTTP_CONFLICT_CODE) {
        throw e;
      } else {
        log.info("Workload {} has already been created, reconnecting...", workloadId);
      }
    }

    // Wait until workload reaches a terminal status
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
        workloadApi.workloadCancel(new WorkloadCancelRequest(workloadId, "user requested", "WorkloadApiWorker"));
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

  private ReplicationOutput getReplicationOutput(final String workloadId) throws DocStoreAccessException {
    final String outputLocation = orchestratorNameGenerator.getOrchestratorOutputLocation(input.getJobRunConfig().getJobId(),
        input.getJobRunConfig().getAttemptId());

    final Optional<ReplicationOutput> output;
    if (useOutputDocStore) {
      output = fetchReplicationOutput(workloadId, (location) -> {
        try {
          return jobOutputDocStore.readSyncOutput(location);
        } catch (final DocStoreAccessException e) {
          throw new RuntimeException(e);
        }
      });
    } else {
      output = fetchReplicationOutput(outputLocation,
          (location) -> documentStoreClient.read(outputLocation).map(s -> Jsons.deserialize(s, ReplicationOutput.class)));
    }

    log.info("Replication output for workload {} : {}", workloadId, output.orElse(null));
    return output.orElse(null);
  }

  private Optional<ReplicationOutput> fetchReplicationOutput(final String location,
                                                             final Function<String, Optional<ReplicationOutput>> replicationFetcher) {
    final Context context = getFeatureFlagContext();
    final int workloadHeartbeatRate = featureFlagClient.intVariation(WorkloadHeartbeatRate.INSTANCE, context);
    final Instant cutoffTime = Instant.now().plus(workloadHeartbeatRate, ChronoUnit.SECONDS);
    do {
      final Optional<ReplicationOutput> output = replicationFetcher.apply(location);
      if (output.isPresent()) {
        return output;
      }
    } while (Instant.now().isBefore(cutoffTime));
    return Optional.empty();
  }

  private Context getFeatureFlagContext() {
    return new Multi(List.of(
        new Workspace(input.getWorkspaceId()),
        new Connection(input.getConnectionId()),
        new Source(input.getSourceId()),
        new Destination(input.getDestinationId())));
  }

  private void createWorkload(final WorkloadCreateRequest workloadCreateRequest) {
    try {
      workloadApi.workloadCreate(workloadCreateRequest);
    } catch (final ClientException e) {
      /*
       * The Workload API returns a 304 response when the request to execute the workload has already been
       * created. That response is handled in the form of a ClientException by the generated OpenAPI
       * client. We do not want to cause the Temporal workflow to retry, so catch it and log the
       * information so that the workflow will continue.
       */
      if (e.getStatusCode() == HttpStatus.CONFLICT.getCode()) {
        log.warn("Workload {} already created and in progress.  Continuing...", workloadCreateRequest.getWorkloadId());
      } else {
        throw new RuntimeException(e);
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Workload getWorkload(final String workloadId) {
    try {
      return workloadApi.workloadGet(workloadId);
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
