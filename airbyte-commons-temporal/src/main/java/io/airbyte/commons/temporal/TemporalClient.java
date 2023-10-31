/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import static io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow.NON_RUNNING_JOB_ID;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.airbyte.commons.temporal.exception.DeletedWorkflowException;
import io.airbyte.commons.temporal.exception.UnreachableWorkflowException;
import io.airbyte.commons.temporal.scheduling.CheckConnectionWorkflow;
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow;
import io.airbyte.commons.temporal.scheduling.DiscoverCatalogWorkflow;
import io.airbyte.commons.temporal.scheduling.SpecWorkflow;
import io.airbyte.commons.temporal.scheduling.state.WorkflowState;
import io.airbyte.config.ActorContext;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.JobCheckConnectionConfig;
import io.airbyte.config.JobDiscoverCatalogConfig;
import io.airbyte.config.JobGetSpecConfig;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardDiscoverCatalogInput;
import io.airbyte.config.persistence.StreamResetPersistence;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.protocol.models.StreamDescriptor;
import io.temporal.api.common.v1.WorkflowType;
import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.api.workflowservice.v1.ListClosedWorkflowExecutionsRequest;
import io.temporal.api.workflowservice.v1.ListClosedWorkflowExecutionsResponse;
import io.temporal.api.workflowservice.v1.ListOpenWorkflowExecutionsRequest;
import io.temporal.api.workflowservice.v1.ListOpenWorkflowExecutionsResponse;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;

/**
 * Airbyte's interface over temporal.
 */
@Slf4j
@Singleton
public class TemporalClient {

  /**
   * This is used to sleep between 2 temporal queries. The query is needed to ensure that the cancel
   * and start manual sync methods wait before returning. Since temporal signals are async, we need to
   * use the queries to make sure that we are in a state in which we want to continue with.
   */
  private static final int DELAY_BETWEEN_QUERY_MS = 10;

  private final Path workspaceRoot;
  private final WorkflowClientWrapped workflowClientWrapped;
  private final WorkflowServiceStubsWrapped serviceStubsWrapped;
  private final StreamResetPersistence streamResetPersistence;
  private final ConnectionManagerUtils connectionManagerUtils;
  private final NotificationClient notificationClient;
  private final StreamResetRecordsHelper streamResetRecordsHelper;
  private final MetricClient metricClient;

  public TemporalClient(@Named("workspaceRootTemporal") final Path workspaceRoot,
                        final WorkflowClientWrapped workflowClientWrapped,
                        final WorkflowServiceStubsWrapped serviceStubsWrapped,
                        final StreamResetPersistence streamResetPersistence,
                        final ConnectionManagerUtils connectionManagerUtils,
                        final NotificationClient notificationClient,
                        final StreamResetRecordsHelper streamResetRecordsHelper,
                        final MetricClient metricClient) {
    this.workspaceRoot = workspaceRoot;
    this.workflowClientWrapped = workflowClientWrapped;
    this.serviceStubsWrapped = serviceStubsWrapped;
    this.streamResetPersistence = streamResetPersistence;
    this.connectionManagerUtils = connectionManagerUtils;
    this.notificationClient = notificationClient;
    this.streamResetRecordsHelper = streamResetRecordsHelper;
    this.metricClient = metricClient;
  }

  private final Set<String> workflowNames = new HashSet<>();

  /**
   * Restart workflows stuck in a certain status.
   *
   * @param executionStatus execution status
   * @return set of connection ids that were restarted, primarily used for tracking purposes
   */
  public int restartClosedWorkflowByStatus(final WorkflowExecutionStatus executionStatus) {
    final Set<UUID> workflowExecutionInfos = fetchClosedWorkflowsByStatus(executionStatus);

    final Set<UUID> nonRunningWorkflow = filterOutRunningWorkspaceId(workflowExecutionInfos);
    nonRunningWorkflow.forEach(connectionId -> {
      connectionManagerUtils.safeTerminateWorkflow(connectionId,
          "Terminating workflow in unreachable state before starting a new workflow for this connection");
      connectionManagerUtils.startConnectionManagerNoSignal(connectionId);
    });

    return nonRunningWorkflow.size();
  }

  Set<UUID> fetchClosedWorkflowsByStatus(final WorkflowExecutionStatus executionStatus) {
    ByteString token;
    ListClosedWorkflowExecutionsRequest workflowExecutionsRequest =
        ListClosedWorkflowExecutionsRequest.newBuilder()
            .setNamespace(workflowClientWrapped.getNamespace())
            .build();

    final Set<UUID> workflowExecutionInfos = new HashSet<>();
    do {
      final ListClosedWorkflowExecutionsResponse listOpenWorkflowExecutionsRequest =
          serviceStubsWrapped.blockingStubListClosedWorkflowExecutions(workflowExecutionsRequest);
      final WorkflowType connectionManagerWorkflowType = WorkflowType.newBuilder().setName(ConnectionManagerWorkflow.class.getSimpleName()).build();
      workflowExecutionInfos.addAll(listOpenWorkflowExecutionsRequest.getExecutionsList().stream()
          .filter(workflowExecutionInfo -> workflowExecutionInfo.getType() == connectionManagerWorkflowType
              || workflowExecutionInfo.getStatus() == executionStatus)
          .flatMap((workflowExecutionInfo -> extractConnectionIdFromWorkflowId(workflowExecutionInfo.getExecution().getWorkflowId()).stream()))
          .collect(Collectors.toSet()));
      token = listOpenWorkflowExecutionsRequest.getNextPageToken();

      workflowExecutionsRequest =
          ListClosedWorkflowExecutionsRequest.newBuilder()
              .setNamespace(workflowClientWrapped.getNamespace())
              .setNextPageToken(token)
              .build();

    } while (token != null && token.size() > 0);

    return workflowExecutionInfos;
  }

  @VisibleForTesting
  Set<UUID> filterOutRunningWorkspaceId(final Set<UUID> workflowIds) {
    refreshRunningWorkflow();

    final Set<UUID> runningWorkflowByUUID =
        workflowNames.stream().flatMap(name -> extractConnectionIdFromWorkflowId(name).stream()).collect(Collectors.toSet());

    return workflowIds.stream().filter(workflowId -> !runningWorkflowByUUID.contains(workflowId)).collect(Collectors.toSet());
  }

  @VisibleForTesting
  void refreshRunningWorkflow() {
    workflowNames.clear();
    ByteString token;
    ListOpenWorkflowExecutionsRequest openWorkflowExecutionsRequest =
        ListOpenWorkflowExecutionsRequest.newBuilder()
            .setNamespace(workflowClientWrapped.getNamespace())
            .build();
    do {
      final ListOpenWorkflowExecutionsResponse listOpenWorkflowExecutionsRequest =
          serviceStubsWrapped.blockingStubListOpenWorkflowExecutions(openWorkflowExecutionsRequest);
      final Set<String> workflowExecutionInfos = listOpenWorkflowExecutionsRequest.getExecutionsList().stream()
          .map((workflowExecutionInfo -> workflowExecutionInfo.getExecution().getWorkflowId()))
          .collect(Collectors.toSet());
      workflowNames.addAll(workflowExecutionInfos);
      token = listOpenWorkflowExecutionsRequest.getNextPageToken();

      openWorkflowExecutionsRequest =
          ListOpenWorkflowExecutionsRequest.newBuilder()
              .setNamespace(workflowClientWrapped.getNamespace())
              .setNextPageToken(token)
              .build();

    } while (token != null && token.size() > 0);
  }

  Optional<UUID> extractConnectionIdFromWorkflowId(final String workflowId) {
    if (!workflowId.startsWith("connection_manager_")) {
      return Optional.empty();
    }
    return Optional.ofNullable(StringUtils.removeStart(workflowId, "connection_manager_"))
        .map(
            stringUUID -> UUID.fromString(stringUUID));
  }

  /**
   * Result of a manual operation.
   */
  @Value
  @Builder
  public static class ManualOperationResult {

    final Optional<String> failingReason;
    final Optional<Long> jobId;
    final Optional<ErrorCode> errorCode;

  }

  public Optional<WorkflowState> getWorkflowState(final UUID connectionId) {
    return connectionManagerUtils.getWorkflowState(connectionId);
  }

  /**
   * Start a manual sync for a connection.
   *
   * @param connectionId connection id
   * @return sync result
   */
  public ManualOperationResult startNewManualSync(final UUID connectionId) {
    log.info("Manual sync request");

    if (connectionManagerUtils.isWorkflowStateRunning(connectionId)) {
      // TODO Bmoric: Error is running
      return new ManualOperationResult(
          Optional.of("A sync is already running for: " + connectionId),
          Optional.empty(), Optional.of(ErrorCode.WORKFLOW_RUNNING));
    }

    try {
      connectionManagerUtils.signalWorkflowAndRepairIfNecessary(connectionId, workflow -> workflow::submitManualSync);
    } catch (final DeletedWorkflowException e) {
      log.error("Can't sync a deleted connection.", e);
      return new ManualOperationResult(
          Optional.of(e.getMessage()),
          Optional.empty(), Optional.of(ErrorCode.WORKFLOW_DELETED));
    }

    do {
      try {
        Thread.sleep(DELAY_BETWEEN_QUERY_MS);
      } catch (final InterruptedException e) {
        return new ManualOperationResult(
            Optional.of("Didn't managed to start a sync for: " + connectionId),
            Optional.empty(), Optional.of(ErrorCode.UNKNOWN));
      }
    } while (!connectionManagerUtils.isWorkflowStateRunning(connectionId));

    log.info("end of manual schedule");

    final long jobId = connectionManagerUtils.getCurrentJobId(connectionId);

    return new ManualOperationResult(
        Optional.empty(),
        Optional.of(jobId), Optional.empty());
  }

  /**
   * Cancel a running job for a connection.
   *
   * @param connectionId connection id
   * @return cancellation result
   */
  public ManualOperationResult startNewCancellation(final UUID connectionId) {
    log.info("Manual cancellation request");

    final long jobId = connectionManagerUtils.getCurrentJobId(connectionId);

    try {
      connectionManagerUtils.signalWorkflowAndRepairIfNecessary(connectionId, workflow -> workflow::cancelJob);
    } catch (final DeletedWorkflowException e) {
      log.error("Can't cancel a deleted workflow", e);
      return new ManualOperationResult(
          Optional.of(e.getMessage()),
          Optional.empty(), Optional.of(ErrorCode.WORKFLOW_DELETED));
    }

    do {
      try {
        Thread.sleep(DELAY_BETWEEN_QUERY_MS);
      } catch (final InterruptedException e) {
        return new ManualOperationResult(
            Optional.of("Didn't manage to cancel a sync for: " + connectionId),
            Optional.empty(), Optional.of(ErrorCode.UNKNOWN));
      }
    } while (connectionManagerUtils.isWorkflowStateRunning(connectionId));

    streamResetRecordsHelper.deleteStreamResetRecordsForJob(jobId, connectionId);

    log.info("end of manual cancellation");

    return new ManualOperationResult(
        Optional.empty(),
        Optional.of(jobId), Optional.empty());
  }

  /**
   * Submit a reset connection job to temporal.
   *
   * @param connectionId connection id
   * @param streamsToReset streams that should be rest on the connection
   * @param syncImmediatelyAfter whether another sync job should be triggered immediately after
   * @return result of reset connection
   */
  public ManualOperationResult resetConnection(final UUID connectionId,
                                               final List<StreamDescriptor> streamsToReset,
                                               final boolean syncImmediatelyAfter) {
    log.info("reset sync request");

    try {
      streamResetPersistence.createStreamResets(connectionId, streamsToReset);
    } catch (final IOException e) {
      log.error("Could not persist streams to reset.", e);
      return new ManualOperationResult(
          Optional.of(e.getMessage()),
          Optional.empty(), Optional.of(ErrorCode.UNKNOWN));
    }

    // get the job ID before the reset, defaulting to NON_RUNNING_JOB_ID if workflow is unreachable
    final long oldJobId = connectionManagerUtils.getCurrentJobId(connectionId);

    try {
      if (syncImmediatelyAfter) {
        connectionManagerUtils.signalWorkflowAndRepairIfNecessary(connectionId, workflow -> workflow::resetConnectionAndSkipNextScheduling);
      } else {
        connectionManagerUtils.signalWorkflowAndRepairIfNecessary(connectionId, workflow -> workflow::resetConnection);
      }
    } catch (final DeletedWorkflowException e) {
      log.error("Can't reset a deleted workflow", e);
      return new ManualOperationResult(
          Optional.of(e.getMessage()),
          Optional.empty(), Optional.of(ErrorCode.UNKNOWN));
    }

    Optional<Long> newJobId;

    do {
      try {
        Thread.sleep(DELAY_BETWEEN_QUERY_MS);
      } catch (final InterruptedException e) {
        return new ManualOperationResult(
            Optional.of("Didn't manage to reset a sync for: " + connectionId),
            Optional.empty(), Optional.of(ErrorCode.UNKNOWN));
      }
      newJobId = getNewJobId(connectionId, oldJobId);
    } while (newJobId.isEmpty());

    log.info("end of reset submission");

    return new ManualOperationResult(
        Optional.empty(),
        newJobId, Optional.empty());
  }

  private Optional<Long> getNewJobId(final UUID connectionId, final long oldJobId) {
    final long currentJobId = connectionManagerUtils.getCurrentJobId(connectionId);
    if (currentJobId == NON_RUNNING_JOB_ID || currentJobId == oldJobId) {
      return Optional.empty();
    } else {
      return Optional.of(currentJobId);
    }
  }

  /**
   * Submit a spec job to temporal.
   *
   * @param jobId job id
   * @param attempt attempt
   * @param config spec config
   * @return spec output
   */
  public TemporalResponse<ConnectorJobOutput> submitGetSpec(final UUID jobId,
                                                            final int attempt,
                                                            final @Nullable UUID workspaceId,
                                                            final JobGetSpecConfig config) {
    final JobRunConfig jobRunConfig = TemporalWorkflowUtils.createJobRunConfig(jobId, attempt);

    final IntegrationLauncherConfig launcherConfig = new IntegrationLauncherConfig()
        .withJobId(jobId.toString())
        .withAttemptId((long) attempt)
        .withWorkspaceId(workspaceId)
        .withDockerImage(config.getDockerImage())
        .withIsCustomConnector(config.getIsCustomConnector());
    return execute(jobRunConfig,
        () -> getWorkflowStub(SpecWorkflow.class, TemporalJobType.GET_SPEC).run(jobRunConfig, launcherConfig));

  }

  /**
   * Submit a check job to temporal.
   *
   * @param jobId job id
   * @param attempt attempt
   * @param taskQueue task queue to submit the job to
   * @param config check config
   * @return check output
   */
  public TemporalResponse<ConnectorJobOutput> submitCheckConnection(final UUID jobId,
                                                                    final int attempt,
                                                                    final UUID workspaceId,
                                                                    final String taskQueue,
                                                                    final JobCheckConnectionConfig config,
                                                                    final ActorContext context) {
    final JobRunConfig jobRunConfig = TemporalWorkflowUtils.createJobRunConfig(jobId, attempt);
    final IntegrationLauncherConfig launcherConfig = new IntegrationLauncherConfig()
        .withJobId(jobId.toString())
        .withAttemptId((long) attempt)
        .withWorkspaceId(workspaceId)
        .withDockerImage(config.getDockerImage())
        .withProtocolVersion(config.getProtocolVersion())
        .withIsCustomConnector(config.getIsCustomConnector());

    final StandardCheckConnectionInput input = new StandardCheckConnectionInput()
        .withActorType(config.getActorType())
        .withActorId(config.getActorId())
        .withConnectionConfiguration(config.getConnectionConfiguration())
        .withResourceRequirements(config.getResourceRequirements())
        .withActorContext(context);

    return execute(jobRunConfig,
        () -> getWorkflowStubWithTaskQueue(CheckConnectionWorkflow.class, taskQueue).run(jobRunConfig, launcherConfig, input));
  }

  /**
   * Submit a discover job to temporal.
   *
   * @param jobId job id
   * @param attempt attempt
   * @param taskQueue task queue to submit the job to
   * @param config discover config
   * @param context actor context
   * @return discover output
   */
  public TemporalResponse<ConnectorJobOutput> submitDiscoverSchema(final UUID jobId,
                                                                   final int attempt,
                                                                   final UUID workspaceId,
                                                                   final String taskQueue,
                                                                   final JobDiscoverCatalogConfig config,
                                                                   final ActorContext context) {
    final JobRunConfig jobRunConfig = TemporalWorkflowUtils.createJobRunConfig(jobId, attempt);
    final IntegrationLauncherConfig launcherConfig = new IntegrationLauncherConfig()
        .withJobId(jobId.toString())
        .withAttemptId((long) attempt)
        .withWorkspaceId(workspaceId)
        .withDockerImage(config.getDockerImage())
        .withProtocolVersion(config.getProtocolVersion())
        .withIsCustomConnector(config.getIsCustomConnector());
    final StandardDiscoverCatalogInput input = new StandardDiscoverCatalogInput().withConnectionConfiguration(config.getConnectionConfiguration())
        .withSourceId(config.getSourceId()).withConnectorVersion(config.getConnectorVersion()).withConfigHash(config.getConfigHash())
        .withResourceRequirements(config.getResourceRequirements()).withActorContext(context);

    return execute(jobRunConfig,
        () -> getWorkflowStubWithTaskQueue(DiscoverCatalogWorkflow.class, taskQueue).run(jobRunConfig, launcherConfig, input));
  }

  /**
   * Run update to start connection manager workflows for connection ids.
   *
   * @param connectionIds connection ids
   */
  // todo (cgardens) - i dunno what this is
  public void migrateSyncIfNeeded(final Set<UUID> connectionIds) {
    final StopWatch globalMigrationWatch = new StopWatch();
    globalMigrationWatch.start();
    refreshRunningWorkflow();

    connectionIds.forEach((connectionId) -> {
      final StopWatch singleSyncMigrationWatch = new StopWatch();
      singleSyncMigrationWatch.start();
      if (!isInRunningWorkflowCache(connectionManagerUtils.getConnectionManagerName(connectionId))) {
        log.info("Migrating: " + connectionId);
        try {
          submitConnectionUpdaterAsync(connectionId);
        } catch (final Exception e) {
          log.error("New workflow submission failed, retrying", e);
          refreshRunningWorkflow();
          submitConnectionUpdaterAsync(connectionId);
        }
      }
      singleSyncMigrationWatch.stop();
      log.info("Sync migration took: " + singleSyncMigrationWatch.formatTime());
    });
    globalMigrationWatch.stop();

    log.info("The migration to the new scheduler took: " + globalMigrationWatch.formatTime());
  }

  @VisibleForTesting
  <T> TemporalResponse<T> execute(final JobRunConfig jobRunConfig, final Supplier<T> executor) {
    final Path jobRoot = TemporalUtils.getJobRoot(workspaceRoot, jobRunConfig);
    final Path logPath = TemporalUtils.getLogPath(jobRoot);

    T operationOutput = null;
    RuntimeException exception = null;

    try {
      operationOutput = executor.get();
    } catch (final RuntimeException e) {
      exception = e;
    }

    boolean succeeded = exception == null;
    if (succeeded && operationOutput instanceof ConnectorJobOutput) {
      succeeded = getConnectorJobSucceeded((ConnectorJobOutput) operationOutput);
    }

    final JobMetadata metadata = new JobMetadata(succeeded, logPath);
    return new TemporalResponse<>(operationOutput, metadata);
  }

  private <T> T getWorkflowStub(final Class<T> workflowClass, final TemporalJobType jobType) {
    return workflowClientWrapped.newWorkflowStub(workflowClass, TemporalWorkflowUtils.buildWorkflowOptions(jobType));
  }

  private <T> T getWorkflowStubWithTaskQueue(final Class<T> workflowClass, final String taskQueue) {
    return workflowClientWrapped.newWorkflowStub(workflowClass, TemporalWorkflowUtils.buildWorkflowOptionsWithTaskQueue(taskQueue));
  }

  /**
   * Signal to the connection manager workflow asynchronously that there has been a change to the
   * connection's configuration.
   *
   * @param connectionId connection id
   */
  public ConnectionManagerWorkflow submitConnectionUpdaterAsync(final UUID connectionId) {
    log.info("Starting the scheduler temporal wf");
    final ConnectionManagerWorkflow connectionManagerWorkflow =
        connectionManagerUtils.startConnectionManagerNoSignal(connectionId);
    try {
      CompletableFuture.supplyAsync(() -> {
        try {
          do {
            Thread.sleep(DELAY_BETWEEN_QUERY_MS);
          } while (!isWorkflowReachable(connectionId));
        } catch (final InterruptedException e) {
          // no op
        }
        return null;
      }).get(60, TimeUnit.SECONDS);
    } catch (final InterruptedException | ExecutionException e) {
      log.error("Failed to create a new connection manager workflow", e);
    } catch (final TimeoutException e) {
      log.error("Can't create a new connection manager workflow due to timeout", e);
    }

    return connectionManagerWorkflow;
  }

  /**
   * This will cancel a workflow even if the connection is deleted already.
   *
   * @param connectionId - connectionId to cancel
   */
  public void forceDeleteWorkflow(final UUID connectionId) {
    connectionManagerUtils.deleteWorkflowIfItExist(connectionId);
  }

  public void sendSchemaChangeNotification(final UUID connectionId,
                                           final String connectionName,
                                           final String sourceName,
                                           final String url,
                                           final boolean containsBreakingChange) {
    notificationClient.sendSchemaChangeNotification(connectionId, connectionName, sourceName, url, containsBreakingChange);
  }

  /**
   * Signal to the connection manager workflow that there has been a change to the connection's
   * configuration.
   *
   * @param connectionId connection id
   */
  public void update(final UUID connectionId) {
    final ConnectionManagerWorkflow connectionManagerWorkflow;
    try {
      connectionManagerWorkflow = connectionManagerUtils.getConnectionManagerWorkflow(connectionId);
    } catch (final DeletedWorkflowException e) {
      log.info("Connection {} is deleted, and therefore cannot be updated.", connectionId);
      return;
    } catch (final UnreachableWorkflowException e) {
      metricClient.count(OssMetricsRegistry.WORFLOW_UNREACHABLE, 1,
          new MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()));
      log.error(
          String.format("Failed to retrieve ConnectionManagerWorkflow for connection %s. Repairing state by creating new workflow.", connectionId),
          e);
      connectionManagerUtils.safeTerminateWorkflow(connectionId,
          "Terminating workflow in unreachable state before starting a new workflow for this connection");
      submitConnectionUpdaterAsync(connectionId);
      return;
    }

    connectionManagerWorkflow.connectionUpdated();
  }

  private boolean getConnectorJobSucceeded(final ConnectorJobOutput output) {
    return output.getFailureReason() == null;
  }

  /**
   * Check if a workflow is reachable for signal calls by attempting to query for current state. If
   * the query succeeds, and the workflow is not marked as deleted, the workflow is reachable.
   */
  @VisibleForTesting
  boolean isWorkflowReachable(final UUID connectionId) {
    try {
      connectionManagerUtils.getConnectionManagerWorkflow(connectionId);
      return true;
    } catch (final Exception e) {
      return false;
    }
  }

  boolean isInRunningWorkflowCache(final String workflowName) {
    return workflowNames.contains(workflowName);
  }

}
