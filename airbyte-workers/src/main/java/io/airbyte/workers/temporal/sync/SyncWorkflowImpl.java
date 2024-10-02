/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.DESTINATION_DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.SOURCE_DOCKER_IMAGE_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import datadog.trace.api.Trace;
import io.airbyte.api.client.model.generated.ConnectionStatus;
import io.airbyte.commons.helper.DockerImageName;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.temporal.annotations.TemporalActivityStub;
import io.airbyte.commons.temporal.scheduling.DiscoverCatalogAndAutoPropagateWorkflow;
import io.airbyte.commons.temporal.scheduling.SyncWorkflow;
import io.airbyte.config.ActorContext;
import io.airbyte.config.ActorType;
import io.airbyte.config.SignalInput;
import io.airbyte.config.StandardDiscoverCatalogInput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.config.SyncStats;
import io.airbyte.config.WebhookOperationSummary;
import io.airbyte.config.WorkloadPriority;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.models.RefreshSchemaActivityInput;
import io.airbyte.workers.models.RefreshSchemaActivityOutput;
import io.airbyte.workers.models.ReplicationActivityInput;
import io.airbyte.workers.temporal.activities.ReportRunTimeActivityInput;
import io.airbyte.workers.temporal.activities.SyncFeatureFlagFetcherInput;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity;
import io.airbyte.workers.temporal.scheduling.activities.RouteToSyncTaskQueueActivity;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.CanceledFailure;
import io.temporal.workflow.CancellationScope;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sync temporal workflow impl.
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class SyncWorkflowImpl implements SyncWorkflow {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncWorkflowImpl.class);

  private static final UUID DEFAULT_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

  private static final HashFunction HASH_FUNCTION = Hashing.md5();

  @TemporalActivityStub(activityOptionsBeanName = "longRunActivityOptions")
  private ReplicationActivity replicationActivity;
  @TemporalActivityStub(activityOptionsBeanName = "refreshSchemaActivityOptions")
  private RefreshSchemaActivity refreshSchemaActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private ConfigFetchActivity configFetchActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private ReportRunTimeActivity reportRunTimeActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private SyncFeatureFlagFetcherActivity syncFeatureFlagFetcherActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private RouteToSyncTaskQueueActivity routeToSyncTaskQueueActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private InvokeOperationsActivity invokeOperationsActivity;
  @TemporalActivityStub(activityOptionsBeanName = "asyncActivityOptions")
  private AsyncReplicationActivity asyncReplicationActivity;
  @TemporalActivityStub(activityOptionsBeanName = "workloadStatusCheckActivityOptions")
  private WorkloadStatusCheckActivity workloadStatusCheckActivity;

  private Boolean shouldBlock;

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public void checkAsyncActivityStatus() {
    this.shouldBlock = false;
  }

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public StandardSyncOutput run(final JobRunConfig jobRunConfig,
                                final IntegrationLauncherConfig sourceLauncherConfig,
                                final IntegrationLauncherConfig destinationLauncherConfig,
                                final StandardSyncInput syncInput,
                                final UUID connectionId) {

    final long startTime = Workflow.currentTimeMillis();
    // TODO: Remove this once Workload API rolled out
    final var sendRunTimeMetrics = shouldReportRuntime();
    final var shouldRunAsChildWorkflow = shouldRunAsAChildWorkflow(connectionId, syncInput.getWorkspaceId(),
        syncInput.getConnectionContext().getSourceDefinitionId(), syncInput.getIsReset());

    ApmTraceUtils
        .addTagsToTrace(Map.of(
            ATTEMPT_NUMBER_KEY, jobRunConfig.getAttemptId(),
            CONNECTION_ID_KEY, connectionId.toString(),
            JOB_ID_KEY, jobRunConfig.getJobId(),
            SOURCE_DOCKER_IMAGE_KEY, sourceLauncherConfig.getDockerImage(),
            DESTINATION_DOCKER_IMAGE_KEY, destinationLauncherConfig.getDockerImage()));

    final String taskQueue = Workflow.getInfo().getTaskQueue();

    final Optional<UUID> sourceId = getSourceId(syncInput);
    RefreshSchemaActivityOutput refreshSchemaOutput = null;
    final boolean shouldRefreshSchema = sourceId.isPresent() && refreshSchemaActivity.shouldRefreshSchema(sourceId.get());
    if (sourceId.isPresent() && (shouldRefreshSchema || shouldRunAsChildWorkflow)) {
      try {
        if (shouldRunAsChildWorkflow) {
          final JsonNode sourceConfig = configFetchActivity.getSourceConfig(sourceId.get());
          final String discoverTaskQueue = routeToSyncTaskQueueActivity.routeToDiscoverCatalog(
              new RouteToSyncTaskQueueActivity.RouteToSyncTaskQueueInput(connectionId)).getTaskQueue();
          refreshSchemaOutput = runDiscoverAsChildWorkflow(jobRunConfig, sourceLauncherConfig, syncInput, sourceConfig, discoverTaskQueue);
        } else if (shouldRefreshSchema) {
          refreshSchemaOutput =
              refreshSchemaActivity.refreshSchemaV2(new RefreshSchemaActivityInput(sourceId.get(), connectionId, syncInput.getWorkspaceId()));
        }
      } catch (final Exception e) {
        ApmTraceUtils.addExceptionToTrace(e);
        return SyncOutputProvider.getRefreshSchemaFailure(e);
      }
    }

    final long discoverSchemaEndTime = Workflow.currentTimeMillis();

    final Optional<ConnectionStatus> status = configFetchActivity.getStatus(connectionId);
    if (!status.isEmpty() && ConnectionStatus.INACTIVE == status.get()) {
      LOGGER.info("Connection {} is disabled. Cancelling run.", connectionId);
      final StandardSyncOutput output =
          new StandardSyncOutput()
              .withStandardSyncSummary(new StandardSyncSummary().withStatus(ReplicationStatus.CANCELLED).withTotalStats(new SyncStats()));
      return output;
    }

    final ReplicationActivityInput replicationActivityInput =
        generateReplicationActivityInput(syncInput, jobRunConfig, sourceLauncherConfig, destinationLauncherConfig, taskQueue,
            refreshSchemaOutput);
    final StandardSyncOutput syncOutput;

    if (syncInput.getUseAsyncReplicate() == null || !syncInput.getUseAsyncReplicate()) {
      syncOutput = replicationActivity.replicateV2(replicationActivityInput);
    } else {
      final String workloadId = asyncReplicationActivity.startReplication(replicationActivityInput);

      try {
        shouldBlock = true;
        while (shouldBlock) {
          Workflow.await(Duration.ofMinutes(15), () -> !shouldBlock);
          shouldBlock = !workloadStatusCheckActivity.isTerminal(workloadId);
        }
      } catch (final CanceledFailure | ActivityFailure cf) {
        if (workloadId != null) {
          // This is in order to be usable from the detached scope
          CancellationScope detached =
              Workflow.newDetachedCancellationScope(() -> {
                asyncReplicationActivity.cancel(replicationActivityInput, workloadId);
                shouldBlock = false;
              });
          detached.run();
        }
        throw cf;
      }

      syncOutput = asyncReplicationActivity.getReplicationOutput(replicationActivityInput, workloadId);
    }

    final WebhookOperationSummary webhookOperationSummary = invokeOperationsActivity.invokeOperations(
        syncInput.getOperationSequence(), syncInput, jobRunConfig);
    syncOutput.setWebhookOperationSummary(webhookOperationSummary);

    final long replicationEndTime = Workflow.currentTimeMillis();

    if (sendRunTimeMetrics) {
      reportRunTimeActivity.reportRunTime(new ReportRunTimeActivityInput(
          connectionId,
          syncInput.getConnectionContext() == null || syncInput.getConnectionContext().getSourceDefinitionId() == null
              ? DEFAULT_UUID
              : syncInput.getConnectionContext().getSourceDefinitionId(),
          startTime,
          discoverSchemaEndTime,
          replicationEndTime,
          shouldRefreshSchema));
    }

    if (shouldRefreshSchema && syncOutput.getStandardSyncSummary() != null && syncOutput.getStandardSyncSummary().getTotalStats() != null) {
      syncOutput.getStandardSyncSummary().getTotalStats().setDiscoverSchemaEndTime(discoverSchemaEndTime);
      syncOutput.getStandardSyncSummary().getTotalStats().setDiscoverSchemaStartTime(startTime);
    }

    return syncOutput;
  }

  private Optional<UUID> getSourceId(final StandardSyncInput syncInput) {
    final int shouldGetSourceFromSyncInput = Workflow.getVersion("SHOULD_GET_SOURCE_FROM_SYNC_INPUT", Workflow.DEFAULT_VERSION, 1);
    if (shouldGetSourceFromSyncInput != Workflow.DEFAULT_VERSION) {
      return Optional.ofNullable(syncInput.getSourceId());
    }
    return configFetchActivity.getSourceId(syncInput.getConnectionId());
  }

  @VisibleForTesting
  public RefreshSchemaActivityOutput runDiscoverAsChildWorkflow(final JobRunConfig jobRunConfig,
                                                                final IntegrationLauncherConfig sourceLauncherConfig,
                                                                final StandardSyncInput syncInput,
                                                                final JsonNode sourceConfig,
                                                                final String discoverTaskQueue) {
    try {
      final DiscoverCatalogAndAutoPropagateWorkflow childDiscoverCatalogWorkflow =
          Workflow.newChildWorkflowStub(DiscoverCatalogAndAutoPropagateWorkflow.class,
              ChildWorkflowOptions.newBuilder()
                  .setWorkflowId("discover_" + jobRunConfig.getJobId() + "_" + jobRunConfig.getAttemptId())
                  .setTaskQueue(discoverTaskQueue)
                  .build());
      return childDiscoverCatalogWorkflow.run(jobRunConfig, sourceLauncherConfig.withPriority(WorkloadPriority.DEFAULT),
          new StandardDiscoverCatalogInput()
              .withActorContext(new ActorContext()
                  .withActorDefinitionId(syncInput.getConnectionContext().getSourceDefinitionId())
                  .withActorType(ActorType.SOURCE)
                  .withActorId(syncInput.getSourceId())
                  .withWorkspaceId(syncInput.getWorkspaceId())
                  .withOrganizationId(syncInput.getConnectionContext().getOrganizationId()))
              .withConnectionConfiguration(syncInput.getSourceConfiguration())
              .withSourceId(syncInput.getSourceId().toString())
              .withConfigHash(HASH_FUNCTION.hashBytes(Jsons.serialize(sourceConfig).getBytes(
                  Charsets.UTF_8)).toString())
              .withConnectorVersion(DockerImageName.INSTANCE.extractTag(sourceLauncherConfig.getDockerImage()))
              .withManual(false));
    } catch (Exception e) {
      LOGGER.error("error", e);
      throw new RuntimeException(e);
    }
  }

  private boolean shouldRunAsAChildWorkflow(final UUID connectionId, final UUID workspaceId, final UUID sourceDefinitionId, final boolean isReset) {
    final int shouldRunAsChildWorkflowVersion = Workflow.getVersion("SHOULD_RUN_AS_CHILD", Workflow.DEFAULT_VERSION, 2);
    final int versionWithoutResetCheck = 1;
    if (shouldRunAsChildWorkflowVersion == Workflow.DEFAULT_VERSION) {
      return false;
    } else if (shouldRunAsChildWorkflowVersion == versionWithoutResetCheck) {
      return syncFeatureFlagFetcherActivity.shouldRunAsChildWorkflow(new SyncFeatureFlagFetcherInput(
          Optional.ofNullable(connectionId).orElse(DEFAULT_UUID),
          sourceDefinitionId,
          workspaceId));
    } else {
      return !isReset
          && syncFeatureFlagFetcherActivity.shouldRunAsChildWorkflow(new SyncFeatureFlagFetcherInput(
              Optional.ofNullable(connectionId).orElse(DEFAULT_UUID),
              sourceDefinitionId,
              workspaceId));
    }

  }

  private boolean shouldReportRuntime() {
    final int shouldReportRuntimeVersion = Workflow.getVersion("SHOULD_REPORT_RUNTIME", Workflow.DEFAULT_VERSION, 1);

    return shouldReportRuntimeVersion != Workflow.DEFAULT_VERSION;
  }

  private ReplicationActivityInput generateReplicationActivityInput(final StandardSyncInput syncInput,
                                                                    final JobRunConfig jobRunConfig,
                                                                    final IntegrationLauncherConfig sourceLauncherConfig,
                                                                    final IntegrationLauncherConfig destinationLauncherConfig,
                                                                    final String taskQueue,
                                                                    final RefreshSchemaActivityOutput refreshSchemaOutput) {
    final String signalInput;
    if (syncInput.getUseAsyncReplicate() != null && syncInput.getUseAsyncReplicate()) {
      signalInput = Jsons.serialize(new SignalInput(SignalInput.SYNC_WORKFLOW, Workflow.getInfo().getWorkflowId()));
    } else {
      signalInput = null;
    }
    return new ReplicationActivityInput(
        syncInput.getSourceId(),
        syncInput.getDestinationId(),
        syncInput.getSourceConfiguration(),
        syncInput.getDestinationConfiguration(),
        jobRunConfig,
        sourceLauncherConfig,
        destinationLauncherConfig,
        syncInput.getSyncResourceRequirements(),
        syncInput.getWorkspaceId(),
        syncInput.getConnectionId(),
        taskQueue,
        syncInput.getIsReset(),
        syncInput.getNamespaceDefinition(),
        syncInput.getNamespaceFormat(),
        syncInput.getPrefix(),
        refreshSchemaOutput,
        syncInput.getConnectionContext(),
        signalInput);
  }

}
