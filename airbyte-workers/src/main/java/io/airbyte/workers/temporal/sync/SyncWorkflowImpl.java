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
import io.airbyte.config.OperatorWebhookInput;
import io.airbyte.config.StandardDiscoverCatalogInput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardSyncOperation.OperatorType;
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
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Workflow;
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
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private WebhookOperationActivity webhookOperationActivity;
  @TemporalActivityStub(activityOptionsBeanName = "refreshSchemaActivityOptions")
  private RefreshSchemaActivity refreshSchemaActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private ConfigFetchActivity configFetchActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private WorkloadFeatureFlagActivity workloadFeatureFlagActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private ReportRunTimeActivity reportRunTimeActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private SyncFeatureFlagFetcherActivity syncFeatureFlagFetcherActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private RouteToSyncTaskQueueActivity routeToSyncTaskQueueActivity;

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public StandardSyncOutput run(final JobRunConfig jobRunConfig,
                                final IntegrationLauncherConfig sourceLauncherConfig,
                                final IntegrationLauncherConfig destinationLauncherConfig,
                                final StandardSyncInput syncInput,
                                final UUID connectionId) {

    final long startTime = Workflow.currentTimeMillis();
    // TODO: Remove this once Workload API rolled out
    final var useWorkloadApi = checkUseWorkloadApiFlag(syncInput.getWorkspaceId());
    final var useWorkloadOutputDocStore = checkUseWorkloadOutputFlag(syncInput);
    final var sendRunTimeMetrics = shouldReportRuntime();
    final var shouldRunAsChildWorkflow = shouldRunAsAChildWorkflow(connectionId, syncInput.getWorkspaceId(),
        syncInput.getConnectionContext().getSourceDefinitionId(), syncInput.getIsReset());

    ApmTraceUtils
        .addTagsToTrace(Map.of(
            ATTEMPT_NUMBER_KEY, jobRunConfig.getAttemptId(),
            CONNECTION_ID_KEY, connectionId.toString(),
            JOB_ID_KEY, jobRunConfig.getJobId(),
            SOURCE_DOCKER_IMAGE_KEY, sourceLauncherConfig.getDockerImage(),
            DESTINATION_DOCKER_IMAGE_KEY, destinationLauncherConfig.getDockerImage(),
            "USE_WORKLOAD_API", useWorkloadApi));

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

    StandardSyncOutput syncOutput = replicationActivity
        .replicateV2(generateReplicationActivityInput(syncInput, jobRunConfig, sourceLauncherConfig, destinationLauncherConfig, taskQueue,
            refreshSchemaOutput, useWorkloadApi, useWorkloadOutputDocStore));

    if (syncInput.getOperationSequence() != null && !syncInput.getOperationSequence().isEmpty()) {
      for (final StandardSyncOperation standardSyncOperation : syncInput.getOperationSequence()) {
        if (standardSyncOperation.getOperatorType() == OperatorType.WEBHOOK) {
          LOGGER.info("running webhook operation");
          LOGGER.debug("webhook operation input: {}", standardSyncOperation);
          final boolean success = webhookOperationActivity
              .invokeWebhook(new OperatorWebhookInput()
                  .withExecutionUrl(standardSyncOperation.getOperatorWebhook().getExecutionUrl())
                  .withExecutionBody(standardSyncOperation.getOperatorWebhook().getExecutionBody())
                  .withWebhookConfigId(standardSyncOperation.getOperatorWebhook().getWebhookConfigId())
                  .withWorkspaceWebhookConfigs(syncInput.getWebhookOperationConfigs())
                  .withConnectionContext(syncInput.getConnectionContext()));
          LOGGER.info("webhook {} completed {}", standardSyncOperation.getOperatorWebhook().getWebhookConfigId(),
              success ? "successfully" : "unsuccessfully");
          // TODO(mfsiega-airbyte): clean up this logic to be returned from the webhook invocation.
          if (syncOutput.getWebhookOperationSummary() == null) {
            syncOutput.withWebhookOperationSummary(new WebhookOperationSummary());
          }
          if (success) {
            syncOutput.getWebhookOperationSummary().getSuccesses().add(standardSyncOperation.getOperatorWebhook().getWebhookConfigId());
          } else {
            syncOutput.getWebhookOperationSummary().getFailures().add(standardSyncOperation.getOperatorWebhook().getWebhookConfigId());
          }
        } else {
          LOGGER.warn("Unsupported operation type '{}' found.  Skipping operation...", standardSyncOperation.getOperatorType());
        }
      }
    }

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
      return checkUseWorkloadApiFlag(workspaceId)
          && syncFeatureFlagFetcherActivity.shouldRunAsChildWorkflow(new SyncFeatureFlagFetcherInput(
              Optional.ofNullable(connectionId).orElse(DEFAULT_UUID),
              sourceDefinitionId,
              workspaceId));
    } else {
      return !isReset && checkUseWorkloadApiFlag(workspaceId)
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
                                                                    final RefreshSchemaActivityOutput refreshSchemaOutput,
                                                                    final boolean useWorkloadApi,
                                                                    final boolean useWorkloadOutputDocStore) {
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
        useWorkloadApi,
        useWorkloadOutputDocStore);
  }

  private boolean checkUseWorkloadApiFlag(final UUID workspaceId) {
    return workloadFeatureFlagActivity.useWorkloadApi(new WorkloadFeatureFlagActivity.Input(workspaceId));
  }

  private boolean checkUseWorkloadOutputFlag(final StandardSyncInput syncInput) {
    return workloadFeatureFlagActivity.useOutputDocStore(new WorkloadFeatureFlagActivity.Input(
        syncInput.getWorkspaceId()));
  }

}
