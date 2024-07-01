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

import datadog.trace.api.Trace;
import io.airbyte.api.client.model.generated.ConnectionStatus;
import io.airbyte.commons.temporal.annotations.TemporalActivityStub;
import io.airbyte.commons.temporal.scheduling.SyncWorkflow;
import io.airbyte.config.OperatorWebhookInput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardSyncOperation.OperatorType;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.config.SyncStats;
import io.airbyte.config.WebhookOperationSummary;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.models.RefreshSchemaActivityInput;
import io.airbyte.workers.models.RefreshSchemaActivityOutput;
import io.airbyte.workers.models.ReplicationActivityInput;
import io.airbyte.workers.temporal.activities.ReportRunTimeActivityInput;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity;
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

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public StandardSyncOutput run(final JobRunConfig jobRunConfig,
                                final IntegrationLauncherConfig sourceLauncherConfig,
                                final IntegrationLauncherConfig destinationLauncherConfig,
                                final StandardSyncInput syncInput,
                                final UUID connectionId) {

    final long startTime = Workflow.currentTimeMillis();
    // TODO: Remove this once Workload API rolled out
    final var useWorkloadApi = checkUseWorkloadApiFlag(syncInput);
    final var useWorkloadOutputDocStore = checkUseWorkloadOutputFlag(syncInput);
    final var sendRunTimeMetrics = shouldReportRuntime();

    ApmTraceUtils
        .addTagsToTrace(Map.of(
            ATTEMPT_NUMBER_KEY, jobRunConfig.getAttemptId(),
            CONNECTION_ID_KEY, connectionId.toString(),
            JOB_ID_KEY, jobRunConfig.getJobId(),
            SOURCE_DOCKER_IMAGE_KEY, sourceLauncherConfig.getDockerImage(),
            DESTINATION_DOCKER_IMAGE_KEY, destinationLauncherConfig.getDockerImage(),
            "USE_WORKLOAD_API", useWorkloadApi));

    final String taskQueue = Workflow.getInfo().getTaskQueue();

    final Optional<UUID> sourceId = configFetchActivity.getSourceId(connectionId);
    RefreshSchemaActivityOutput refreshSchemaOutput = null;
    final boolean shouldRefreshSchema = refreshSchemaActivity.shouldRefreshSchema(sourceId.get());
    if (!sourceId.isEmpty() && shouldRefreshSchema) {
      LOGGER.info("Refreshing source schema...");
      try {
        refreshSchemaOutput =
            refreshSchemaActivity.refreshSchemaV2(new RefreshSchemaActivityInput(sourceId.get(), connectionId, syncInput.getWorkspaceId()));
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
              ? UUID.fromString("00000000-0000-0000-0000-000000000000")
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

  private boolean checkUseWorkloadApiFlag(final StandardSyncInput syncInput) {
    return workloadFeatureFlagActivity.useWorkloadApi(new WorkloadFeatureFlagActivity.Input(
        syncInput.getWorkspaceId()));
  }

  private boolean checkUseWorkloadOutputFlag(final StandardSyncInput syncInput) {
    return workloadFeatureFlagActivity.useOutputDocStore(new WorkloadFeatureFlagActivity.Input(
        syncInput.getWorkspaceId()));
  }

}
