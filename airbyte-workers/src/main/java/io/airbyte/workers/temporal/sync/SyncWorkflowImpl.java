/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
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
import io.airbyte.config.NormalizationInput;
import io.airbyte.config.NormalizationSummary;
import io.airbyte.config.OperatorDbtInput;
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
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.models.RefreshSchemaActivityInput;
import io.airbyte.workers.models.RefreshSchemaActivityOutput;
import io.airbyte.workers.models.ReplicationActivityInput;
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
  private static final String USE_WORKLOAD_API_FF_CHECK_TAG = "use_workload_api_ff_check";
  private static final int USE_WORKLOAD_API_FF_CHECK_VERSION = 1;
  private static final String USE_WORKLOAD_OUTPUT_DOC_STORE_FF_CHECK_TAG = "use_workload_output_doc_store_ff_check";
  private static final int USE_WORKLOAD_OUTPUT_DOC_STORE_FF_CHECK_VERSION = 1;
  @TemporalActivityStub(activityOptionsBeanName = "longRunActivityOptions")
  private ReplicationActivity replicationActivity;
  @TemporalActivityStub(activityOptionsBeanName = "longRunActivityOptions")
  private NormalizationActivity normalizationActivity;
  @TemporalActivityStub(activityOptionsBeanName = "longRunActivityOptions")
  private DbtTransformationActivity dbtTransformationActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private NormalizationSummaryCheckActivity normalizationSummaryCheckActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private WebhookOperationActivity webhookOperationActivity;
  @TemporalActivityStub(activityOptionsBeanName = "refreshSchemaActivityOptions")
  private RefreshSchemaActivity refreshSchemaActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private ConfigFetchActivity configFetchActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private WorkloadFeatureFlagActivity workloadFeatureFlagActivity;

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public StandardSyncOutput run(final JobRunConfig jobRunConfig,
                                final IntegrationLauncherConfig sourceLauncherConfig,
                                final IntegrationLauncherConfig destinationLauncherConfig,
                                final StandardSyncInput syncInput,
                                final UUID connectionId) {

    // TODO: Remove this once Workload API rolled out
    final var useWorkloadApi = checkUseWorkloadApiFlag(syncInput);
    final var useWorkloadOutputDocStore = checkUseWorkloadOutputFlag(syncInput);

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
    if (!sourceId.isEmpty() && refreshSchemaActivity.shouldRefreshSchema(sourceId.get())) {
      LOGGER.info("Refreshing source schema...");
      try {
        final var version = Workflow.getVersion("AUTO_BACKFILL_ON_NEW_COLUMNS", Workflow.DEFAULT_VERSION, 1);
        if (version == Workflow.DEFAULT_VERSION) {
          refreshSchemaActivity.refreshSchema(sourceId.get(), connectionId);
        } else {
          refreshSchemaOutput =
              refreshSchemaActivity.refreshSchemaV2(new RefreshSchemaActivityInput(sourceId.get(), connectionId, syncInput.getWorkspaceId()));
        }
      } catch (final Exception e) {
        ApmTraceUtils.addExceptionToTrace(e);
        return SyncOutputProvider.getRefreshSchemaFailure(e);
      }
    }

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
        if (standardSyncOperation.getOperatorType() == OperatorType.NORMALIZATION) {
          if (destinationLauncherConfig.getNormalizationDockerImage() == null
              || destinationLauncherConfig.getNormalizationIntegrationType() == null) {
            // In the case that this connection used to run normalization but the destination no longer supports
            // it (destinations v1 -> v2)
            LOGGER.info("Not Running Normalization Container for connection {}, attempt id {}, because destination no longer supports normalization",
                connectionId, jobRunConfig.getAttemptId());
          } else if (syncInput.getNormalizeInDestinationContainer()) {
            LOGGER.info("Not Running Normalization Container for connection {}, attempt id {}, because it ran in destination",
                connectionId, jobRunConfig.getAttemptId());
          } else {
            final NormalizationInput normalizationInput = generateNormalizationInput(syncInput, syncOutput);
            final NormalizationSummary normalizationSummary =
                normalizationActivity.normalize(jobRunConfig, destinationLauncherConfig, normalizationInput);
            syncOutput = syncOutput.withNormalizationSummary(normalizationSummary);
            MetricClientFactory.getMetricClient().count(OssMetricsRegistry.NORMALIZATION_IN_NORMALIZATION_CONTAINER, 1,
                new MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()));
          }
        } else if (standardSyncOperation.getOperatorType() == OperatorType.DBT) {
          final OperatorDbtInput operatorDbtInput = new OperatorDbtInput()
              .withConnectionId(syncInput.getConnectionId())
              .withWorkspaceId(syncInput.getWorkspaceId())
              .withDestinationConfiguration(syncInput.getDestinationConfiguration())
              .withOperatorDbt(standardSyncOperation.getOperatorDbt())
              .withConnectionContext(syncInput.getConnectionContext());

          dbtTransformationActivity.run(jobRunConfig, destinationLauncherConfig,
              syncInput.getSyncResourceRequirements() != null ? syncInput.getSyncResourceRequirements().getOrchestrator() : null,
              operatorDbtInput);
        } else if (standardSyncOperation.getOperatorType() == OperatorType.WEBHOOK) {
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
          final String message = String.format("Unsupported operation type: %s", standardSyncOperation.getOperatorType());
          LOGGER.error(message);
          throw new IllegalArgumentException(message);
        }
      }
    }

    return syncOutput;
  }

  private NormalizationInput generateNormalizationInput(final StandardSyncInput syncInput,
                                                        final StandardSyncOutput syncOutput) {
    return normalizationActivity.generateNormalizationInputWithMinimumPayloadWithConnectionId(syncInput.getDestinationConfiguration(),
        syncOutput.getOutputCatalog(),
        syncInput.getWorkspaceId(),
        syncInput.getConnectionId(),
        syncInput.getConnectionContext().getOrganizationId());
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
        syncInput.getNormalizeInDestinationContainer(),
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
    final int version = Workflow.getVersion(USE_WORKLOAD_API_FF_CHECK_TAG, Workflow.DEFAULT_VERSION, USE_WORKLOAD_API_FF_CHECK_VERSION);
    final boolean shouldCheckFlag = version >= USE_WORKLOAD_API_FF_CHECK_VERSION;

    if (!shouldCheckFlag) {
      return false;
    }

    return workloadFeatureFlagActivity.useWorkloadApi(new WorkloadFeatureFlagActivity.Input(
        syncInput.getWorkspaceId(),
        syncInput.getConnectionId(),
        syncInput.getConnectionContext().getOrganizationId()));
  }

  private boolean checkUseWorkloadOutputFlag(final StandardSyncInput syncInput) {
    final int version =
        Workflow.getVersion(USE_WORKLOAD_OUTPUT_DOC_STORE_FF_CHECK_TAG, Workflow.DEFAULT_VERSION, USE_WORKLOAD_OUTPUT_DOC_STORE_FF_CHECK_VERSION);
    final boolean shouldCheckFlag = version >= USE_WORKLOAD_OUTPUT_DOC_STORE_FF_CHECK_VERSION;

    if (!shouldCheckFlag) {
      return false;
    }

    return workloadFeatureFlagActivity.useOutputDocStore(new WorkloadFeatureFlagActivity.Input(
        syncInput.getWorkspaceId(),
        syncInput.getConnectionId(),
        syncInput.getConnectionContext().getOrganizationId()));
  }

}
