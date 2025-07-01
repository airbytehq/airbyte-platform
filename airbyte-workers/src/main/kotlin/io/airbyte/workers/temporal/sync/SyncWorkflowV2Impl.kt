/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.commons.temporal.TemporalJobType
import io.airbyte.commons.temporal.TemporalTaskQueueUtils.getTaskQueue
import io.airbyte.commons.temporal.annotations.TemporalActivityStub
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.commons.temporal.scheduling.DiscoverCommandApiInput
import io.airbyte.commons.temporal.scheduling.ReplicationCommandApiInput
import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2
import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2Input
import io.airbyte.config.CatalogDiff
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.SyncStats
import io.airbyte.config.helpers.log
import io.airbyte.workers.models.PostprocessCatalogInput
import io.airbyte.workers.models.PostprocessCatalogOutput
import io.airbyte.workers.temporal.activities.GetConnectionContextInput
import io.airbyte.workers.temporal.activities.GetWebhookConfigInput
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogHelperActivity
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity
import io.temporal.api.enums.v1.ParentClosePolicy
import io.temporal.workflow.ChildWorkflowOptions
import io.temporal.workflow.Workflow
import java.util.UUID

open class SyncWorkflowV2Impl : SyncWorkflowV2 {
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private lateinit var configFetchActivity: ConfigFetchActivity

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private lateinit var discoverCatalogHelperActivity: DiscoverCatalogHelperActivity

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private lateinit var invokeOperationsActivity: InvokeOperationsActivity

  constructor() {}

  @VisibleForTesting
  constructor(
    configFetchActivity: ConfigFetchActivity,
    discoverCatalogHelperActivity: DiscoverCatalogHelperActivity,
    invokeOperationsActivity: InvokeOperationsActivity,
  ) {
    this.configFetchActivity = configFetchActivity
    this.discoverCatalogHelperActivity = discoverCatalogHelperActivity
    this.invokeOperationsActivity = invokeOperationsActivity
  }

  override fun run(input: SyncWorkflowV2Input): StandardSyncOutput {
    val discoverOutput =
      runDiscoverCommand(
        actorId = input.sourceId,
        jobId = input.jobId.toString(),
        attemptId = input.attemptNumber,
      )

    val postprocessCatalogOutput: PostprocessCatalogOutput =
      discoverCatalogHelperActivity
        .postprocess(PostprocessCatalogInput(discoverOutput.discoverCatalogId, input.connectionId))

    val status = configFetchActivity.getStatus(input.connectionId)
    if (status.isPresent && ConnectionStatus.INACTIVE == status.get()) {
      log.info { "Connection ${input.connectionId} is disabled. Cancelling run." }
      return StandardSyncOutput()
        .withStandardSyncSummary(StandardSyncSummary().withStatus(StandardSyncSummary.ReplicationStatus.CANCELLED).withTotalStats(SyncStats()))
    }

    val output: ConnectorJobOutput =
      runReplicationCommand(
        connectionId = input.connectionId,
        jobId = input.jobId.toString(),
        attemptNumber = input.attemptNumber,
        appliedCatalogDiff = postprocessCatalogOutput.diff,
      )

    // TODO: send the metrics

    val connectionContext = configFetchActivity.getConnectionContext(GetConnectionContextInput(input.connectionId)).connectionContext
    val webhookConfig = configFetchActivity.getWebhookConfig(GetWebhookConfigInput(input.jobId))
    val webhookOperationSummary =
      invokeOperationsActivity.invokeOperationsV2(
        operations = webhookConfig.operations,
        webhookOperationConfigs = webhookConfig.webhookOperationConfigs,
        connectionContext = connectionContext,
        jobId = input.jobId.toString(),
        attemptId = input.attemptNumber,
      )
    output.replicate.webhookOperationSummary = webhookOperationSummary

    return output.replicate
  }

  @VisibleForTesting
  internal fun runDiscoverCommand(
    actorId: UUID,
    jobId: String,
    attemptId: Long,
  ): ConnectorJobOutput {
    val workflowId = "discover_${jobId}_$attemptId"
    val taskQueue = getTaskQueue(TemporalJobType.SYNC)
    val childCheck =
      Workflow.newChildWorkflowStub(
        ConnectorCommandWorkflow::class.java,
        ChildWorkflowOptions
          .newBuilder()
          .setWorkflowId(workflowId)
          .setTaskQueue(taskQueue) // This will cancel the child workflow when the parent is terminated
          .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL)
          .build(),
      )

    return childCheck.run(
      DiscoverCommandApiInput(
        DiscoverCommandApiInput.DiscoverApiInput(
          actorId = actorId,
          jobId = jobId,
          attemptNumber = attemptId,
        ),
      ),
    )
  }

  @VisibleForTesting
  internal fun runReplicationCommand(
    connectionId: UUID,
    jobId: String,
    attemptNumber: Long,
    appliedCatalogDiff: CatalogDiff?,
  ): ConnectorJobOutput {
    val workflowId = "replication_$jobId"
    val taskQueue = getTaskQueue(TemporalJobType.SYNC)
    val childCheck =
      Workflow.newChildWorkflowStub(
        ConnectorCommandWorkflow::class.java,
        ChildWorkflowOptions
          .newBuilder()
          .setWorkflowId(workflowId)
          .setTaskQueue(taskQueue) // This will cancel the child workflow when the parent is terminated
          .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL)
          .build(),
      )

    return childCheck.run(
      ReplicationCommandApiInput(
        ReplicationCommandApiInput.ReplicationApiInput(
          connectionId = connectionId,
          jobId = jobId,
          attemptId = attemptNumber,
          appliedCatalogDiff = appliedCatalogDiff,
        ),
      ),
    )
  }
}
