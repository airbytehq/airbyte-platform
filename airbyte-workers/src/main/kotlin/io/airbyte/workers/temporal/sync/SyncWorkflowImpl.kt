/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import com.google.common.hash.Hashing
import datadog.trace.api.Trace
import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.commons.helper.DockerImageName.extractTag
import io.airbyte.commons.json.Jsons.serialize
import io.airbyte.commons.temporal.annotations.TemporalActivityStub
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.commons.temporal.scheduling.SyncWorkflow
import io.airbyte.config.ActorContext
import io.airbyte.config.ActorType
import io.airbyte.config.SignalInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.SyncStats
import io.airbyte.config.WorkloadPriority
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.DESTINATION_DOCKER_IMAGE_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.JOB_ID_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.SOURCE_DOCKER_IMAGE_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.ApmTraceUtils.addExceptionToTrace
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.PostprocessCatalogInput
import io.airbyte.workers.models.RefreshSchemaActivityOutput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.temporal.activities.ReportRunTimeActivityInput
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogHelperActivity
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity
import io.airbyte.workers.temporal.sync.GenerateReplicationActivityInputActivity.Companion.toReplicationActivityInput
import io.temporal.failure.ActivityFailure
import io.temporal.failure.CanceledFailure
import io.temporal.workflow.ChildWorkflowOptions
import io.temporal.workflow.Workflow
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Optional
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Sync temporal workflow impl.
 */
open class SyncWorkflowImpl : SyncWorkflow {
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val configFetchActivity: ConfigFetchActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val generateReplicationActivityInputActivity: GenerateReplicationActivityInputActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val reportRunTimeActivity: ReportRunTimeActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val invokeOperationsActivity: InvokeOperationsActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "asyncActivityOptions")
  private val asyncReplicationActivity: AsyncReplicationActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "workloadStatusCheckActivityOptions")
  private val workloadStatusCheckActivity: WorkloadStatusCheckActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val discoverCatalogHelperActivity: DiscoverCatalogHelperActivity? = null

  private var shouldBlock: Boolean? = null

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  override fun checkAsyncActivityStatus() {
    this.shouldBlock = false
  }

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  override fun run(
    jobRunConfig: JobRunConfig,
    sourceLauncherConfig: IntegrationLauncherConfig,
    destinationLauncherConfig: IntegrationLauncherConfig,
    syncInput: StandardSyncInput,
    connectionId: UUID,
  ): StandardSyncOutput {
    val startTime = Workflow.currentTimeMillis()
    // TODO: Remove this once Workload API rolled out
    val sendRunTimeMetrics = shouldReportRuntime()

    ApmTraceUtils
      .addTagsToTrace(
        mapOf(
          ATTEMPT_NUMBER_KEY to jobRunConfig.attemptId,
          CONNECTION_ID_KEY to connectionId.toString(),
          JOB_ID_KEY to jobRunConfig.jobId,
          SOURCE_DOCKER_IMAGE_KEY to sourceLauncherConfig.dockerImage,
          DESTINATION_DOCKER_IMAGE_KEY to destinationLauncherConfig.dockerImage,
        ),
      )

    val taskQueue = Workflow.getInfo().taskQueue

    val sourceId = getSourceId(syncInput)
    val refreshSchemaOutput: RefreshSchemaActivityOutput

    try {
      val sourceConfig = configFetchActivity!!.getSourceConfig(sourceId.get())
      refreshSchemaOutput = runDiscoverAsChildWorkflow(jobRunConfig, sourceLauncherConfig, syncInput, sourceConfig)
    } catch (e: Exception) {
      addExceptionToTrace(e)
      return SyncOutputProvider.getRefreshSchemaFailure(e)
    }

    val discoverSchemaEndTime = Workflow.currentTimeMillis()

    val status = configFetchActivity.getStatus(connectionId)
    if (status.isPresent && ConnectionStatus.INACTIVE == status.get()) {
      LOGGER.info("Connection {} is disabled. Cancelling run.", connectionId)
      return StandardSyncOutput()
        .withStandardSyncSummary(
          StandardSyncSummary().withStatus(StandardSyncSummary.ReplicationStatus.CANCELLED).withTotalStats(SyncStats()),
        )
    }

    val replicationActivityInput =
      generateReplicationActivityInput(
        syncInput,
        jobRunConfig,
        sourceLauncherConfig,
        destinationLauncherConfig,
        taskQueue,
        refreshSchemaOutput,
      )
    val syncOutput: StandardSyncOutput

    val workloadId = asyncReplicationActivity!!.startReplication(replicationActivityInput)

    try {
      shouldBlock = !workloadStatusCheckActivity!!.isTerminal(workloadId)
      while (shouldBlock == true) {
        Workflow.await(Duration.ofMinutes(15)) { !shouldBlock!! }
        shouldBlock = !workloadStatusCheckActivity.isTerminal(workloadId)
      }
    } catch (cf: CanceledFailure) {
      if (workloadId != null) {
        // This is in order to be usable from the detached scope
        val detached =
          Workflow.newDetachedCancellationScope {
            asyncReplicationActivity.cancel(replicationActivityInput, workloadId)
            shouldBlock = false
          }
        detached.run()
      }
      throw cf
    } catch (cf: ActivityFailure) {
      if (workloadId != null) {
        val detached =
          Workflow.newDetachedCancellationScope {
            asyncReplicationActivity.cancel(replicationActivityInput, workloadId)
            shouldBlock = false
          }
        detached.run()
      }
      throw cf
    }

    syncOutput = asyncReplicationActivity.getReplicationOutput(replicationActivityInput, workloadId)

    val webhookOperationSummary =
      invokeOperationsActivity!!.invokeOperations(
        syncInput.operationSequence,
        syncInput,
        jobRunConfig,
      )
    syncOutput.webhookOperationSummary = webhookOperationSummary

    val replicationEndTime = Workflow.currentTimeMillis()

    if (sendRunTimeMetrics) {
      reportRunTimeActivity!!.reportRunTime(
        ReportRunTimeActivityInput(
          connectionId,
          if (syncInput.connectionContext == null || syncInput.connectionContext.sourceDefinitionId == null) {
            DEFAULT_UUID
          } else {
            syncInput.connectionContext.sourceDefinitionId
          },
          startTime,
          discoverSchemaEndTime,
          replicationEndTime,
        ),
      )
    }

    if (syncOutput.standardSyncSummary != null && syncOutput.standardSyncSummary.totalStats != null) {
      syncOutput.standardSyncSummary.totalStats.discoverSchemaEndTime = discoverSchemaEndTime
      syncOutput.standardSyncSummary.totalStats.discoverSchemaStartTime = startTime
    }

    return syncOutput
  }

  private fun getSourceId(syncInput: StandardSyncInput): Optional<UUID> {
    val shouldGetSourceFromSyncInput = Workflow.getVersion("SHOULD_GET_SOURCE_FROM_SYNC_INPUT", Workflow.DEFAULT_VERSION, 1)
    if (shouldGetSourceFromSyncInput != Workflow.DEFAULT_VERSION) {
      return Optional.ofNullable<UUID>(syncInput.sourceId)
    }
    return configFetchActivity!!.getSourceId(syncInput.connectionId)
  }

  @VisibleForTesting
  fun runDiscoverAsChildWorkflow(
    jobRunConfig: JobRunConfig,
    sourceLauncherConfig: IntegrationLauncherConfig,
    syncInput: StandardSyncInput,
    sourceConfig: JsonNode?,
  ): RefreshSchemaActivityOutput {
    try {
      val discoverCatalogInput =
        StandardDiscoverCatalogInput()
          .withActorContext(
            ActorContext()
              .withActorDefinitionId(syncInput.connectionContext.sourceDefinitionId)
              .withActorType(ActorType.SOURCE)
              .withActorId(syncInput.sourceId)
              .withWorkspaceId(syncInput.workspaceId)
              .withOrganizationId(syncInput.connectionContext.organizationId),
          ).withConnectionConfiguration(syncInput.sourceConfiguration)
          .withSourceId(syncInput.sourceId.toString())
          .withConfigHash(
            HASH_FUNCTION
              .hashBytes(
                serialize<JsonNode?>(sourceConfig).toByteArray(
                  StandardCharsets.UTF_8,
                ),
              ).toString(),
          ).withConnectorVersion(extractTag(sourceLauncherConfig.dockerImage))
          .withManual(false)
      val childDiscoverWorkflow =
        Workflow.newChildWorkflowStub<ConnectorCommandWorkflow>(
          ConnectorCommandWorkflow::class.java,
          ChildWorkflowOptions
            .newBuilder()
            .setWorkflowId("discover_" + jobRunConfig.jobId + "_" + jobRunConfig.attemptId)
            .build(),
        )
      val discoverOutput =
        childDiscoverWorkflow.run(
          DiscoverCommandInput(
            DiscoverCommandInput.DiscoverCatalogInput(
              jobRunConfig,
              sourceLauncherConfig.withPriority(WorkloadPriority.DEFAULT),
              discoverCatalogInput,
            ),
          ),
        )

      val postprocessCatalogOutput =
        discoverCatalogHelperActivity!!
          .postprocess(PostprocessCatalogInput(discoverOutput.discoverCatalogId, sourceLauncherConfig.connectionId))
      return RefreshSchemaActivityOutput(postprocessCatalogOutput.diff)
    } catch (e: Exception) {
      LOGGER.error("error", e)
      throw RuntimeException(e)
    }
  }

  private fun shouldReportRuntime(): Boolean {
    val shouldReportRuntimeVersion = Workflow.getVersion("SHOULD_REPORT_RUNTIME", Workflow.DEFAULT_VERSION, 1)

    return shouldReportRuntimeVersion != Workflow.DEFAULT_VERSION
  }

  private fun generateReplicationActivityInput(
    syncInput: StandardSyncInput,
    jobRunConfig: JobRunConfig,
    sourceLauncherConfig: IntegrationLauncherConfig,
    destinationLauncherConfig: IntegrationLauncherConfig,
    taskQueue: String,
    refreshSchemaOutput: RefreshSchemaActivityOutput,
  ): ReplicationActivityInput {
    val signalInput: String?
    if (syncInput.useAsyncReplicate != null && syncInput.useAsyncReplicate) {
      signalInput = serialize<SignalInput?>(SignalInput(SignalInput.SYNC_WORKFLOW, Workflow.getInfo().workflowId))
    } else {
      signalInput = null
    }
    val version =
      Workflow.getVersion(
        GENERATE_REPLICATION_ACTIVITY_INPUT_ACTIVITY,
        Workflow.DEFAULT_VERSION,
        GENERATE_REPLICATION_ACTIVITY_INPUT_ACTIVITY_VERSION,
      )
    if (version == Workflow.DEFAULT_VERSION) {
      return toReplicationActivityInput(
        syncInput,
        jobRunConfig,
        sourceLauncherConfig,
        destinationLauncherConfig,
        taskQueue,
        refreshSchemaOutput,
        signalInput,
        mapOf(),
        TimeUnit.HOURS.toSeconds(24),
        false,
        null,
        null,
      )
    } else {
      return generateReplicationActivityInputActivity!!.generate(
        syncInput,
        jobRunConfig,
        sourceLauncherConfig,
        destinationLauncherConfig,
        taskQueue,
        refreshSchemaOutput,
        signalInput,
      )
    }
  }

  companion object {
    private val LOGGER: Logger = LoggerFactory.getLogger(SyncWorkflowImpl::class.java)

    private val DEFAULT_UUID: UUID = UUID.fromString("00000000-0000-0000-0000-000000000000")

    private val HASH_FUNCTION = Hashing.md5()

    private const val GENERATE_REPLICATION_ACTIVITY_INPUT_ACTIVITY = "generate_replication_activity_input_activity"
    private const val GENERATE_REPLICATION_ACTIVITY_INPUT_ACTIVITY_VERSION = 1
  }
}
