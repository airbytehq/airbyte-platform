/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.temporal

import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.temporal.config.TemporalQueueConfiguration
import io.airbyte.commons.temporal.exception.DeletedWorkflowException
import io.airbyte.commons.temporal.exception.UnreachableWorkflowException
import io.airbyte.commons.temporal.scheduling.CheckCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow
import io.airbyte.commons.temporal.scheduling.ConnectorCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.commons.temporal.scheduling.SpecCommandInput
import io.airbyte.commons.temporal.scheduling.state.WorkflowState
import io.airbyte.config.ActorContext
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.JobCheckConnectionConfig
import io.airbyte.config.JobDiscoverCatalogConfig
import io.airbyte.config.JobGetSpecConfig
import io.airbyte.config.RefreshStream
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.persistence.StreamRefreshesRepository
import io.airbyte.config.persistence.StreamResetPersistence
import io.airbyte.config.persistence.saveStreamsToRefresh
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.shared.NetworkSecurityTokenKey
import io.airbyte.featureflag.ANONYMOUS
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.api.common.v1.WorkflowType
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.workflowservice.v1.ListClosedWorkflowExecutionsRequest
import io.temporal.api.workflowservice.v1.ListOpenWorkflowExecutionsRequest
import io.temporal.client.WorkflowOptions
import io.temporal.common.RetryOptions
import io.temporal.workflow.Functions
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.apache.commons.lang3.time.StopWatch
import java.io.IOException
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.function.Consumer
import java.util.function.Supplier
import kotlin.jvm.optionals.getOrNull

private const val SAFE_TERMINATE_MESSAGE = "Terminating workflow in unreachable state before starting a new workflow for this connection"

/**
 * This is used to sleep between 2 temporal queries. The query is needed to ensure that the cancel
 * and start manual sync methods wait before returning. Since temporal signals are async, we need to
 * use the queries to make sure that we are in a state in which we want to continue with.
 */
private const val DELAY_BETWEEN_QUERY_MS = 10
private val logger = KotlinLogging.logger { }

/**
 * Result of a manual operation.
 */
data class ManualOperationResult(
  val failingReason: String? = null,
  val jobId: Long? = null,
  val errorCode: ErrorCode? = null,
)

/**
 * Airbyte's interface over temporal.
 */
@Singleton
class TemporalClient(
  @param:Named("workspaceRootTemporal") private val workspaceRoot: Path,
  private val queueConfiguration: TemporalQueueConfiguration,
  private val workflowClientWrapped: WorkflowClientWrapped,
  private val serviceStubsWrapped: WorkflowServiceStubsWrapped,
  private val streamResetPersistence: StreamResetPersistence,
  private val streamRefreshesRepository: StreamRefreshesRepository,
  private val connectionManagerUtils: ConnectionManagerUtils,
  private val streamResetRecordsHelper: StreamResetRecordsHelper,
  private val metricClient: MetricClient,
  private val featureFlagClient: FeatureFlagClient,
  private val scopedConfigurationService: ScopedConfigurationService,
) {
  private val workflowNames = mutableSetOf<String>()

  /**
   * Restart workflows stuck in a certain status.
   *
   * @param executionStatus execution status
   * @return set of connection ids that were restarted, primarily used for tracking purposes
   */
  fun restartClosedWorkflowByStatus(executionStatus: WorkflowExecutionStatus): Int {
    val workflowExecutionInfos = fetchClosedWorkflowsByStatus(executionStatus)
    val nonRunningWorkflow = filterOutRunningWorkspaceId(workflowExecutionInfos)

    nonRunningWorkflow.forEach { connectionId ->
      with(connectionManagerUtils) {
        safeTerminateWorkflow(connectionId, SAFE_TERMINATE_MESSAGE)
        startConnectionManagerNoSignal(connectionId)
      }
    }

    return nonRunningWorkflow.size
  }

  fun fetchClosedWorkflowsByStatus(executionStatus: WorkflowExecutionStatus): MutableSet<UUID> {
    var workflowExecutionsRequest =
      ListClosedWorkflowExecutionsRequest
        .newBuilder()
        .setNamespace(workflowClientWrapped.getNamespace())
        .build()

    val workflowExecutionInfos = mutableSetOf<UUID>()
    do {
      val listOpenWorkflowExecutionsRequest = serviceStubsWrapped.blockingStubListClosedWorkflowExecutions(workflowExecutionsRequest)
      val connectionManagerWorkflowType = WorkflowType.newBuilder().setName(ConnectionManagerWorkflow::class.java.getSimpleName()).build()

      listOpenWorkflowExecutionsRequest
        .executionsList
        .filterNotNull()
        .filter { it.type == connectionManagerWorkflowType || it.status == executionStatus }
        .mapNotNull { extractConnectionIdFromWorkflowId(it.execution.workflowId) }
        .toSet()
        .also {
          workflowExecutionInfos.addAll(it)
        }

      val token: ByteString? = listOpenWorkflowExecutionsRequest.nextPageToken

      workflowExecutionsRequest =
        ListClosedWorkflowExecutionsRequest
          .newBuilder()
          .setNamespace(workflowClientWrapped.namespace)
          .setNextPageToken(token)
          .build()
    } while (token != null && token.size() > 0)

    return workflowExecutionInfos
  }

  // once tests have been migrated to kotlin, mark internal
  @InternalForTesting
  fun filterOutRunningWorkspaceId(workflowIds: MutableSet<UUID>): Set<UUID> {
    refreshRunningWorkflow()

    val runningWorkflowByUUID =
      workflowNames
        .mapNotNull { extractConnectionIdFromWorkflowId(it) }
        .toSet()

    return workflowIds - runningWorkflowByUUID
  }

  // once tests have been migrated to kotlin, mark internal
  @InternalForTesting
  fun refreshRunningWorkflow() {
    workflowNames.clear()

    var openWorkflowExecutionsRequest =
      ListOpenWorkflowExecutionsRequest
        .newBuilder()
        .setNamespace(workflowClientWrapped.namespace)
        .build()

    do {
      val listOpenWorkflowExecutionsRequest = serviceStubsWrapped.blockingStubListOpenWorkflowExecutions(openWorkflowExecutionsRequest)
      listOpenWorkflowExecutionsRequest.executionsList
        .map { it.execution.workflowId }
        .toSet()
        .also { workflowNames.addAll(it) }

      val token: ByteString? = listOpenWorkflowExecutionsRequest.nextPageToken

      openWorkflowExecutionsRequest =
        ListOpenWorkflowExecutionsRequest
          .newBuilder()
          .setNamespace(workflowClientWrapped.namespace)
          .setNextPageToken(token)
          .build()
    } while (token != null && token.size() > 0)
  }

  private fun extractConnectionIdFromWorkflowId(workflowId: String): UUID? =
    when {
      workflowId.startsWith("connection_manager_") -> {
        workflowId.removePrefix("connection_manager_").let { UUID.fromString(it) }
      }

      else -> null
    }

  fun getWorkflowState(connectionId: UUID): WorkflowState? = connectionManagerUtils.getWorkflowState(connectionId).getOrNull()

  /**
   * Start a manual sync for a connection.
   *
   * @param connectionId connection id
   * @return sync result
   */
  fun startNewManualSync(connectionId: UUID?): ManualOperationResult {
    logger.info { "Manual sync request" }

    if (connectionManagerUtils.isWorkflowStateRunning(connectionId)) {
      // TODO Bmoric: Error is running
      return ManualOperationResult(
        failingReason = "A sync is already running for: $connectionId",
        errorCode = ErrorCode.WORKFLOW_RUNNING,
      )
    }

    try {
      connectionManagerUtils.signalWorkflowAndRepairIfNecessary(connectionId) {
        Functions.Proc { it.submitManualSync() }
      }
    } catch (e: DeletedWorkflowException) {
      logger.error(e) { "Can't sync a deleted connection." }
      return ManualOperationResult(
        failingReason = e.message,
        errorCode = ErrorCode.WORKFLOW_DELETED,
      )
    }

    do {
      try {
        Thread.sleep(DELAY_BETWEEN_QUERY_MS.toLong())
      } catch (e: InterruptedException) {
        return ManualOperationResult(
          failingReason = "Didn't managed to start a sync for: $connectionId",
          errorCode = ErrorCode.UNKNOWN,
        )
      }
    } while (!connectionManagerUtils.isWorkflowStateRunning(connectionId))

    logger.info { "end of manual schedule" }
    return ManualOperationResult(jobId = connectionManagerUtils.getCurrentJobId(connectionId))
  }

  /**
   * Cancel a running job for a connection.
   *
   * @param connectionId connection id
   * @return cancellation result
   */
  fun startNewCancellation(connectionId: UUID?): ManualOperationResult {
    logger.info { "Manual cancellation request" }

    val jobId = connectionManagerUtils.getCurrentJobId(connectionId)

    try {
      connectionManagerUtils.signalWorkflowAndRepairIfNecessary(connectionId) {
        Functions.Proc { it.cancelJob() }
      }
    } catch (e: DeletedWorkflowException) {
      logger.error(e) { "Can't cancel a deleted workflow" }
      return ManualOperationResult(
        failingReason = e.message,
        errorCode = ErrorCode.WORKFLOW_DELETED,
      )
    }

    do {
      try {
        Thread.sleep(DELAY_BETWEEN_QUERY_MS.toLong())
      } catch (e: InterruptedException) {
        return ManualOperationResult(
          failingReason = "Didn't manage to cancel a sync for: $connectionId",
          errorCode = ErrorCode.UNKNOWN,
        )
      }
    } while (connectionManagerUtils.isWorkflowStateRunning(connectionId))

    streamResetRecordsHelper.deleteStreamResetRecordsForJob(jobId, connectionId)

    logger.info { "end of manual cancellation" }

    return ManualOperationResult(jobId = jobId)
  }

  fun resetConnectionAsync(
    connectionId: UUID?,
    streamsToReset: MutableList<StreamDescriptor?>?,
  ) {
    try {
      streamResetPersistence.createStreamResets(connectionId, streamsToReset)
      connectionManagerUtils.signalWorkflowAndRepairIfNecessary(connectionId) {
        Functions.Proc { it.resetConnection() }
      }
    } catch (e: IOException) {
      logger.error { "Not able to properly create a reset" }
      throw RuntimeException(e)
    } catch (e: DeletedWorkflowException) {
      logger.error { "Not able to properly create a reset" }
      throw RuntimeException(e)
    }
  }

  fun refreshConnectionAsync(
    connectionId: UUID,
    streamsToRefresh: List<StreamDescriptor>,
    refreshType: RefreshStream.RefreshType,
  ) {
    try {
      streamRefreshesRepository.saveStreamsToRefresh(connectionId, streamsToRefresh, refreshType)
      // This isn't actually doing a reset. workflow::resetConnection will cancel the current run if any
      // and cause the workflow to run immediately. The next run will be a refresh because we just saved a
      // refresh configuration.
      connectionManagerUtils.signalWorkflowAndRepairIfNecessary(connectionId) {
        Functions.Proc { it.resetConnection() }
      }
    } catch (e: DeletedWorkflowException) {
      logger.error { "Not able to properly create a reset" }
      throw RuntimeException(e)
    }
  }

  /**
   * Submit a reset connection job to temporal.
   *
   * @param connectionId connection id
   * @param streamsToReset streams that should be rest on the connection
   * @return result of reset connection
   */
  fun resetConnection(
    connectionId: UUID,
    streamsToReset: List<StreamDescriptor>,
  ): ManualOperationResult {
    logger.info { "reset sync request" }

    try {
      streamResetPersistence.createStreamResets(connectionId, streamsToReset)
    } catch (e: IOException) {
      logger.error(e) { "Could not persist streams to reset." }
      return ManualOperationResult(failingReason = e.message, errorCode = ErrorCode.UNKNOWN)
    }

    // get the job ID before the reset, defaulting to NON_RUNNING_JOB_ID if workflow is unreachable
    val oldJobId = connectionManagerUtils.getCurrentJobId(connectionId)

    try {
      connectionManagerUtils.signalWorkflowAndRepairIfNecessary(connectionId) {
        Functions.Proc { it.resetConnection() }
      }
    } catch (e: DeletedWorkflowException) {
      logger.error(e) { "Can't reset a deleted workflow" }
      return ManualOperationResult(failingReason = e.message, errorCode = ErrorCode.UNKNOWN)
    }

    var newJobId: Long?

    do {
      try {
        Thread.sleep(DELAY_BETWEEN_QUERY_MS.toLong())
      } catch (_: InterruptedException) {
        return ManualOperationResult(failingReason = "Didn't manage to reset a sync for: $connectionId", errorCode = ErrorCode.UNKNOWN)
      }

      newJobId = getNewJobId(connectionId, oldJobId)
    } while (newJobId == null)

    logger.info { "end of reset submission" }

    return ManualOperationResult(jobId = newJobId)
  }

  private fun getNewJobId(
    connectionId: UUID?,
    oldJobId: Long,
  ): Long? {
    val currentJobId =
      connectionManagerUtils.getCurrentJobId(connectionId)
    if (currentJobId == ConnectionManagerWorkflow.NON_RUNNING_JOB_ID || currentJobId == oldJobId) {
      return null
    } else {
      return currentJobId
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
  fun submitGetSpec(
    jobId: UUID,
    attempt: Int,
    workspaceId: UUID?,
    config: JobGetSpecConfig,
  ): TemporalResponse<ConnectorJobOutput> {
    val jobRunConfig = TemporalWorkflowUtils.createJobRunConfig(jobId, attempt)
    // Since SPEC happens before a connector is created, it is expected for a SPEC job to not have a
    // workspace id unless it is a custom connector.
    //
    // This differs from CHECK/DISCOVER/REPLICATION which always have a workspace id thus requiring,
    // downstream FF checks to null check the workspace before adding the context or failing. Thus, we
    // default the workspace to simplify this process.
    val resolvedWorkspaceId = workspaceId ?: ANONYMOUS
    val launcherConfig =
      IntegrationLauncherConfig()
        .withJobId(jobId.toString())
        .withAttemptId(attempt.toLong())
        .withWorkspaceId(resolvedWorkspaceId)
        .withDockerImage(config.getDockerImage())
        .withIsCustomConnector(config.getIsCustomConnector())

    return executeConnectorCommandWorkflow(jobRunConfig, SpecCommandInput(SpecCommandInput.SpecInput(jobRunConfig, launcherConfig)))
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
  fun submitCheckConnection(
    jobId: UUID,
    attempt: Int,
    workspaceId: UUID,
    taskQueue: String?,
    config: JobCheckConnectionConfig,
    context: ActorContext?,
  ): TemporalResponse<ConnectorJobOutput> {
    val jobRunConfig = TemporalWorkflowUtils.createJobRunConfig(jobId, attempt)
    val launcherConfig =
      IntegrationLauncherConfig()
        .withJobId(jobId.toString())
        .withAttemptId(attempt.toLong())
        .withWorkspaceId(workspaceId)
        .withDockerImage(config.getDockerImage())
        .withProtocolVersion(config.getProtocolVersion())
        .withIsCustomConnector(config.getIsCustomConnector())
        .withPriority(WorkloadPriority.HIGH)

    val input =
      StandardCheckConnectionInput()
        .withActorType(config.getActorType())
        .withActorId(config.getActorId())
        .withConnectionConfiguration(config.getConnectionConfiguration())
        .withResourceRequirements(config.getResourceRequirements())
        .withActorContext(context)
        .withNetworkSecurityTokens(getNetworkSecurityTokens(workspaceId))

    return executeConnectorCommandWorkflow(
      jobRunConfig,
      CheckCommandInput(CheckCommandInput.CheckConnectionInput(jobRunConfig, launcherConfig, input)),
    )
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
  fun submitDiscoverSchema(
    jobId: UUID,
    attempt: Int,
    workspaceId: UUID,
    taskQueue: String?,
    config: JobDiscoverCatalogConfig,
    context: ActorContext?,
    priority: WorkloadPriority?,
  ): TemporalResponse<ConnectorJobOutput> {
    val jobRunConfig = TemporalWorkflowUtils.createJobRunConfig(jobId, attempt)
    val launcherConfig =
      IntegrationLauncherConfig()
        .withJobId(jobId.toString())
        .withAttemptId(attempt.toLong())
        .withWorkspaceId(workspaceId)
        .withDockerImage(config.getDockerImage())
        .withProtocolVersion(config.getProtocolVersion())
        .withIsCustomConnector(config.getIsCustomConnector())
        .withPriority(priority)
    val input =
      StandardDiscoverCatalogInput()
        .withConnectionConfiguration(config.getConnectionConfiguration())
        .withSourceId(config.getSourceId())
        .withConnectorVersion(config.getConnectorVersion())
        .withConfigHash(config.getConfigHash())
        .withResourceRequirements(config.getResourceRequirements())
        .withActorContext(context)
        .withManual(true)
        .withNetworkSecurityTokens(getNetworkSecurityTokens(workspaceId))

    return executeConnectorCommandWorkflow(
      jobRunConfig,
      DiscoverCommandInput(DiscoverCommandInput.DiscoverCatalogInput(jobRunConfig, launcherConfig, input)),
    )
  }

  /**
   * Run update to start connection manager workflows for connection ids.
   *
   * @param connectionIds connection ids
   * // todo (cgardens) - i dunno what this is
   */
  fun migrateSyncIfNeeded(connectionIds: MutableSet<UUID?>) {
    val globalMigrationWatch = StopWatch()
    globalMigrationWatch.start()
    refreshRunningWorkflow()

    connectionIds.forEach(
      Consumer { connectionId: UUID? ->
        val singleSyncMigrationWatch = StopWatch()
        singleSyncMigrationWatch.start()
        if (!isInRunningWorkflowCache(connectionManagerUtils.getConnectionManagerName(connectionId))) {
          logger.info { "Migrating: $connectionId" }
          try {
            submitConnectionUpdaterAsync(connectionId)
          } catch (e: Exception) {
            logger.error(e) { "New workflow submission failed, retrying" }
            refreshRunningWorkflow()
            submitConnectionUpdaterAsync(connectionId)
          }
        }
        singleSyncMigrationWatch.stop()
        logger.info { "Sync migration took: " + singleSyncMigrationWatch.formatTime() }
      },
    )
    globalMigrationWatch.stop()

    logger.info { "The migration to the new scheduler took: " + globalMigrationWatch.formatTime() }
  }

  @VisibleForTesting
  fun <T> execute(
    jobRunConfig: JobRunConfig,
    executor: Supplier<T>,
  ): TemporalResponse<T> {
    val jobRoot = TemporalUtils.getJobRoot(workspaceRoot, jobRunConfig.getJobId(), jobRunConfig.getAttemptId())
    val logPath = TemporalUtils.getLogPath(jobRoot)

    var operationOutput: T? = null
    var exception: RuntimeException? = null

    try {
      operationOutput = executor.get()
    } catch (e: RuntimeException) {
      exception = e
    }

    var succeeded = exception == null
    if (succeeded && operationOutput is ConnectorJobOutput) {
      succeeded = getConnectorJobSucceeded(operationOutput as ConnectorJobOutput)
    }

    val metadata = JobMetadata(succeeded, logPath)
    return TemporalResponse<T>(operationOutput, metadata)
  }

  private fun executeConnectorCommandWorkflow(
    jobRunConfig: JobRunConfig,
    input: ConnectorCommandInput,
  ): TemporalResponse<ConnectorJobOutput> {
    val workflowOptions =
      WorkflowOptions
        .newBuilder()
        .setTaskQueue(queueConfiguration.uiCommandsQueue)
        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(1).build())
        .setWorkflowId(String.format("%s_%s", input.type, jobRunConfig.getJobId()))
        .build()

    return execute<ConnectorJobOutput>(jobRunConfig) {
      workflowClientWrapped
        .newWorkflowStub<ConnectorCommandWorkflow>(ConnectorCommandWorkflow::class.java, workflowOptions)
        .run(input)
    }
  }

  private fun <T> getWorkflowStub(
    workflowClass: Class<T>,
    jobType: TemporalJobType,
    jobId: UUID?,
  ): T = workflowClientWrapped.newWorkflowStub<T>(workflowClass, TemporalWorkflowUtils.buildWorkflowOptions(jobType, jobId))

  private fun <T> getWorkflowStubWithTaskQueue(
    workflowClass: Class<T>,
    taskQueue: String?,
    jobId: UUID,
  ): T = workflowClientWrapped.newWorkflowStub<T>(workflowClass, TemporalWorkflowUtils.buildWorkflowOptionsWithTaskQueue(taskQueue, jobId))

  /**
   * Signal to the connection manager workflow asynchronously that there has been a change to the
   * connection's configuration.
   *
   * @param connectionId connection id
   */
  fun submitConnectionUpdaterAsync(connectionId: UUID?): ConnectionManagerWorkflow? {
    logger.info { "Starting the scheduler temporal wf" }
    val connectionManagerWorkflow = connectionManagerUtils.startConnectionManagerNoSignal(connectionId)

    try {
      CompletableFuture
        .supplyAsync {
          try {
            do {
              Thread.sleep(DELAY_BETWEEN_QUERY_MS.toLong())
            } while (!isWorkflowReachable(connectionId))
          } catch (_: InterruptedException) {
            // no op
          }
          null
        }.get(60, TimeUnit.SECONDS)
    } catch (e: InterruptedException) {
      logger.error(e) { "Failed to create a new connection manager workflow" }
    } catch (e: ExecutionException) {
      logger.error(e) { "Failed to create a new connection manager workflow" }
    } catch (e: TimeoutException) {
      logger.error(e) { "Can't create a new connection manager workflow due to timeout" }
    }

    return connectionManagerWorkflow
  }

  /**
   * This will cancel a workflow even if the connection is deleted already.
   *
   * @param connectionId - connectionId to cancel
   */
  fun forceDeleteWorkflow(connectionId: UUID?): Unit = connectionManagerUtils.deleteWorkflowIfItExist(connectionId)

  /**
   * Signal to the connection manager workflow that there has been a change to the connection's
   * configuration.
   *
   * @param connectionId connection id
   */
  fun update(connectionId: UUID) {
    val connectionManagerWorkflow: ConnectionManagerWorkflow

    try {
      connectionManagerWorkflow = connectionManagerUtils.getConnectionManagerWorkflow(connectionId)
    } catch (_: DeletedWorkflowException) {
      logger.info { "Connection $connectionId is deleted, and therefore cannot be updated." }
      return
    } catch (e: UnreachableWorkflowException) {
      metricClient.count(
        OssMetricsRegistry.WORFLOW_UNREACHABLE,
        1,
        MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
      )
      logger.error(e) {
        "Failed to retrieve ConnectionManagerWorkflow for connection $connectionId. Repairing state by creating new workflow."
      }
      connectionManagerUtils.safeTerminateWorkflow(
        connectionId,
        "Terminating workflow in unreachable state before starting a new workflow for this connection",
      )
      submitConnectionUpdaterAsync(connectionId)
      return
    }

    connectionManagerWorkflow.connectionUpdated()
  }

  private fun getConnectorJobSucceeded(output: ConnectorJobOutput): Boolean = output.failureReason == null

  /**
   * Check if a workflow is reachable for signal calls by attempting to query for current state. If
   * the query succeeds, and the workflow is not marked as deleted, the workflow is reachable.
   */
  @InternalForTesting
  internal fun isWorkflowReachable(connectionId: UUID?): Boolean =
    try {
      connectionManagerUtils.getConnectionManagerWorkflow(connectionId)
      true
    } catch (_: Exception) {
      false
    }

  fun isInRunningWorkflowCache(workflowName: String?): Boolean = workflowNames.contains(workflowName)

  private fun getNetworkSecurityTokens(workspaceId: UUID): List<String> =
    try {
      scopedConfigurationService
        .getScopedConfigurations(NetworkSecurityTokenKey, mapOf(ConfigScopeType.WORKSPACE to workspaceId))
        .map { it.value }
        .toList()
    } catch (e: IllegalArgumentException) {
      logger.error { e.message }
      emptyList()
    }
}
