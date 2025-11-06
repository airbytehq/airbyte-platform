/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.workflows

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import datadog.trace.api.Trace
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.temporal.annotations.TemporalActivityStub
import io.airbyte.commons.temporal.scheduling.CheckCommandApiInput
import io.airbyte.commons.temporal.scheduling.CheckCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.commons.temporal.scheduling.DiscoverCommandApiInput
import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.commons.temporal.scheduling.ReplicationCommandApiInput
import io.airbyte.commons.temporal.scheduling.SpecCommandApiInput
import io.airbyte.commons.temporal.scheduling.SpecCommandInput
import io.airbyte.commons.timer.Stopwatch
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.Metadata
import io.airbyte.config.SignalInput
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME
import io.airbyte.metrics.lib.ApmTraceConstants.Tags
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workers.commands.CheckCommand
import io.airbyte.workers.commands.CheckCommandV2
import io.airbyte.workers.commands.ConnectorCommand
import io.airbyte.workers.commands.DiscoverCommand
import io.airbyte.workers.commands.DiscoverCommandV2
import io.airbyte.workers.commands.ReplicationCommand
import io.airbyte.workers.commands.SpecCommand
import io.airbyte.workers.commands.SpecCommandV2
import io.airbyte.workers.models.CheckConnectionApiInput
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.DiscoverSourceApiInput
import io.airbyte.workers.models.ReplicationApiInput
import io.airbyte.workers.models.SpecApiInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.workload.WorkspaceNotFoundException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.Activity
import io.temporal.activity.ActivityExecutionContext
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.failure.ActivityFailure
import io.temporal.failure.ApplicationFailure
import io.temporal.failure.CanceledFailure
import io.temporal.workflow.Workflow
import jakarta.inject.Singleton
import java.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration

@JsonDeserialize(builder = ConnectorCommandActivityInput.Builder::class)
data class ConnectorCommandActivityInput(
  val input: ConnectorCommandInput,
  val signalPayload: String?,
  val id: String?,
  val startTimeInMillis: Long,
) {
  class Builder(
    var input: ConnectorCommandInput? = null,
    var signalPayload: String? = null,
    var id: String? = null,
    var startTimeInMillis: Long? = null,
  ) {
    fun input(input: ConnectorCommandInput) = apply { this.input = input }

    fun signalPayload(signalPayload: String) = apply { this.signalPayload = signalPayload }

    fun id(id: String) = apply { this.id = id }

    fun startTimeInMillis(startTimeInMillis: Long) = apply { this.startTimeInMillis = startTimeInMillis }

    fun build() =
      ConnectorCommandActivityInput(
        input = input ?: throw IllegalArgumentException("input must be defined"),
        signalPayload = signalPayload,
        id = id,
        startTimeInMillis = startTimeInMillis ?: throw IllegalArgumentException("startTimeInMillis must be defined"),
      )
  }
}

@ActivityInterface
interface ConnectorCommandActivity {
  @ActivityMethod
  fun startCommand(activityInput: ConnectorCommandActivityInput): String

  @ActivityMethod
  fun isCommandTerminal(activityInput: ConnectorCommandActivityInput): Boolean

  @ActivityMethod
  fun getCommandOutput(activityInput: ConnectorCommandActivityInput): ConnectorJobOutput

  @ActivityMethod
  fun cancelCommand(activityInput: ConnectorCommandActivityInput)

  @ActivityMethod
  fun getAwaitDuration(activityInput: ConnectorCommandActivityInput): Duration
}

/**
 * Wraps static activity context accessor to make it testable.
 */
@Singleton
class ActivityExecutionContextProvider {
  fun get(): ActivityExecutionContext = Activity.getExecutionContext()
}

@Singleton
class ConnectorCommandActivityImpl(
  private val checkCommand: CheckCommand,
  private val checkCommandApi: CheckCommandV2,
  private val discoverCommand: DiscoverCommand,
  private val discoverCommandApi: DiscoverCommandV2,
  private val specCommand: SpecCommand,
  private val specCommandApi: SpecCommandV2,
  private val replicationCommandApi: ReplicationCommand,
  private val activityExecutionContextProvider: ActivityExecutionContextProvider,
  private val metricClient: MetricClient,
) : ConnectorCommandActivity {
  companion object {
    fun CheckCommandInput.CheckConnectionInput.toWorkerModels(): CheckConnectionInput =
      CheckConnectionInput(jobRunConfig, integrationLauncherConfig, checkConnectionInput)

    fun CheckCommandApiInput.CheckConnectionApiInput.toWorkerModels(): CheckConnectionApiInput = CheckConnectionApiInput(actorId, jobId, attemptId)

    fun DiscoverCommandInput.DiscoverCatalogInput.toWorkerModels(): DiscoverCatalogInput =
      DiscoverCatalogInput(jobRunConfig, integrationLauncherConfig, discoverCatalogInput)

    fun DiscoverCommandApiInput.DiscoverApiInput.toWorkerModels(): DiscoverSourceApiInput = DiscoverSourceApiInput(actorId, jobId, attemptNumber)

    fun SpecCommandInput.SpecInput.toWorkerModels(): SpecInput = SpecInput(jobRunConfig, integrationLauncherConfig)

    fun SpecCommandApiInput.SpecApiInput.toWorkerModels(): SpecApiInput =
      SpecApiInput(requestId, commandId, actorDefinitionId, dockerImage, dockerImageTag, workspaceId)

    fun ReplicationCommandApiInput.ReplicationApiInput.toWorkerModels(): ReplicationApiInput =
      ReplicationApiInput(connectionId, jobId, attemptId, appliedCatalogDiff)

    val logger = KotlinLogging.logger {}
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun startCommand(activityInput: ConnectorCommandActivityInput): String =
    withInstrumentation(activityInput) {
      when (activityInput.input) {
        is CheckCommandInput -> checkCommand.start(activityInput.input.input.toWorkerModels(), activityInput.signalPayload)
        is DiscoverCommandInput -> discoverCommand.start(activityInput.input.input.toWorkerModels(), activityInput.signalPayload)
        is SpecCommandInput -> specCommand.start(activityInput.input.input.toWorkerModels(), activityInput.signalPayload)
        is SpecCommandApiInput -> specCommandApi.start(activityInput.input.input.toWorkerModels(), activityInput.signalPayload)
        is CheckCommandApiInput -> checkCommandApi.start(activityInput.input.input.toWorkerModels(), activityInput.signalPayload)
        is DiscoverCommandApiInput -> discoverCommandApi.start(activityInput.input.input.toWorkerModels(), activityInput.signalPayload)
        is ReplicationCommandApiInput -> replicationCommandApi.start(activityInput.input.input.toWorkerModels(), activityInput.signalPayload)
      }
    }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun isCommandTerminal(activityInput: ConnectorCommandActivityInput): Boolean =
    withInstrumentation(activityInput) {
      getCommand(activityInput.input).isTerminal(id = activityInput.id ?: throw IllegalStateException("id must exist"))
    }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun getCommandOutput(activityInput: ConnectorCommandActivityInput): ConnectorJobOutput =
    withInstrumentation(activityInput, reportCommandSummaryMetrics = true) {
      getCommand(activityInput.input).getOutput(id = activityInput.id ?: throw IllegalStateException("id must exist"))
    }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun cancelCommand(activityInput: ConnectorCommandActivityInput) {
    withInstrumentation(activityInput, reportCommandSummaryMetrics = true) {
      getCommand(activityInput.input).cancel(id = activityInput.id ?: throw IllegalStateException("id must exist"))
    }
  }

  override fun getAwaitDuration(activityInput: ConnectorCommandActivityInput): Duration =
    getCommand(activityInput.input).getAwaitDuration().toJavaDuration()

  private fun <T> withInstrumentation(
    activityInput: ConnectorCommandActivityInput,
    reportCommandSummaryMetrics: Boolean = false,
    block: () -> T,
  ): T {
    var success = true
    val metricAttributes =
      mutableListOf(
        MetricAttribute(MetricTags.COMMAND, activityInput.input.type),
      )
    ApmTraceUtils.addTagsToTrace(metricAttributes)

    try {
      val activityInfo = activityExecutionContextProvider.get().info
      metricAttributes.add(MetricAttribute(MetricTags.COMMAND_STEP, activityInfo.activityType))
      ApmTraceUtils.addTagsToTrace(
        listOf(
          MetricAttribute(Tags.TEMPORAL_WORKFLOW_ID_KEY, activityInfo.workflowId),
          MetricAttribute(Tags.TEMPORAL_RUN_ID_KEY, activityInfo.runId),
          MetricAttribute(Tags.TEMPORAL_TASK_QUEUE_KEY, activityInfo.activityTaskQueue),
        ),
      )
    } catch (e: Exception) {
      // We shouldn't fail because we fail to populate trace attributes
      logger.warn(e) { "Failed to lookup activity execution context" }
    }

    val stopwatch = Stopwatch()

    try {
      return stopwatch.time { block() }
    } catch (e: Throwable) {
      ApmTraceUtils.addExceptionToTrace(e)
      success = false
      throw e
    } finally {
      metricAttributes.add(MetricAttribute(MetricTags.STATUS, if (success) MetricTags.SUCCESS else MetricTags.FAILURE))
      metricClient.count(metric = OssMetricsRegistry.COMMAND_STEP, attributes = metricAttributes.toTypedArray())
      metricClient.distribution(
        metric = OssMetricsRegistry.COMMAND_STEP_DURATION,
        value = stopwatch.getElapsedTimeInNanos().toDouble(),
        attributes = metricAttributes.toTypedArray(),
      )

      if (reportCommandSummaryMetrics) {
        metricClient.count(metric = OssMetricsRegistry.COMMAND, attributes = arrayOf(MetricAttribute(MetricTags.COMMAND, activityInput.input.type)))
        metricClient.distribution(
          metric = OssMetricsRegistry.COMMAND_DURATION,
          value = (System.currentTimeMillis() - activityInput.startTimeInMillis).toDouble(),
          attributes = arrayOf(MetricAttribute(MetricTags.COMMAND, activityInput.input.type)),
        )
      }
    }
  }

  private fun getCommand(input: ConnectorCommandInput): ConnectorCommand<*> =
    when (input) {
      is CheckCommandInput -> checkCommand
      is DiscoverCommandInput -> discoverCommand
      is SpecCommandInput -> specCommand
      is SpecCommandApiInput -> specCommandApi
      is CheckCommandApiInput -> checkCommandApi
      is DiscoverCommandApiInput -> discoverCommandApi
      is ReplicationCommandApiInput -> replicationCommandApi
    }
}

open class ConnectorCommandWorkflowImpl : ConnectorCommandWorkflow {
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private lateinit var connectorCommandActivity: ConnectorCommandActivity

  private var shouldBlock = true

  override fun checkTerminalStatus() {
    shouldBlock = false
  }

  override fun run(input: ConnectorCommandInput): ConnectorJobOutput {
    val signalPayload = Jsons.serialize(SignalInput(SignalInput.CONNECTOR_COMMAND_WORKFLOW, Workflow.getInfo().workflowId))
    var activityInput =
      ConnectorCommandActivityInput(
        input = input,
        signalPayload = signalPayload,
        id = null,
        startTimeInMillis = System.currentTimeMillis(),
      )

    // Versioning for a smoother that doesn't fail all ongoing commands upon release.
    val useAwaitFromCommand = Workflow.getVersion("useAwaitFromCommand", Workflow.DEFAULT_VERSION, 1)
    // We look up the await duration at the beginning of the workflow so that we can freely change the awaitDuration of a command
    // without impacting already running workflows. This way, they don't NonDeterministicException and get to finish on their initial config.
    val awaitDuration =
      if (useAwaitFromCommand == Workflow.DEFAULT_VERSION) {
        1.minutes.toJavaDuration()
      } else {
        connectorCommandActivity.getAwaitDuration(activityInput)
      }

    try {
      val id = connectorCommandActivity.startCommand(activityInput)
      activityInput = activityInput.copy(id = id)

      shouldBlock = !connectorCommandActivity.isCommandTerminal(activityInput)
      while (shouldBlock) {
        Workflow.await(awaitDuration) { !shouldBlock }
        shouldBlock = !connectorCommandActivity.isCommandTerminal(activityInput)
      }

      return connectorCommandActivity.getCommandOutput(activityInput)
    } catch (e: ActivityFailure) {
      // Check if the underlying cause is WorkspaceNotFoundException
      if (isWorkspaceNotFoundException(e)) {
        // Return structured output instead of failing the workflow
        return createWorkspaceNotFoundOutput(input, e)
      }

      // Handle other activity failures (cancellation, etc.)
      when (e.cause) {
        is CanceledFailure -> {
          val detachedCancellationScope =
            Workflow.newDetachedCancellationScope {
              connectorCommandActivity.cancelCommand(activityInput)
              shouldBlock = false
            }
          detachedCancellationScope.run()
        }
      }
      throw e
    }
  }

  /**
   * Check if the exception chain contains a WorkspaceNotFoundException.
   * Temporal wraps exceptions, so we need to check the cause chain.
   */
  private fun isWorkspaceNotFoundException(throwable: Throwable?): Boolean {
    if (throwable == null) return false
    if (throwable is WorkspaceNotFoundException) return true

    // Check the cause chain
    var current: Throwable? = throwable.cause
    while (current != null) {
      if (current is WorkspaceNotFoundException) return true

      // Check if it's an ApplicationFailure with WorkspaceNotFoundException type
      // (Temporal serializes exceptions across activity boundaries)
      if (current is ApplicationFailure && current.type == WorkspaceNotFoundException::class.java.name) {
        return true
      }

      current = current.cause
    }

    return false
  }

  /**
   * Create a structured failure output for workspace not found errors.
   * This provides explicit, programmatic error information to workflow consumers.
   */
  private fun createWorkspaceNotFoundOutput(
    input: ConnectorCommandInput,
    activityFailure: ActivityFailure,
  ): ConnectorJobOutput {
    val failureReason =
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
        .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
        .withInternalMessage(activityFailure.cause?.message ?: "Workspace not found")
        .withExternalMessage("The workspace for this operation no longer exists. It may have been deleted.")
        .withRetryable(false)
        .withTimestamp(System.currentTimeMillis())
        .withStacktrace(activityFailure.stackTraceToString())
        .withMetadata(
          Metadata()
            .withAdditionalProperty("errorCode", "WORKSPACE_NOT_FOUND")
            .withAdditionalProperty("workflowType", input.type),
        )

    return ConnectorJobOutput()
      .withFailureReason(failureReason)
  }
}
