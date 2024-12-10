package io.airbyte.workers.temporal.workflows

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import datadog.trace.api.Trace
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.temporal.annotations.TemporalActivityStub
import io.airbyte.commons.temporal.scheduling.CheckCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.commons.temporal.scheduling.SpecCommandInput
import io.airbyte.commons.timer.Stopwatch
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.SignalInput
import io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME
import io.airbyte.metrics.lib.ApmTraceConstants.Tags
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.airbyte.workers.commands.CheckCommand
import io.airbyte.workers.commands.ConnectorCommand
import io.airbyte.workers.commands.DiscoverCommand
import io.airbyte.workers.commands.SpecCommand
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.SpecInput
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.Activity
import io.temporal.activity.ActivityExecutionContext
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.failure.ActivityFailure
import io.temporal.failure.CanceledFailure
import io.temporal.workflow.Workflow
import jakarta.inject.Singleton
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
  private val discoverCommand: DiscoverCommand,
  private val specCommand: SpecCommand,
  private val activityExecutionContextProvider: ActivityExecutionContextProvider,
  private val metricClient: MetricClient,
) : ConnectorCommandActivity {
  companion object {
    fun CheckCommandInput.CheckConnectionInput.toWorkerModels(): CheckConnectionInput =
      CheckConnectionInput(jobRunConfig, integrationLauncherConfig, checkConnectionInput)

    fun DiscoverCommandInput.DiscoverCatalogInput.toWorkerModels(): DiscoverCatalogInput =
      DiscoverCatalogInput(jobRunConfig, integrationLauncherConfig, discoverCatalogInput)

    fun SpecCommandInput.SpecInput.toWorkerModels(): SpecInput = SpecInput(jobRunConfig, integrationLauncherConfig)

    val logger = KotlinLogging.logger {}
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun startCommand(activityInput: ConnectorCommandActivityInput): String =
    withInstrumentation(activityInput) {
      when (activityInput.input) {
        is CheckCommandInput -> checkCommand.start(activityInput.input.input.toWorkerModels(), activityInput.signalPayload)
        is DiscoverCommandInput -> discoverCommand.start(activityInput.input.input.toWorkerModels(), activityInput.signalPayload)
        is SpecCommandInput -> specCommand.start(activityInput.input.input.toWorkerModels(), activityInput.signalPayload)
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
      metricClient.count(OssMetricsRegistry.COMMAND_STEP, 1, *metricAttributes.toTypedArray())
      metricClient.distribution(
        OssMetricsRegistry.COMMAND_STEP_DURATION,
        stopwatch.getElapsedTimeInNanos().toDouble(),
        *metricAttributes.toTypedArray(),
      )

      if (reportCommandSummaryMetrics) {
        metricClient.count(OssMetricsRegistry.COMMAND, 1, MetricAttribute(MetricTags.COMMAND, activityInput.input.type))
        metricClient.distribution(
          OssMetricsRegistry.COMMAND_DURATION,
          (System.currentTimeMillis() - activityInput.startTimeInMillis).toDouble(),
          MetricAttribute(MetricTags.COMMAND, activityInput.input.type),
        )
      }
    }
  }

  private fun getCommand(input: ConnectorCommandInput): ConnectorCommand<*> =
    when (input) {
      is CheckCommandInput -> checkCommand
      is DiscoverCommandInput -> discoverCommand
      is SpecCommandInput -> specCommand
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
    val id = connectorCommandActivity.startCommand(activityInput)
    activityInput = activityInput.copy(id = id)

    try {
      shouldBlock = !connectorCommandActivity.isCommandTerminal(activityInput)
      while (shouldBlock) {
        Workflow.await(1.minutes.toJavaDuration()) { !shouldBlock }
        shouldBlock = !connectorCommandActivity.isCommandTerminal(activityInput)
      }
    } catch (e: Exception) {
      when (e) {
        is CanceledFailure, is ActivityFailure -> {
          val detachedCancellationScope =
            Workflow.newDetachedCancellationScope {
              connectorCommandActivity.cancelCommand(activityInput)
              shouldBlock = false
            }
          detachedCancellationScope.run()
        }
        else -> throw e
      }
    }

    return connectorCommandActivity.getCommandOutput(activityInput)
  }
}
