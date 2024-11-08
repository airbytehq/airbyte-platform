package io.airbyte.workers.temporal.workflows

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
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.failure.ActivityFailure
import io.temporal.failure.CanceledFailure
import io.temporal.workflow.Workflow
import jakarta.inject.Singleton
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration

@ActivityInterface
interface ConnectorCommandActivity {
  @ActivityMethod
  fun startCommand(
    input: ConnectorCommandInput,
    signalPayload: String?,
  ): String

  @ActivityMethod
  fun isCommandTerminal(
    input: ConnectorCommandInput,
    id: String,
  ): Boolean

  @ActivityMethod
  fun getCommandOutput(
    input: ConnectorCommandInput,
    id: String,
  ): ConnectorJobOutput

  @ActivityMethod
  fun cancelCommand(
    input: ConnectorCommandInput,
    id: String,
  )
}

@Singleton
class ConnectorCommandActivityImpl(
  private val checkCommand: CheckCommand,
  private val discoverCommand: DiscoverCommand,
  private val specCommand: SpecCommand,
  private val metricClient: MetricClient,
) : ConnectorCommandActivity {
  companion object {
    fun CheckCommandInput.CheckConnectionInput.toWorkerModels(): CheckConnectionInput =
      CheckConnectionInput(jobRunConfig, integrationLauncherConfig, checkConnectionInput)

    fun DiscoverCommandInput.DiscoverCatalogInput.toWorkerModels(): DiscoverCatalogInput =
      DiscoverCatalogInput(jobRunConfig, integrationLauncherConfig, discoverCatalogInput)

    fun SpecCommandInput.SpecInput.toWorkerModels(): SpecInput = SpecInput(jobRunConfig, integrationLauncherConfig)
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun startCommand(
    input: ConnectorCommandInput,
    signalPayload: String?,
  ): String =
    withInstrumentation(input) {
      when (input) {
        is CheckCommandInput -> checkCommand.start(input.input.toWorkerModels(), signalPayload)
        is DiscoverCommandInput -> discoverCommand.start(input.input.toWorkerModels(), signalPayload)
        is SpecCommandInput -> specCommand.start(input.input.toWorkerModels(), signalPayload)
      }
    }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun isCommandTerminal(
    input: ConnectorCommandInput,
    id: String,
  ): Boolean =
    withInstrumentation(input) {
      getCommand(input).isTerminal(id)
    }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun getCommandOutput(
    input: ConnectorCommandInput,
    id: String,
  ): ConnectorJobOutput =
    withInstrumentation(input) {
      getCommand(input).getOutput(id)
    }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  override fun cancelCommand(
    input: ConnectorCommandInput,
    id: String,
  ) {
    withInstrumentation(input) {
      getCommand(input).cancel(id)
    }
  }

  private fun <T> withInstrumentation(
    input: ConnectorCommandInput,
    block: () -> T,
  ): T {
    var success = true
    val metricAttributes =
      mutableListOf(
        MetricAttribute(MetricTags.COMMAND, input.type),
        MetricAttribute(MetricTags.COMMAND_STEP, Thread.currentThread().stackTrace[2].methodName),
      )
    ApmTraceUtils.addTagsToTrace(metricAttributes)
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
    val id = connectorCommandActivity.startCommand(input, signalPayload)

    try {
      shouldBlock = true
      while (shouldBlock) {
        Workflow.await(1.minutes.toJavaDuration()) { !shouldBlock }
        shouldBlock = !connectorCommandActivity.isCommandTerminal(input, id)
      }
    } catch (e: Exception) {
      when (e) {
        is CanceledFailure, is ActivityFailure -> {
          val detachedCancellationScope =
            Workflow.newDetachedCancellationScope {
              connectorCommandActivity.cancelCommand(input, id)
              shouldBlock = false
            }
          detachedCancellationScope.run()
        }
        else -> throw e
      }
    }

    return connectorCommandActivity.getCommandOutput(input, id)
  }
}
