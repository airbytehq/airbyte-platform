/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.check

import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.logging.DEFAULT_JOB_LOG_PATH_MDC_KEY
import io.airbyte.commons.logging.LogSource
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.WorkloadType
import io.airbyte.config.secrets.InlinedConfigWithSecretRefs
import io.airbyte.config.secrets.toConfigWithRefs
import io.airbyte.directjobrunner.connector.DirectCheckRunner
import io.airbyte.directjobrunner.connector.SourceCheckResult
import io.airbyte.directjobrunner.core.DirectJobHandler
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.workers.helper.FailureHelper
import io.airbyte.workers.hydration.ConnectorSecretsHydrator
import io.airbyte.workers.hydration.SecretHydrationContext
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workers.workload.WorkloadOutputWriter
import io.airbyte.workload.api.domain.LogDeliveryMode
import io.airbyte.workload.api.domain.Workload
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.withLoggingContext
import jakarta.inject.Singleton
import java.time.Duration

private val logger = KotlinLogging.logger {}

private val RESULT_MARGIN: Duration = Duration.ofSeconds(30)

/**
 * Runs CHECK workloads for the source-postgres version baked into this image. Each check is its own connector process
 * ([DirectCheckRunner]), which exits when the check is done, so customers never share a process.
 *
 * Platform and connector logs go to the workload's log path, like a check pod's. Not handled: control-message config
 * updates, which a check pod applies to the saved actor.
 */
@Singleton
class CheckJobHandler(
  private val payloadDeserializer: PayloadDeserializer,
  private val secretsHydrator: ConnectorSecretsHydrator,
  private val directCheckRunner: DirectCheckRunner,
  private val outputWriter: WorkloadOutputWriter,
  private val metricClient: MetricClient,
) : DirectJobHandler {
  override val workloadType: WorkloadType = WorkloadType.CHECK

  // The connector's own timeout plus time to hydrate and write the result. A check whose runner died fails this soon.
  override val runningDeadline: Duration = directCheckRunner.checkTimeout.plus(RESULT_MARGIN)

  // FLEX workloads upload logs to the organization's own destination, which only the check pod's sidecar sets up.
  override fun accepts(workload: Workload): Boolean =
    workload.logDeliveryMode != LogDeliveryMode.FLEX &&
      payloadDeserializer.toCheckConnectionInput(workload.inputPayload).launcherConfig.dockerImage == directCheckRunner.image

  override fun run(
    workloadId: String,
    inputPayload: String,
    logPath: String,
  ) = withLoggingContext(mapOf(DEFAULT_JOB_LOG_PATH_MDC_KEY to logPath) + LogSource.PLATFORM.toMdc()) {
    runCheck(workloadId, inputPayload)
  }

  private fun runCheck(
    workloadId: String,
    inputPayload: String,
  ) {
    val start = System.currentTimeMillis()
    val input = payloadDeserializer.toCheckConnectionInput(inputPayload)
    val image = input.launcherConfig.dockerImage

    val config = hydrate(input.checkConnectionInput)
    val hydratedAt = System.currentTimeMillis()

    val result = directCheckRunner.check(config, workloadId)
    val ranAt = System.currentTimeMillis()

    val output = toJobOutput(result, input.checkConnectionInput.actorType)
    outputWriter.write(workloadId, output)

    metricClient.distribution(
      OssMetricsRegistry.DIRECT_CHECK_CONNECTOR_DURATION,
      (ranAt - hydratedAt).toDouble(),
      MetricAttribute(MetricTags.CONNECTOR_IMAGE, image),
      MetricAttribute(MetricTags.RESULT_TAG, result.javaClass.simpleName),
    )
    logger.info {
      "Check $workloadId image=$image result=${result::class.simpleName} status=${output.checkConnection?.status} " +
        "hydrateMs=${hydratedAt - start} connectorMs=${ranAt - hydratedAt} " +
        "writeMs=${System.currentTimeMillis() - ranAt} totalMs=${System.currentTimeMillis() - start}"
    }
  }

  private fun hydrate(raw: StandardCheckConnectionInput) =
    checkNotNull(
      secretsHydrator.hydrateConfig(
        InlinedConfigWithSecretRefs(raw.connectionConfiguration).toConfigWithRefs(),
        SecretHydrationContext(organizationId = raw.actorContext.organizationId, workspaceId = raw.actorContext.workspaceId),
      ),
    ) { "Secret hydration returned no config" }

  // Same shape the connector sidecar produces, so the server reads the result the same way as a pod check.
  private fun toJobOutput(
    result: SourceCheckResult,
    actorType: ActorType,
  ): ConnectorJobOutput {
    val output = ConnectorJobOutput().withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
    when (result) {
      is SourceCheckResult.Reported ->
        output.checkConnection =
          StandardCheckConnectionOutput()
            .withStatus(result.status.status.convertTo<StandardCheckConnectionOutput.Status>())
            .withMessage(result.status.message)
      is SourceCheckResult.ConnectorError -> {
        // FailureHelper reads emittedAt, and the connector's own value isn't kept, so stamp it with when it was read.
        val trace =
          AirbyteTraceMessage()
            .withType(AirbyteTraceMessage.Type.ERROR)
            .withEmittedAt(System.currentTimeMillis().toDouble())
            .withError(result.error)
        output.failureReason = FailureHelper.connectorCommandFailure(trace, null, null, FailureHelper.ConnectorCommand.CHECK)
        output.checkConnection = failed(result.error.message)
      }
      is SourceCheckResult.Crashed -> {
        output.failureReason = systemFailure("The check operation returned an exit code ${result.exitCode}", actorType)
        output.checkConnection = failed("The connector running check exited with ${result.exitCode} exit code")
      }
      SourceCheckResult.TimedOut -> {
        output.failureReason = systemFailure("The check operation timed out", actorType)
        output.checkConnection = failed("The connector check timed out")
      }
    }
    return output
  }

  private fun failed(message: String?) = StandardCheckConnectionOutput().withStatus(StandardCheckConnectionOutput.Status.FAILED).withMessage(message)

  private fun systemFailure(
    message: String,
    actorType: ActorType,
  ) = FailureReason()
    .withExternalMessage(message)
    .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
    .withFailureOrigin(if (actorType == ActorType.SOURCE) FailureReason.FailureOrigin.SOURCE else FailureReason.FailureOrigin.DESTINATION)
}
