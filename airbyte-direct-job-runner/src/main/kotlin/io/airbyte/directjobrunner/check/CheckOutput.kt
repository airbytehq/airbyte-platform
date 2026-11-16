/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.directjobrunner.check

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.enums.convertTo
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.secrets.InlinedConfigWithSecretRefs
import io.airbyte.config.secrets.toConfigWithRefs
import io.airbyte.directjobrunner.connector.SourceCheckResult
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.workers.helper.FailureHelper
import io.airbyte.workers.hydration.ConnectorSecretsHydrator
import io.airbyte.workers.hydration.SecretHydrationContext
import java.time.Duration

// What every check handler shares: filling in a check's secrets and writing its result the way a check pod does.

/** Time on top of a check's own run to hydrate its config and write its result. */
internal val RESULT_MARGIN: Duration = Duration.ofSeconds(30)

/** The check's connection configuration with its secrets filled in, as a check pod gets it. */
internal fun ConnectorSecretsHydrator.hydrateCheckConfig(raw: StandardCheckConnectionInput): JsonNode =
  checkNotNull(
    hydrateConfig(
      InlinedConfigWithSecretRefs(raw.connectionConfiguration).toConfigWithRefs(),
      SecretHydrationContext(organizationId = raw.actorContext.organizationId, workspaceId = raw.actorContext.workspaceId),
    ),
  ) { "Secret hydration returned no config" }

/** Same shape the connector sidecar produces, so the server reads the result the same way as a pod check. */
internal fun toCheckJobOutput(
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
