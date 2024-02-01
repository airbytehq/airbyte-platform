package io.airbyte.connectorSidecar

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.converters.ConnectorConfigUpdater
import io.airbyte.commons.enums.Enums
import io.airbyte.commons.io.IOs
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.protocol.models.AirbyteConnectionStatus
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteTraceMessage
import io.airbyte.workers.WorkerUtils
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.helper.FailureHelper
import io.airbyte.workers.helper.FailureHelper.ConnectorCommand
import io.airbyte.workers.internal.AirbyteStreamFactory
import jakarta.inject.Singleton
import java.io.IOException
import java.io.InputStream
import java.util.Optional
import java.util.stream.Collectors

@Singleton
class ConnectorMessageProcessor(val connectorConfigUpdater: ConnectorConfigUpdater) {
  fun runCheck(
    inputStream: InputStream,
    streamFactory: AirbyteStreamFactory,
    input: StandardCheckConnectionInput,
    exitCode: Int,
  ): ConnectorJobOutput {
    if (exitCode != 0) {
      throw WorkerException("Check operation returned a non zero exit code, the exit code is: $exitCode")
    }

    try {
      val jobOutput: ConnectorJobOutput =
        ConnectorJobOutput()
          .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
      val messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>> = getMessagesByType(inputStream, streamFactory)
      val connectionStatus = getConnectionStatus(messagesByType)

      val inputConfig = input.connectionConfiguration

      updateConfigFromControlMessage(input, messagesByType, inputConfig, jobOutput)

      val failureReason = getJobFailureReasonFromMessages(ConnectorJobOutput.OutputType.CHECK_CONNECTION, messagesByType)
      failureReason.ifPresent { failureReason: FailureReason? -> jobOutput.failureReason = failureReason }

      if (connectionStatus.isPresent) {
        val output =
          StandardCheckConnectionOutput()
            .withStatus(Enums.convertTo(connectionStatus.get().status, StandardCheckConnectionOutput.Status::class.java))
            .withMessage(connectionStatus.get().message)
        jobOutput.checkConnection = output
      } else if (failureReason.isEmpty) {
        throw WorkerException("Error checking connection status: no status nor failure reason were outputted")
      }

      return jobOutput
    } catch (e: IOException) {
      val errorMessage: String = String.format("Lost connection to the %s: ", input.getActorType())
      throw WorkerException(errorMessage, e)
    } catch (e: Exception) {
      throw WorkerException("Unexpected error while getting checking connection.", e)
    }
  }

  @VisibleForTesting
  fun updateConfigFromControlMessage(
    input: StandardCheckConnectionInput,
    messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>,
    inputConfig: JsonNode,
    jobOutput: ConnectorJobOutput,
  ) {
    if (input.actorId != null && input.actorType != null) {
      val optionalConfigMsg = WorkerUtils.getMostRecentConfigControlMessage(messagesByType)
      if (optionalConfigMsg.isPresent && WorkerUtils.getDidControlMessageChangeConfig(inputConfig, optionalConfigMsg.get())) {
        when (input.actorType) {
          ActorType.SOURCE ->
            connectorConfigUpdater.updateSource(
              input.actorId,
              optionalConfigMsg.get().config,
            )

          ActorType.DESTINATION ->
            connectorConfigUpdater.updateDestination(
              input.actorId,
              optionalConfigMsg.get().config,
            )
        }
        jobOutput.connectorConfigurationUpdated = true
      }
    }
  }

  companion object {
    fun getConnectionStatus(messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>): Optional<AirbyteConnectionStatus> {
      return messagesByType
        .getOrDefault(AirbyteMessage.Type.CONNECTION_STATUS, ArrayList()).stream()
        .map { obj: AirbyteMessage -> obj.connectionStatus }
        .findFirst()
    }

    fun getJobFailureReasonFromMessages(
      outputType: ConnectorJobOutput.OutputType,
      messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>,
    ): Optional<FailureReason> {
      val traceMessage = getTraceMessageFromMessagesByType(messagesByType)
      if (traceMessage.isPresent) {
        val connectorCommand = getConnectorCommandFromOutputType(outputType)
        return Optional.of(FailureHelper.connectorCommandFailure(traceMessage.get(), null, null, connectorCommand))
      } else {
        return Optional.empty()
      }
    }

    fun getTraceMessageFromMessagesByType(messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>): Optional<AirbyteTraceMessage> {
      return messagesByType.getOrDefault(AirbyteMessage.Type.TRACE, java.util.ArrayList()).stream()
        .map { obj: AirbyteMessage -> obj.trace }
        .filter { trace: AirbyteTraceMessage -> trace.type == AirbyteTraceMessage.Type.ERROR }
        .findFirst()
    }

    private fun getConnectorCommandFromOutputType(outputType: ConnectorJobOutput.OutputType): ConnectorCommand {
      return when (outputType) {
        ConnectorJobOutput.OutputType.SPEC -> ConnectorCommand.SPEC
        ConnectorJobOutput.OutputType.CHECK_CONNECTION -> ConnectorCommand.CHECK
        ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID -> ConnectorCommand.DISCOVER
      }
    }

    fun getMessagesByType(
      inputStream: InputStream,
      streamFactory: AirbyteStreamFactory,
    ): Map<AirbyteMessage.Type, List<AirbyteMessage>> {
      return streamFactory.create(IOs.newBufferedReader(inputStream))
        .collect(Collectors.groupingBy { it.type })
    }
  }
}
