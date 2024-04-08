package io.airbyte.connectorSidecar

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.client.generated.SourceApi
import io.airbyte.api.client.model.generated.SourceDiscoverSchemaWriteRequestBody
import io.airbyte.commons.converters.CatalogClientConverters
import io.airbyte.commons.converters.ConnectorConfigUpdater
import io.airbyte.commons.enums.Enums
import io.airbyte.commons.io.IOs
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.protocol.models.AirbyteCatalog
import io.airbyte.protocol.models.AirbyteConnectionStatus
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteTraceMessage
import io.airbyte.protocol.models.ConnectorSpecification
import io.airbyte.workers.WorkerUtils
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.helper.FailureHelper
import io.airbyte.workers.helper.FailureHelper.ConnectorCommand
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.workers.models.SidecarInput.OperationType
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.IOException
import java.io.InputStream
import java.util.Optional
import java.util.UUID
import java.util.stream.Collectors
import javax.naming.OperationNotSupportedException

private val logger = KotlinLogging.logger {}

@Singleton
class ConnectorMessageProcessor(
  val connectorConfigUpdater: ConnectorConfigUpdater,
  val sourceApi: SourceApi,
) {
  data class OperationResult(
    val connectionStatus: AirbyteConnectionStatus? = null,
    val catalog: AirbyteCatalog? = null,
    val spec: ConnectorSpecification? = null,
  )

  data class OperationInput(
    val checkInput: StandardCheckConnectionInput? = null,
    val discoveryInput: StandardDiscoverCatalogInput? = null,
  )

  fun run(
    inputStream: InputStream,
    streamFactory: AirbyteStreamFactory,
    input: OperationInput,
    exitCode: Int,
    operationType: OperationType,
  ): ConnectorJobOutput {
    if (exitCode != 0) {
      throw WorkerException("Check operation returned a non zero exit code, the exit code is: $exitCode")
    }

    try {
      val jobOutput: ConnectorJobOutput = createBaseOutput(operationType)
      val messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>> = getMessagesByType(inputStream, streamFactory)
      val result =
        when (operationType) {
          OperationType.CHECK -> getConnectionStatus(messagesByType)
          OperationType.DISCOVER -> getDiscoveryResult(messagesByType)
          OperationType.SPEC -> getSpecResult(messagesByType)
        }

      val inputConfig =
        when (operationType) {
          OperationType.CHECK -> input.checkInput!!.connectionConfiguration
          OperationType.DISCOVER -> input.discoveryInput!!.connectionConfiguration
          OperationType.SPEC -> Jsons.emptyObject()
        }
      if (operationType != OperationType.SPEC) {
        updateConfigFromControlMessagePerMessageType(operationType, input, messagesByType, inputConfig, jobOutput)
      }
      val failureReason = getJobFailureReasonFromMessages(ConnectorJobOutput.OutputType.CHECK_CONNECTION, messagesByType)
      failureReason.ifPresent { failureReason: FailureReason? -> jobOutput.failureReason = failureReason }

      setOutput(operationType, result, jobOutput, failureReason, input)
      return jobOutput
    } catch (e: IOException) {
      val errorMessage: String = String.format("Lost connection to the connector")
      throw WorkerException(errorMessage, e)
    } catch (e: Exception) {
      throw WorkerException("Unexpected error performing $operationType.", e)
    }
  }

  private fun createBaseOutput(operation: OperationType): ConnectorJobOutput {
    return ConnectorJobOutput()
      .withOutputType(
        when (operation) {
          OperationType.CHECK -> ConnectorJobOutput.OutputType.CHECK_CONNECTION
          OperationType.DISCOVER -> ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID
          OperationType.SPEC -> ConnectorJobOutput.OutputType.SPEC
        },
      )
  }

  private fun updateConfigFromControlMessagePerMessageType(
    operationType: OperationType,
    input: OperationInput,
    messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>,
    inputConfig: JsonNode,
    jobOutput: ConnectorJobOutput,
  ) {
    when (operationType) {
      OperationType.CHECK ->
        updateConfigFromControlMessage(
          input.checkInput!!.actorId,
          input.checkInput!!.actorType,
          messagesByType,
          inputConfig,
          jobOutput,
        )
      OperationType.DISCOVER ->
        updateConfigFromControlMessage(
          UUID.fromString(input.discoveryInput!!.sourceId),
          ActorType.SOURCE,
          messagesByType,
          inputConfig,
          jobOutput,
        )

      else -> {
        logger.error { "Updating the connector with the control message is not supported for the $operationType operation" }
        throw OperationNotSupportedException("Updating the connector with the control message is not supported for the $operationType operation")
      }
    }
  }

  private fun setOutput(
    operationType: OperationType,
    result: OperationResult,
    jobOutput: ConnectorJobOutput,
    failureReason: Optional<FailureReason>,
    input: OperationInput,
  ) {
    when (operationType) {
      OperationType.CHECK ->
        if (result.connectionStatus != null) {
          val output =
            StandardCheckConnectionOutput()
              .withStatus(Enums.convertTo(result.connectionStatus.status, StandardCheckConnectionOutput.Status::class.java))
              .withMessage(result.connectionStatus.message)
          jobOutput.checkConnection = output
        } else if (failureReason.isEmpty) {
          throw WorkerException("Error checking connection status: no status nor failure reason provided")
        }

      OperationType.DISCOVER ->
        if (result.catalog != null && input.discoveryInput != null) {
          val apiResult =
            sourceApi
              .writeDiscoverCatalogResult(buildSourceDiscoverSchemaWriteRequestBody(input.discoveryInput, result.catalog))
          jobOutput.discoverCatalogId = apiResult.catalogId
        } else if (failureReason.isEmpty) {
          throw WorkerException("Error discovering catalog: no failure reason provided")
        }

      OperationType.SPEC ->
        if (result.spec != null) {
          jobOutput.spec = result.spec
        }
    }
  }

  private fun buildSourceDiscoverSchemaWriteRequestBody(
    discoverSchemaInput: StandardDiscoverCatalogInput,
    catalog: AirbyteCatalog,
  ): SourceDiscoverSchemaWriteRequestBody {
    return SourceDiscoverSchemaWriteRequestBody().catalog(
      CatalogClientConverters.toAirbyteCatalogClientApi(catalog),
    ).sourceId(
      if (discoverSchemaInput.sourceId == null) null else UUID.fromString(discoverSchemaInput.sourceId),
    )
      .connectorVersion(
        if (discoverSchemaInput.connectorVersion == null) "" else discoverSchemaInput.connectorVersion,
      )
      .configurationHash(
        discoverSchemaInput.configHash,
      )
  }

  @VisibleForTesting
  fun updateConfigFromControlMessage(
    actorId: UUID?,
    actorType: ActorType,
    messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>,
    inputConfig: JsonNode,
    jobOutput: ConnectorJobOutput,
  ) {
    if (actorId != null && actorType != null) {
      val optionalConfigMsg = WorkerUtils.getMostRecentConfigControlMessage(messagesByType)
      if (optionalConfigMsg.isPresent && WorkerUtils.getDidControlMessageChangeConfig(inputConfig, optionalConfigMsg.get())) {
        when (actorType) {
          ActorType.SOURCE ->
            connectorConfigUpdater.updateSource(
              actorId,
              optionalConfigMsg.get().config,
            )

          ActorType.DESTINATION ->
            connectorConfigUpdater.updateDestination(
              actorId,
              optionalConfigMsg.get().config,
            )
        }
        jobOutput.connectorConfigurationUpdated = true
      }
    }
  }

  companion object {
    fun getConnectionStatus(messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>): OperationResult {
      return OperationResult(
        connectionStatus =
          messagesByType
            .getOrDefault(AirbyteMessage.Type.CONNECTION_STATUS, ArrayList()).stream()
            .map { obj: AirbyteMessage -> obj.connectionStatus }
            .findFirst()
            .orElse(null),
      )
    }

    fun getDiscoveryResult(messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>): OperationResult {
      return OperationResult(
        catalog =
          messagesByType
            .getOrDefault(AirbyteMessage.Type.CATALOG, ArrayList()).stream()
            .map { obj: AirbyteMessage -> obj.catalog }
            .findFirst()
            .orElse(null),
      )
    }

    fun getSpecResult(messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>): OperationResult {
      return OperationResult(
        spec =
          messagesByType
            .getOrDefault(AirbyteMessage.Type.SPEC, ArrayList()).stream()
            .map { obj: AirbyteMessage -> obj.spec }
            .findFirst()
            .orElse(null),
      )
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
