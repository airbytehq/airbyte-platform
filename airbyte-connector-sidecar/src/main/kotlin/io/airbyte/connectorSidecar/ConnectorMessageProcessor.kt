/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorSidecar

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.client.AirbyteApiClient
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
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteTraceMessage
import io.airbyte.protocol.models.v0.ConnectorSpecification
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
  private val connectorConfigUpdater: ConnectorConfigUpdater,
  private val airbyteApiClient: AirbyteApiClient,
  private val catalogClientConverters: CatalogClientConverters,
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
      val failureReason = getJobFailureReasonFromMessages(operationType.toConnectorOutputType(), messagesByType)

      setOutput(operationType, result, jobOutput, failureReason, input, exitCode)
      return jobOutput
    } catch (e: IOException) {
      val errorMessage: String = String.format("Lost connection to the connector")
      throw WorkerException(errorMessage, e)
    } catch (e: Exception) {
      throw WorkerException("Unexpected error performing $operationType. The exit of the connector was: $exitCode", e)
    }
  }

  private fun createBaseOutput(operation: OperationType): ConnectorJobOutput =
    ConnectorJobOutput()
      .withOutputType(operation.toConnectorOutputType())

  private fun OperationType.toConnectorOutputType(): ConnectorJobOutput.OutputType =
    when (this) {
      OperationType.CHECK -> ConnectorJobOutput.OutputType.CHECK_CONNECTION
      OperationType.DISCOVER -> ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID
      OperationType.SPEC -> ConnectorJobOutput.OutputType.SPEC
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
          input.checkInput.actorType,
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

  @VisibleForTesting
  fun setOutput(
    operationType: OperationType,
    result: OperationResult,
    jobOutput: ConnectorJobOutput,
    failureReason: Optional<FailureReason>,
    input: OperationInput,
    exitCode: Int,
  ) {
    when (operationType) {
      OperationType.CHECK ->
        if (result.connectionStatus != null) {
          val output =
            StandardCheckConnectionOutput()
              .withStatus(Enums.convertTo(result.connectionStatus.status, StandardCheckConnectionOutput.Status::class.java))
              .withMessage(result.connectionStatus.message)
          jobOutput.checkConnection = output
        } else if (failureReason.isEmpty && exitCode == 0) {
          throw WorkerException("Connector exited successfully without an output for $operationType.")
        } else if (exitCode != 0) {
          jobOutput.checkConnection =
            StandardCheckConnectionOutput()
              .withStatus(StandardCheckConnectionOutput.Status.FAILED)
              .withMessage("The connector running check exited with $exitCode exit code")

          jobOutput.failureReason =
            failureReason.orElse(
              getFailureReasonForNon0ExitCode(
                operationType,
                exitCode,
                if (input.checkInput!!.actorType == ActorType.SOURCE) FailureReason.FailureOrigin.SOURCE else FailureReason.FailureOrigin.DESTINATION,
              ),
            )
        }

      OperationType.DISCOVER ->
        if (result.catalog != null && input.discoveryInput != null) {
          logger.info { "Writing catalog result to API..." }
          val apiResult =
            airbyteApiClient.sourceApi
              .writeDiscoverCatalogResult(buildSourceDiscoverSchemaWriteRequestBody(input.discoveryInput, result.catalog))
          logger.info { "Finished writing catalog result to API." }
          jobOutput.discoverCatalogId = apiResult.catalogId
        } else if (failureReason.isEmpty && exitCode == 0) {
          throw WorkerException("Connector exited successfully without an output for $operationType.")
        } else if (exitCode != 0) {
          jobOutput.failureReason = failureReason.orElse(getFailureReasonForNon0ExitCode(operationType, exitCode, FailureReason.FailureOrigin.SOURCE))
        }

      OperationType.SPEC ->
        if (result.spec != null) {
          jobOutput.spec = result.spec
        } else if (failureReason.isEmpty && exitCode == 0) {
          throw WorkerException("Connector exited successfully without an output for $operationType.")
        } else if (exitCode != 0) {
          jobOutput.failureReason = failureReason.orElse(getFailureReasonForNon0ExitCode(operationType, exitCode, FailureReason.FailureOrigin.SOURCE))
        }
    }
  }

  private fun getFailureReasonForNon0ExitCode(
    operationType: OperationType,
    exitCode: Int,
    failureOrigin: FailureReason.FailureOrigin,
  ): FailureReason =
    FailureReason()
      .withExternalMessage("The $operationType operation returned an exit code $exitCode")
      .withExternalMessage("The main container of the $operationType operation returned an exit code $exitCode")
      .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
      .withFailureOrigin(failureOrigin)

  private fun buildSourceDiscoverSchemaWriteRequestBody(
    discoverSchemaInput: StandardDiscoverCatalogInput,
    catalog: AirbyteCatalog,
  ): SourceDiscoverSchemaWriteRequestBody =
    SourceDiscoverSchemaWriteRequestBody(
      catalog = catalogClientConverters.toAirbyteCatalogClientApi(catalog),
      sourceId = if (discoverSchemaInput.sourceId == null) null else UUID.fromString(discoverSchemaInput.sourceId),
      connectorVersion = if (discoverSchemaInput.connectorVersion == null) "" else discoverSchemaInput.connectorVersion,
      configurationHash = discoverSchemaInput.configHash,
    )

  @VisibleForTesting
  fun updateConfigFromControlMessage(
    actorId: UUID?,
    actorType: ActorType,
    messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>,
    inputConfig: JsonNode,
    jobOutput: ConnectorJobOutput,
  ) {
    logger.info { "Checking for optional control message..." }
    if (actorId != null) {
      val optionalConfigMsg = WorkerUtils.getMostRecentConfigControlMessage(messagesByType)
      if (optionalConfigMsg.isPresent && WorkerUtils.getDidControlMessageChangeConfig(inputConfig, optionalConfigMsg.get())) {
        logger.info { "Optional control message present. Updating..." }
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
        logger.info { "Update complete." }
      }
    } else {
      logger.info { "Optional control message not found. Skipping..." }
    }
  }

  companion object {
    fun getConnectionStatus(messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>): OperationResult =
      OperationResult(
        connectionStatus =
          messagesByType
            .getOrDefault(AirbyteMessage.Type.CONNECTION_STATUS, ArrayList())
            .stream()
            .map { obj: AirbyteMessage -> obj.connectionStatus }
            .findFirst()
            .orElse(null),
      )

    fun getDiscoveryResult(messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>): OperationResult =
      OperationResult(
        catalog =
          messagesByType
            .getOrDefault(AirbyteMessage.Type.CATALOG, ArrayList())
            .stream()
            .map { obj: AirbyteMessage -> obj.catalog }
            .findFirst()
            .orElse(null),
      )

    fun getSpecResult(messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>): OperationResult =
      OperationResult(
        spec =
          messagesByType
            .getOrDefault(AirbyteMessage.Type.SPEC, ArrayList())
            .stream()
            .map { obj: AirbyteMessage -> obj.spec }
            .findFirst()
            .orElse(null),
      )

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

    fun getTraceMessageFromMessagesByType(messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>): Optional<AirbyteTraceMessage> =
      messagesByType
        .getOrDefault(AirbyteMessage.Type.TRACE, java.util.ArrayList())
        .stream()
        .map { obj: AirbyteMessage -> obj.trace }
        .filter { trace: AirbyteTraceMessage -> trace.type == AirbyteTraceMessage.Type.ERROR }
        .findFirst()

    private fun getConnectorCommandFromOutputType(outputType: ConnectorJobOutput.OutputType): ConnectorCommand =
      when (outputType) {
        ConnectorJobOutput.OutputType.SPEC -> ConnectorCommand.SPEC
        ConnectorJobOutput.OutputType.CHECK_CONNECTION -> ConnectorCommand.CHECK
        ConnectorJobOutput.OutputType.DISCOVER_CATALOG_ID -> ConnectorCommand.DISCOVER
        ConnectorJobOutput.OutputType.REPLICATE -> throw IllegalStateException("Cannot get connector command from output type $outputType")
      }

    fun getMessagesByType(
      inputStream: InputStream,
      streamFactory: AirbyteStreamFactory,
    ): Map<AirbyteMessage.Type, List<AirbyteMessage>> =
      streamFactory
        .create(IOs.newBufferedReader(inputStream))
        .collect(Collectors.groupingBy { it.type })
  }
}
