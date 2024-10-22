package io.airbyte.connectorSidecar

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.SourceApi
import io.airbyte.api.client.model.generated.DiscoverCatalogResult
import io.airbyte.commons.converters.CatalogClientConverters
import io.airbyte.commons.converters.ConnectorConfigUpdater
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.config.helpers.FieldGenerator
import io.airbyte.protocol.models.AirbyteCatalog
import io.airbyte.protocol.models.AirbyteConnectionStatus
import io.airbyte.protocol.models.AirbyteControlConnectorConfigMessage
import io.airbyte.protocol.models.AirbyteControlMessage
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteStream
import io.airbyte.protocol.models.AirbyteTraceMessage
import io.airbyte.protocol.models.Config
import io.airbyte.protocol.models.ConnectorSpecification
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.airbyte.workers.models.SidecarInput
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.util.Optional
import java.util.UUID
import java.util.stream.Stream

@ExtendWith(MockKExtension::class)
class ConnectorMessageProcessorTest {
  @MockK
  private lateinit var airbyteApiClient: AirbyteApiClient

  @MockK
  private lateinit var streamFactory: AirbyteStreamFactory

  @MockK
  private lateinit var connectorConfigUpdater: ConnectorConfigUpdater

  @MockK
  private lateinit var sourceApi: SourceApi

  private lateinit var connectorMessageProcessor: ConnectorMessageProcessor

  private val catalogClientConverters = CatalogClientConverters(FieldGenerator())

  @BeforeEach
  fun init() {
    every { airbyteApiClient.sourceApi } returns sourceApi
    connectorMessageProcessor = ConnectorMessageProcessor(connectorConfigUpdater, airbyteApiClient, catalogClientConverters)
  }

  @Test
  fun `test that message are properly aggregated by type`() {
    every { streamFactory.create(any()) } returns
      Stream.of(
        AirbyteMessage().withType(AirbyteMessage.Type.CONTROL).withAdditionalProperty("control", "one"),
        AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withAdditionalProperty("record", "two"),
        AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withAdditionalProperty("record", "three"),
      )

    val messageByType = ConnectorMessageProcessor.getMessagesByType(InputStream.nullInputStream(), streamFactory)

    assertEquals(2, messageByType.size)
    assertEquals(1, messageByType[AirbyteMessage.Type.CONTROL]!!.size)
    assertEquals(2, messageByType[AirbyteMessage.Type.RECORD]!!.size)
  }

  @Test
  fun `test find first trace message`() {
    val messageByType =
      mapOf(
        AirbyteMessage.Type.TRACE to
          listOf(
            AirbyteMessage()
              .withType(AirbyteMessage.Type.TRACE)
              .withTrace(
                AirbyteTraceMessage()
                  .withType(AirbyteTraceMessage.Type.ERROR)
                  .withAdditionalProperty("trace", "one"),
              ),
          ),
        AirbyteMessage.Type.RECORD to listOf(AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withAdditionalProperty("record", "two")),
      )

    val traceMessage = ConnectorMessageProcessor.getTraceMessageFromMessagesByType(messageByType)

    assert(traceMessage.isPresent)
    assertEquals(traceMessage.get().additionalProperties["trace"], "one")

    val messageByTypeNoTrace =
      mapOf(
        AirbyteMessage.Type.RECORD to listOf(AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withAdditionalProperty("record", "two")),
      )

    val noTraceMessage = ConnectorMessageProcessor.getTraceMessageFromMessagesByType(messageByTypeNoTrace)

    assert(noTraceMessage.isEmpty)
  }

  @Test
  fun `test find connection status`() {
    val messageByTypeSuccessfullConnection =
      mapOf(
        AirbyteMessage.Type.CONNECTION_STATUS to
          listOf(
            AirbyteMessage()
              .withType(AirbyteMessage.Type.CONNECTION_STATUS)
              .withConnectionStatus(
                AirbyteConnectionStatus()
                  .withStatus(AirbyteConnectionStatus.Status.SUCCEEDED),
              ),
          ),
        AirbyteMessage.Type.RECORD to listOf(AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withAdditionalProperty("record", "two")),
      )

    val successFullConnection = ConnectorMessageProcessor.getConnectionStatus(messageByTypeSuccessfullConnection)

    assert(successFullConnection.connectionStatus != null)
    assertEquals(AirbyteConnectionStatus.Status.SUCCEEDED, successFullConnection.connectionStatus!!.status)

    val messageByTypeFailedConnection =
      mapOf(
        AirbyteMessage.Type.CONNECTION_STATUS to
          listOf(
            AirbyteMessage()
              .withType(AirbyteMessage.Type.CONNECTION_STATUS)
              .withConnectionStatus(
                AirbyteConnectionStatus()
                  .withStatus(AirbyteConnectionStatus.Status.FAILED),
              ),
          ),
        AirbyteMessage.Type.RECORD to listOf(AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withAdditionalProperty("record", "two")),
      )

    val failedConnection = ConnectorMessageProcessor.getConnectionStatus(messageByTypeFailedConnection)

    assert(failedConnection.connectionStatus != null)
    assertEquals(AirbyteConnectionStatus.Status.FAILED, failedConnection.connectionStatus!!.status)

    val messageByTypeMissingConnectionStatus =
      mapOf(
        AirbyteMessage.Type.RECORD to listOf(AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withAdditionalProperty("record", "two")),
      )

    val missingConnection = ConnectorMessageProcessor.getConnectionStatus(messageByTypeMissingConnectionStatus)

    assert(missingConnection.connectionStatus == null)
  }

  @Test
  fun `test find catalog discovery`() {
    val catalog =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.CATALOG)
        .withCatalog(
          AirbyteCatalog()
            .withStreams(
              listOf(
                AirbyteStream().withName("name"),
              ),
            ),
        )
    val messageByTypeSuccessfullConnection =
      mapOf(
        AirbyteMessage.Type.CATALOG to
          listOf(catalog),
        AirbyteMessage.Type.RECORD to listOf(AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withAdditionalProperty("record", "two")),
      )

    val successFullConnection = ConnectorMessageProcessor.getDiscoveryResult(messageByTypeSuccessfullConnection)

    assert(successFullConnection.catalog != null)
    assertEquals(catalog.catalog, successFullConnection.catalog)

    val messageByTypeMissingConnectionStatus =
      mapOf(
        AirbyteMessage.Type.RECORD to listOf(AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withAdditionalProperty("record", "two")),
      )

    val missingConnection = ConnectorMessageProcessor.getConnectionStatus(messageByTypeMissingConnectionStatus)

    assert(missingConnection.catalog == null)
  }

  @Test
  fun `test find specs`() {
    val specsMessage =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.SPEC)
        .withSpec(
          ConnectorSpecification()
            .withProtocolVersion("test"),
        )
    val messageByTypeSuccessfullConnection =
      mapOf(
        AirbyteMessage.Type.SPEC to
          listOf(specsMessage),
        AirbyteMessage.Type.RECORD to listOf(AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withAdditionalProperty("record", "two")),
      )

    val expectedSpecs = ConnectorMessageProcessor.getSpecResult(messageByTypeSuccessfullConnection)

    assert(expectedSpecs.spec != null)
    assertEquals(specsMessage.spec, expectedSpecs.spec)

    val messageByTypeMissingConnectionStatus =
      mapOf(
        AirbyteMessage.Type.RECORD to listOf(AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withAdditionalProperty("record", "two")),
      )

    val missingSpecs = ConnectorMessageProcessor.getSpecResult(messageByTypeMissingConnectionStatus)

    assert(missingSpecs.spec == null)
  }

  data class ConnectorUpdateInput(
    val actorId: UUID,
    val actorType: ActorType,
    val messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>,
    val inputConfig: JsonNode,
    val jobOutput: ConnectorJobOutput,
  )

  private fun getConnectorUpdateInputWithRandomInputConfig(
    actorType: ActorType,
    airbyteControlConnectorConfigMessage: AirbyteControlConnectorConfigMessage,
  ): ConnectorUpdateInput =
    getConnectorUpdateInput(actorType, airbyteControlConnectorConfigMessage, Jsons.jsonNode(mapOf("random" to UUID.randomUUID().toString())))

  private fun getConnectorUpdateInput(
    actorType: ActorType,
    airbyteControlConnectorConfigMessage: AirbyteControlConnectorConfigMessage,
    inputConfig: JsonNode,
  ): ConnectorUpdateInput {
    val messageByType =
      mapOf(
        AirbyteMessage.Type.CONTROL to
          listOf(
            AirbyteMessage()
              .withType(AirbyteMessage.Type.CONTROL)
              .withControl(
                AirbyteControlMessage()
                  .withType(AirbyteControlMessage.Type.CONNECTOR_CONFIG)
                  .withConnectorConfig(airbyteControlConnectorConfigMessage),
              ),
          ),
      )

    val actorId = UUID.randomUUID()

    val jobOutput = ConnectorJobOutput()

    return ConnectorUpdateInput(
      actorId,
      actorType,
      messageByType,
      inputConfig,
      jobOutput,
    )
  }

  @Test
  fun `properly update source`() {
    val controlMessageConfig = AirbyteControlConnectorConfigMessage().withConfig(Config().withAdditionalProperty("config", "config"))

    val (actorId, actorType, messageByType, inputConfig, jobOutput) =
      getConnectorUpdateInputWithRandomInputConfig(
        ActorType.SOURCE,
        controlMessageConfig,
      )

    every {
      connectorConfigUpdater.updateSource(
        actorId,
        controlMessageConfig.config,
      )
    } returns Unit

    connectorMessageProcessor.updateConfigFromControlMessage(actorId, actorType, messageByType, inputConfig, jobOutput)

    verify {
      connectorConfigUpdater.updateSource(
        actorId,
        controlMessageConfig.config,
      )
    }

    assert(jobOutput.connectorConfigurationUpdated)
  }

  @Test
  fun `properly update destination`() {
    val controlMessageConfig = AirbyteControlConnectorConfigMessage().withConfig(Config().withAdditionalProperty("config", "config"))

    val (actorId, actorType, messageByType, inputConfig, jobOutput) =
      getConnectorUpdateInputWithRandomInputConfig(
        ActorType.DESTINATION,
        controlMessageConfig,
      )

    every {
      connectorConfigUpdater.updateDestination(
        actorId,
        controlMessageConfig.config,
      )
    } returns Unit

    connectorMessageProcessor.updateConfigFromControlMessage(actorId, actorType, messageByType, inputConfig, jobOutput)

    verify {
      connectorConfigUpdater.updateDestination(
        actorId,
        controlMessageConfig.config,
      )
    }

    assert(jobOutput.connectorConfigurationUpdated)
  }

  @Test
  fun `don't update connector if there is no change`() {
    val controlMessageConfig = AirbyteControlConnectorConfigMessage().withConfig(Config().withAdditionalProperty("config", "config"))

    val (actorId, actorType, messageByType, inputConfig, jobOutput) =
      getConnectorUpdateInput(
        ActorType.SOURCE,
        controlMessageConfig,
        Jsons.jsonNode(controlMessageConfig.config),
      )

    connectorMessageProcessor.updateConfigFromControlMessage(actorId, actorType, messageByType, inputConfig, jobOutput)

    assertFalse(jobOutput.connectorConfigurationUpdated)
  }

  private fun airbyteMessagesToInputStream(airbyteMessages: List<AirbyteMessage>): InputStream {
    val messageAsString =
      airbyteMessages
        .map { "${Jsons.serialize(it)}${System.lineSeparator()}" }

    val byteArrayOutputStream = ByteArrayOutputStream()

    for (line in messageAsString) {
      byteArrayOutputStream.write(line.toByteArray())
    }

    val bytes: ByteArray = byteArrayOutputStream.toByteArray()

    return ByteArrayInputStream(bytes)
  }

  @Test
  fun `fail if non 0 exit code`() {
    every { streamFactory.create(any()) } returns Stream.of()

    val jobOutput =
      connectorMessageProcessor.run(
        InputStream.nullInputStream(),
        streamFactory,
        ConnectorMessageProcessor.OperationInput(
          StandardCheckConnectionInput()
            .withActorType(ActorType.SOURCE)
            .withActorId(UUID.randomUUID())
            .withConnectionConfiguration(Jsons.emptyObject()),
        ),
        1,
        SidecarInput.OperationType.CHECK,
      )

    assertTrue(jobOutput.failureReason != null)
    assertEquals(StandardCheckConnectionOutput.Status.FAILED, jobOutput.checkConnection.status)
  }

  @ParameterizedTest
  @EnumSource(value = SidecarInput.OperationType::class)
  fun `do not override failure reason`(operationType: SidecarInput.OperationType) {
    val operationInput =
      if (operationType == SidecarInput.OperationType.CHECK) {
        ConnectorMessageProcessor.OperationInput(checkInput = StandardCheckConnectionInput().withActorType(ActorType.SOURCE))
      } else {
        ConnectorMessageProcessor.OperationInput()
      }
    val failureReason = FailureReason().withExternalMessage("test")
    val jobOutput = ConnectorJobOutput()
    connectorMessageProcessor.setOutput(
      operationType,
      ConnectorMessageProcessor.OperationResult(),
      jobOutput,
      Optional.of(failureReason),
      operationInput,
      1,
    )

    assertEquals(failureReason, jobOutput.failureReason)
  }

  @Test
  fun `properly make connection successful`() {
    every { streamFactory.create(any()) } returns
      Stream.of(
        AirbyteMessage()
          .withType(AirbyteMessage.Type.CONNECTION_STATUS)
          .withConnectionStatus(
            AirbyteConnectionStatus()
              .withStatus(AirbyteConnectionStatus.Status.SUCCEEDED)
              .withMessage("working"),
          ),
      )

    val output =
      connectorMessageProcessor.run(
        InputStream.nullInputStream(),
        streamFactory,
        ConnectorMessageProcessor.OperationInput(
          StandardCheckConnectionInput()
            .withConnectionConfiguration(Jsons.emptyObject())
            .withActorId(UUID.randomUUID())
            .withActorType(ActorType.SOURCE),
        ),
        0,
        SidecarInput.OperationType.CHECK,
      )

    assertEquals(StandardCheckConnectionOutput.Status.SUCCEEDED, output.checkConnection.status)
    assertEquals("working", output.checkConnection.message)
  }

  @Test
  fun `properly make connection failed`() {
    every { streamFactory.create(any()) } returns
      Stream.of(
        AirbyteMessage()
          .withType(AirbyteMessage.Type.CONNECTION_STATUS)
          .withConnectionStatus(
            AirbyteConnectionStatus()
              .withStatus(AirbyteConnectionStatus.Status.FAILED)
              .withMessage("broken"),
          ),
      )

    val output =
      connectorMessageProcessor.run(
        InputStream.nullInputStream(),
        streamFactory,
        ConnectorMessageProcessor.OperationInput(
          StandardCheckConnectionInput()
            .withActorId(UUID.randomUUID())
            .withActorType(ActorType.SOURCE)
            .withConnectionConfiguration(Jsons.emptyObject()),
        ),
        0,
        SidecarInput.OperationType.CHECK,
      )

    assertEquals(StandardCheckConnectionOutput.Status.FAILED, output.checkConnection.status)
    assertEquals("broken", output.checkConnection.message)
  }

  @Test
  fun `properly discover schema`() {
    val catalog =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.CATALOG)
        .withCatalog(
          AirbyteCatalog()
            .withStreams(
              listOf(
                AirbyteStream().withName("name"),
              ),
            ),
        )

    every { streamFactory.create(any()) } returns
      Stream.of(
        catalog,
      )

    val discoveredCatalogId = UUID.randomUUID()
    every { sourceApi.writeDiscoverCatalogResult(any()) } returns DiscoverCatalogResult(catalogId = discoveredCatalogId)

    val output =
      connectorMessageProcessor.run(
        InputStream.nullInputStream(),
        streamFactory,
        ConnectorMessageProcessor.OperationInput(
          discoveryInput =
            StandardDiscoverCatalogInput()
              .withConnectionConfiguration(Jsons.emptyObject())
              .withSourceId(UUID.randomUUID().toString()),
        ),
        0,
        SidecarInput.OperationType.DISCOVER,
      )

    assertEquals(discoveredCatalogId, output.discoverCatalogId)
  }

  @Test
  fun `properly spec connector`() {
    val specMessage =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.SPEC)
        .withSpec(
          ConnectorSpecification()
            .withProtocolVersion("test"),
        )

    every { streamFactory.create(any()) } returns
      Stream.of(
        specMessage,
      )

    val output =
      connectorMessageProcessor.run(
        InputStream.nullInputStream(),
        streamFactory,
        ConnectorMessageProcessor.OperationInput(
          discoveryInput =
            StandardDiscoverCatalogInput()
              .withConnectionConfiguration(Jsons.emptyObject())
              .withSourceId(UUID.randomUUID().toString()),
        ),
        0,
        SidecarInput.OperationType.SPEC,
      )

    assertEquals(specMessage.spec, output.spec)
  }
}
