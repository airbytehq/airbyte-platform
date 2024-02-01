package io.airbyte.connectorSidecar

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.converters.ConnectorConfigUpdater
import io.airbyte.commons.json.Jsons
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.protocol.models.AirbyteConnectionStatus
import io.airbyte.protocol.models.AirbyteControlConnectorConfigMessage
import io.airbyte.protocol.models.AirbyteControlMessage
import io.airbyte.protocol.models.AirbyteMessage
import io.airbyte.protocol.models.AirbyteTraceMessage
import io.airbyte.protocol.models.Config
import io.airbyte.workers.exception.WorkerException
import io.airbyte.workers.internal.AirbyteStreamFactory
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.util.UUID
import java.util.stream.Stream

@ExtendWith(MockKExtension::class)
class ConnectorMessageProcessorTest {
  @MockK
  private lateinit var streamFactory: AirbyteStreamFactory

  @MockK
  private lateinit var connectorConfigUpdater: ConnectorConfigUpdater

  private lateinit var connectorMessageProcessor: ConnectorMessageProcessor

  @BeforeEach
  fun init() {
    connectorMessageProcessor = ConnectorMessageProcessor(connectorConfigUpdater)
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
    assertEquals(1, messageByType.get(AirbyteMessage.Type.CONTROL)!!.size)
    assertEquals(2, messageByType.get(AirbyteMessage.Type.RECORD)!!.size)
  }

  @Test
  fun `test find first trace message`() {
    val messageByType =
      mapOf(
        AirbyteMessage.Type.TRACE to
          listOf(
            AirbyteMessage().withType(AirbyteMessage.Type.TRACE)
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
            AirbyteMessage().withType(AirbyteMessage.Type.CONNECTION_STATUS)
              .withConnectionStatus(
                AirbyteConnectionStatus()
                  .withStatus(AirbyteConnectionStatus.Status.SUCCEEDED),
              ),
          ),
        AirbyteMessage.Type.RECORD to listOf(AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withAdditionalProperty("record", "two")),
      )

    val successFullConnection = ConnectorMessageProcessor.getConnectionStatus(messageByTypeSuccessfullConnection)

    assert(successFullConnection.isPresent)
    assertEquals(AirbyteConnectionStatus.Status.SUCCEEDED, successFullConnection.get().status)

    val messageByTypeFailedConnection =
      mapOf(
        AirbyteMessage.Type.CONNECTION_STATUS to
          listOf(
            AirbyteMessage().withType(AirbyteMessage.Type.CONNECTION_STATUS)
              .withConnectionStatus(
                AirbyteConnectionStatus()
                  .withStatus(AirbyteConnectionStatus.Status.FAILED),
              ),
          ),
        AirbyteMessage.Type.RECORD to listOf(AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withAdditionalProperty("record", "two")),
      )

    val failedConnection = ConnectorMessageProcessor.getConnectionStatus(messageByTypeFailedConnection)

    assert(failedConnection.isPresent)
    assertEquals(AirbyteConnectionStatus.Status.FAILED, failedConnection.get().status)

    val messageByTypeMissingConnectionStatus =
      mapOf(
        AirbyteMessage.Type.RECORD to listOf(AirbyteMessage().withType(AirbyteMessage.Type.RECORD).withAdditionalProperty("record", "two")),
      )

    val missingConnection = ConnectorMessageProcessor.getConnectionStatus(messageByTypeMissingConnectionStatus)

    assert(missingConnection.isEmpty)
  }

  data class ConnectorUpdateInput(
    val input: StandardCheckConnectionInput,
    val messagesByType: Map<AirbyteMessage.Type, List<AirbyteMessage>>,
    val inputConfig: JsonNode,
    val jobOutput: ConnectorJobOutput,
  )

  private fun getConnectorUpdateInputWithRandomInputConfig(
    actorType: ActorType,
    airbyteControlConnectorConfigMessage: AirbyteControlConnectorConfigMessage,
  ): ConnectorUpdateInput {
    return getConnectorUpdateInput(actorType, airbyteControlConnectorConfigMessage, Jsons.jsonNode(mapOf("random" to UUID.randomUUID().toString())))
  }

  private fun getConnectorUpdateInput(
    actorType: ActorType,
    airbyteControlConnectorConfigMessage: AirbyteControlConnectorConfigMessage,
    inputConfig: JsonNode,
  ): ConnectorUpdateInput {
    val messageByType =
      mapOf(
        AirbyteMessage.Type.CONTROL to
          listOf(
            AirbyteMessage().withType(AirbyteMessage.Type.CONTROL)
              .withControl(
                AirbyteControlMessage()
                  .withType(AirbyteControlMessage.Type.CONNECTOR_CONFIG)
                  .withConnectorConfig(airbyteControlConnectorConfigMessage),
              ),
          ),
      )

    val actorId = UUID.randomUUID()

    val input =
      StandardCheckConnectionInput()
        .withActorId(actorId)
        .withActorType(actorType)

    val jobOutput = ConnectorJobOutput()

    return ConnectorUpdateInput(
      input,
      messageByType,
      inputConfig,
      jobOutput,
    )
  }

  @Test
  fun `properly update source`() {
    val controlMessageConfig = AirbyteControlConnectorConfigMessage().withConfig(Config().withAdditionalProperty("config", "config"))

    val (input, messageByType, inputConfig, jobOutput) = getConnectorUpdateInputWithRandomInputConfig(ActorType.SOURCE, controlMessageConfig)

    every {
      connectorConfigUpdater.updateSource(
        input.actorId,
        controlMessageConfig.config,
      )
    } returns Unit

    connectorMessageProcessor.updateConfigFromControlMessage(input, messageByType, inputConfig, jobOutput)

    verify {
      connectorConfigUpdater.updateSource(
        input.actorId,
        controlMessageConfig.config,
      )
    }

    assert(jobOutput.connectorConfigurationUpdated)
  }

  @Test
  fun `properly update destination`() {
    val controlMessageConfig = AirbyteControlConnectorConfigMessage().withConfig(Config().withAdditionalProperty("config", "config"))

    val (input, messageByType, inputConfig, jobOutput) = getConnectorUpdateInputWithRandomInputConfig(ActorType.DESTINATION, controlMessageConfig)

    every {
      connectorConfigUpdater.updateDestination(
        input.actorId,
        controlMessageConfig.config,
      )
    } returns Unit

    connectorMessageProcessor.updateConfigFromControlMessage(input, messageByType, inputConfig, jobOutput)

    verify {
      connectorConfigUpdater.updateDestination(
        input.actorId,
        controlMessageConfig.config,
      )
    }

    assert(jobOutput.connectorConfigurationUpdated)
  }

  @Test
  fun `don't update connector if there is no change`() {
    val controlMessageConfig = AirbyteControlConnectorConfigMessage().withConfig(Config().withAdditionalProperty("config", "config"))

    val (input, messageByType, inputConfig, jobOutput) =
      getConnectorUpdateInput(
        ActorType.SOURCE,
        controlMessageConfig,
        Jsons.jsonNode(controlMessageConfig.config),
      )

    connectorMessageProcessor.updateConfigFromControlMessage(input, messageByType, inputConfig, jobOutput)

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
    assertThrows<WorkerException> {
      connectorMessageProcessor.runCheck(
        InputStream.nullInputStream(),
        streamFactory,
        StandardCheckConnectionInput(),
        1,
      )
    }
  }

  @Test
  fun `properly make connection successful`() {
    every { streamFactory.create(any()) } returns
      Stream.of(
        AirbyteMessage().withType(AirbyteMessage.Type.CONNECTION_STATUS)
          .withConnectionStatus(
            AirbyteConnectionStatus()
              .withStatus(AirbyteConnectionStatus.Status.SUCCEEDED)
              .withMessage("working"),
          ),
      )

    val output =
      connectorMessageProcessor.runCheck(
        InputStream.nullInputStream(),
        streamFactory,
        StandardCheckConnectionInput()
          .withConnectionConfiguration(Jsons.emptyObject()),
        0,
      )

    assertEquals(StandardCheckConnectionOutput.Status.SUCCEEDED, output.checkConnection.status)
    assertEquals("working", output.checkConnection.message)
  }

  @Test
  fun `properly make connection failed`() {
    every { streamFactory.create(any()) } returns
      Stream.of(
        AirbyteMessage().withType(AirbyteMessage.Type.CONNECTION_STATUS)
          .withConnectionStatus(
            AirbyteConnectionStatus()
              .withStatus(AirbyteConnectionStatus.Status.FAILED)
              .withMessage("broken"),
          ),
      )

    val output =
      connectorMessageProcessor.runCheck(
        InputStream.nullInputStream(),
        streamFactory,
        StandardCheckConnectionInput()
          .withConnectionConfiguration(Jsons.emptyObject()),
        0,
      )

    assertEquals(StandardCheckConnectionOutput.Status.FAILED, output.checkConnection.status)
    assertEquals("broken", output.checkConnection.message)
  }
}
