package io.airbyte.initContainer.input

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.protocol.ProtocolSerializer
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.SyncMode
import io.airbyte.initContainer.system.FileClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.internal.NamespacingMapper
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.serde.ObjectSerializer
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workload.api.client.model.generated.Workload
import io.airbyte.workload.api.client.model.generated.WorkloadType
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.UUID

@ExtendWith(MockKExtension::class)
class ReplicationHydrationProcessorTest {
  @MockK
  lateinit var replicationInputHydrator: ReplicationInputHydrator

  @MockK
  lateinit var serializer: ObjectSerializer

  @MockK
  lateinit var deserializer: PayloadDeserializer

  @MockK
  lateinit var protocolSerializer: ProtocolSerializer

  @MockK(relaxed = true)
  lateinit var fileClient: FileClient

  private lateinit var processor: ReplicationHydrationProcessor

  @BeforeEach
  fun setup() {
    processor =
      ReplicationHydrationProcessor(
        replicationInputHydrator,
        deserializer,
        serializer,
        protocolSerializer,
        fileClient,
      )
  }

  @Test
  fun `parses input, hydrates, and writes output to expected files`() {
    val input = Fixtures.workload
    val catalog =
      ConfiguredAirbyteCatalog(
        listOf(
          ConfiguredAirbyteStream(
            AirbyteStream("test", Jsons.emptyObject(), listOf(SyncMode.FULL_REFRESH)),
          ),
        ),
      )
    val activityInput = ReplicationActivityInput()
    val hydrated =
      ReplicationInput()
        .withDestinationLauncherConfig(IntegrationLauncherConfig())
        .withSourceLauncherConfig(IntegrationLauncherConfig())
        .withSourceConfiguration(Jsons.jsonNode(mapOf("src" to "value-1")))
        .withDestinationConfiguration(Jsons.jsonNode(mapOf("dest" to "value-2")))
        .withDestinationSupportsRefreshes(true)
        .withCatalog(catalog)
        .withPrefix("dest_test") // for validating the mapper ran
    val mapper =
      NamespacingMapper(
        null,
        null,
        hydrated.prefix,
      )

    val serializedReplInput = "serialized hydrated blob"
    val serializedSrcCatalog = "serialized src catalog"
    val serializedSrcConfig = "serialized src config"
    val serializedDestCatalog = "serialized dest catalog"
    val serializedDestConfig = "serialized dest config"
    val serializedState = "serialized state config"

    every { deserializer.toReplicationActivityInput(input.inputPayload) } returns activityInput
    every { replicationInputHydrator.getHydratedReplicationInput(activityInput) } returns hydrated
    every { serializer.serialize(hydrated) } returns serializedReplInput
    every { serializer.serialize(hydrated.sourceConfiguration) } returns serializedSrcConfig
    every { serializer.serialize(hydrated.destinationConfiguration) } returns serializedDestConfig
    every { serializer.serialize(hydrated.state) } returns serializedState
    every { protocolSerializer.serialize(hydrated.catalog, false) } returns serializedSrcCatalog
    every { protocolSerializer.serialize(mapper.mapCatalog(hydrated.catalog), hydrated.destinationSupportsRefreshes) } returns serializedDestCatalog

    processor.process(input)

    verify { deserializer.toReplicationActivityInput(input.inputPayload) }
    verify { replicationInputHydrator.getHydratedReplicationInput(activityInput) }
    verify { serializer.serialize(hydrated) }
    verify { fileClient.writeInputFile(FileConstants.INIT_INPUT_FILE, serializedReplInput) }
    verify { serializer.serialize(hydrated.sourceConfiguration) }
    verify { serializer.serialize(hydrated.destinationConfiguration) }
    verify { serializer.serialize(hydrated.state) }
    verify { protocolSerializer.serialize(hydrated.catalog, false) }
    verify { protocolSerializer.serialize(mapper.mapCatalog(hydrated.catalog), hydrated.destinationSupportsRefreshes) }
    verify { fileClient.writeInputFile(FileConstants.SOURCE_CATALOG_FILE, serializedSrcCatalog) }
    verify { fileClient.writeInputFile(FileConstants.SOURCE_CONFIG_FILE, serializedSrcConfig) }
    verify { fileClient.writeInputFile(FileConstants.DESTINATION_CATALOG_FILE, serializedDestCatalog) }
    verify { fileClient.writeInputFile(FileConstants.DESTINATION_CONFIG_FILE, serializedDestConfig) }
    verify { fileClient.writeInputFile(FileConstants.INPUT_STATE_FILE, serializedState) }
    verify { fileClient.makeNamedPipes() }
  }

  object Fixtures {
    private const val WORKLOAD_ID = "workload-id-13"

    val workload =
      Workload(
        WORKLOAD_ID,
        listOf(),
        "inputPayload",
        "logPath",
        "geography",
        WorkloadType.SYNC,
        UUID.randomUUID(),
      )
  }
}
