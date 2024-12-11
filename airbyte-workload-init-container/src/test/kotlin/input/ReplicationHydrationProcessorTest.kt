package io.airbyte.initContainer.input

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.protocol.ProtocolSerializer
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.State
import io.airbyte.config.SyncMode
import io.airbyte.initContainer.system.FileClient
import io.airbyte.mappers.transformations.DestinationCatalogGenerator
import io.airbyte.metrics.lib.MetricClient
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
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.stream.Stream

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

  @MockK
  lateinit var destinationCatalogGenerator: DestinationCatalogGenerator

  @MockK
  lateinit var metricClient: MetricClient

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
        destinationCatalogGenerator,
        metricClient,
      )
  }

  @ParameterizedTest
  @MethodSource("stateMatrix")
  fun `parses input, hydrates, and writes output to expected files`(
    state: State?,
    timesStateFileWritten: Int,
  ) {
    val input = Fixtures.workload
    val catalog =
      ConfiguredAirbyteCatalog(
        listOf(
          ConfiguredAirbyteStream(
            AirbyteStream("test", Jsons.emptyObject(), listOf(SyncMode.FULL_REFRESH)),
          ),
        ),
      )
    val activityInput =
      ReplicationActivityInput(
        connectionId = UUID.randomUUID(),
      )
    val hydrated =
      ReplicationInput()
        .withDestinationLauncherConfig(IntegrationLauncherConfig())
        .withSourceLauncherConfig(IntegrationLauncherConfig())
        .withSourceConfiguration(Jsons.jsonNode(mapOf("src" to "value-1")))
        .withDestinationConfiguration(Jsons.jsonNode(mapOf("dest" to "value-2")))
        .withDestinationSupportsRefreshes(true)
        .withCatalog(catalog)
        .withPrefix("dest_test") // for validating the mapper ran
        .withState(state)
        .withConnectionId(activityInput.connectionId)
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
    every { serializer.serialize(hydrated.state?.state) } returns serializedState
    every { protocolSerializer.serialize(hydrated.catalog, false) } returns serializedSrcCatalog
    every { protocolSerializer.serialize(mapper.mapCatalog(hydrated.catalog), hydrated.destinationSupportsRefreshes) } returns serializedDestCatalog
    every {
      destinationCatalogGenerator.generateDestinationCatalog(any())
    } returns DestinationCatalogGenerator.CatalogGenerationResult(hydrated.catalog, mapOf())

    processor.process(input)

    verify { deserializer.toReplicationActivityInput(input.inputPayload) }
    verify { replicationInputHydrator.getHydratedReplicationInput(activityInput) }
    verify { serializer.serialize(hydrated) }
    verify { fileClient.writeInputFile(FileConstants.INIT_INPUT_FILE, serializedReplInput) }
    verify { serializer.serialize(hydrated.sourceConfiguration) }
    verify { serializer.serialize(hydrated.destinationConfiguration) }
    verify { protocolSerializer.serialize(hydrated.catalog, false) }
    verify { protocolSerializer.serialize(mapper.mapCatalog(hydrated.catalog), hydrated.destinationSupportsRefreshes) }
    verify { fileClient.writeInputFile(FileConstants.CATALOG_FILE, serializedSrcCatalog, FileConstants.SOURCE_DIR) }
    verify { fileClient.writeInputFile(FileConstants.CONNECTOR_CONFIG_FILE, serializedSrcConfig, FileConstants.SOURCE_DIR) }
    verify(exactly = timesStateFileWritten) { fileClient.writeInputFile(FileConstants.INPUT_STATE_FILE, serializedState, FileConstants.SOURCE_DIR) }
    verify { fileClient.writeInputFile(FileConstants.CATALOG_FILE, serializedDestCatalog, FileConstants.DEST_DIR) }
    verify { fileClient.writeInputFile(FileConstants.CONNECTOR_CONFIG_FILE, serializedDestConfig, FileConstants.DEST_DIR) }
    verify { fileClient.makeNamedPipes() }
  }

  companion object {
    // Validates empty or null states serialize as "{}"
    @JvmStatic
    private fun stateMatrix(): Stream<Arguments> =
      Stream.of(
        Arguments.of(State().withState(null), 0),
        Arguments.of(null, 0),
        Arguments.of(State().withState(Jsons.jsonNode("this is" to "nested for some reason")), 1),
      )
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
