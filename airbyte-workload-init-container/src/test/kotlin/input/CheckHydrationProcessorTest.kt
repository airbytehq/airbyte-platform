package io.airbyte.initContainer.input

import io.airbyte.commons.json.Jsons
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.initContainer.system.FileClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.workers.CheckConnectionInputHydrator
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.SidecarInput
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
class CheckHydrationProcessorTest {
  @MockK
  lateinit var inputHydrator: CheckConnectionInputHydrator

  @MockK
  lateinit var serializer: ObjectSerializer

  @MockK
  lateinit var deserializer: PayloadDeserializer

  @MockK
  lateinit var fileClient: FileClient

  private lateinit var processor: CheckHydrationProcessor

  @BeforeEach
  fun setup() {
    processor =
      CheckHydrationProcessor(
        inputHydrator,
        deserializer,
        serializer,
        fileClient,
      )
  }

  @Test
  fun `parses input, hydrates and writes output to expected file`() {
    val input = Fixtures.workload

    val unhydrated = StandardCheckConnectionInput()
    val parsed = CheckConnectionInput()
    parsed.checkConnectionInput = unhydrated
    parsed.launcherConfig = IntegrationLauncherConfig()

    val connectionConfiguration = Jsons.jsonNode(mapOf("key-1" to "value-1"))
    val hydrated = StandardCheckConnectionInput()
    hydrated.connectionConfiguration = connectionConfiguration
    val serializedConfig = "serialized hydrated config"
    val serializedInput = "serialized hydrated blob"

    val sidecarInput =
      SidecarInput(
        hydrated,
        null,
        input.id,
        parsed.launcherConfig,
        SidecarInput.OperationType.CHECK,
        input.logPath,
      )

    every { deserializer.toCheckConnectionInput(input.inputPayload) } returns parsed
    every { inputHydrator.getHydratedStandardCheckInput(unhydrated) } returns hydrated
    every { serializer.serialize(connectionConfiguration) } returns serializedConfig
    every { serializer.serialize(sidecarInput) } returns serializedInput
    every { fileClient.writeInputFile(FileConstants.CONNECTION_CONFIGURATION_FILE, serializedConfig) } returns Unit
    every { fileClient.writeInputFile(FileConstants.SIDECAR_INPUT_FILE, serializedInput) } returns Unit

    processor.process(input)

    verify { deserializer.toCheckConnectionInput(input.inputPayload) }
    verify { inputHydrator.getHydratedStandardCheckInput(unhydrated) }
    verify { serializer.serialize(connectionConfiguration) }
    verify { serializer.serialize(sidecarInput) }
    verify { fileClient.writeInputFile(FileConstants.CONNECTION_CONFIGURATION_FILE, serializedConfig) }
    verify { fileClient.writeInputFile(FileConstants.SIDECAR_INPUT_FILE, serializedInput) }
  }

  object Fixtures {
    private const val WORKLOAD_ID = "workload-id-14"

    val workload =
      Workload(
        WORKLOAD_ID,
        listOf(),
        "inputPayload",
        "logPath",
        "geography",
        WorkloadType.CHECK,
        UUID.randomUUID(),
      )
  }
}
