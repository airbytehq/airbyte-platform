package io.airbyte.initContainer.input

import io.airbyte.initContainer.system.FileClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.SidecarInput
import io.airbyte.workers.models.SpecInput
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
class SpecHydrationProcessorTest {
  @MockK
  lateinit var serializer: ObjectSerializer

  @MockK
  lateinit var deserializer: PayloadDeserializer

  @MockK
  lateinit var fileClient: FileClient

  private lateinit var processor: SpecHydrationProcessor

  @BeforeEach
  fun setup() {
    processor =
      SpecHydrationProcessor(
        deserializer,
        serializer,
        fileClient,
      )
  }

  @Test
  fun `parses input and writes output to expected file`() {
    val input = Fixtures.workload

    val parsed =
      SpecInput(
        jobRunConfig = JobRunConfig(),
        launcherConfig = IntegrationLauncherConfig(),
      )

    val serializedInput = "serialized hydrated blob"

    val sidecarInput =
      SidecarInput(
        null,
        null,
        input.id,
        parsed.launcherConfig,
        SidecarInput.OperationType.SPEC,
        input.logPath,
      )

    every { deserializer.toSpecInput(input.inputPayload) } returns parsed
    every { serializer.serialize(sidecarInput) } returns serializedInput
    every { fileClient.writeInputFile(FileConstants.SIDECAR_INPUT_FILE, serializedInput) } returns Unit

    processor.process(input)

    verify { deserializer.toSpecInput(input.inputPayload) }
    verify { serializer.serialize(sidecarInput) }
    verify { fileClient.writeInputFile(FileConstants.SIDECAR_INPUT_FILE, serializedInput) }
  }

  object Fixtures {
    private const val WORKLOAD_ID = "workload-id-16"

    val workload =
      Workload(
        WORKLOAD_ID,
        listOf(),
        "inputPayload",
        "logPath",
        "geography",
        WorkloadType.SPEC,
        UUID.randomUUID(),
      )
  }
}
