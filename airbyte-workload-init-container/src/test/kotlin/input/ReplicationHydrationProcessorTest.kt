package io.airbyte.initContainer.input

import io.airbyte.initContainer.system.FileClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.input.setDestinationLabels
import io.airbyte.workers.input.setSourceLabels
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.pod.FileConstants
import io.airbyte.workers.pod.PodLabeler
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
  lateinit var fileClient: FileClient

  @MockK
  lateinit var labeler: PodLabeler

  private lateinit var processor: ReplicationHydrationProcessor

  @BeforeEach
  fun setup() {
    processor =
      ReplicationHydrationProcessor(
        replicationInputHydrator,
        deserializer,
        serializer,
        fileClient,
        labeler,
      )

    every { labeler.getSharedLabels(any(), any(), any(), any()) } returns mapOf()
  }

  @Test
  fun `parses input, hydrates, adds labels and writes output to expected file`() {
    val input = Fixtures.workload
    val activityInput = ReplicationActivityInput()
    val hydrated =
      ReplicationInput()
        .withDestinationLauncherConfig(IntegrationLauncherConfig())
        .withSourceLauncherConfig(IntegrationLauncherConfig())
    val serialized = "serialized hydrated blob"
    val labels = mapOf("label-1" to "value-1", "label-7" to "value-7")
    val hydratedWithLabels = hydrated.setDestinationLabels(labels).setSourceLabels(labels)

    every { deserializer.toReplicationActivityInput(input.inputPayload) } returns activityInput
    every { replicationInputHydrator.getHydratedReplicationInput(activityInput) } returns hydrated
    every { labeler.getSharedLabels(any(), any(), any(), any()) } returns labels
    every { serializer.serialize(hydratedWithLabels) } returns serialized
    every { fileClient.writeInputFile(FileConstants.INIT_INPUT_FILE, serialized) } returns Unit

    processor.process(input)

    verify { deserializer.toReplicationActivityInput(input.inputPayload) }
    verify { replicationInputHydrator.getHydratedReplicationInput(activityInput) }
    verify { labeler.getSharedLabels(any(), any(), any(), any()) }
    verify { serializer.serialize(hydratedWithLabels) }
    verify { fileClient.writeInputFile(FileConstants.INIT_INPUT_FILE, serialized) }
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
