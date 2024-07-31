package io.airbyte.initContainer.input

import io.airbyte.initContainer.system.FileClient
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.serde.ObjectSerializer
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workers.sync.OrchestratorConstants
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith

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

  private lateinit var processor: ReplicationHydrationProcessor

  @BeforeEach
  fun setup() {
    processor =
      ReplicationHydrationProcessor(
        replicationInputHydrator,
        deserializer,
        serializer,
        fileClient,
      )
  }

  @Test
  fun `parses input, hydrates and writes output to expected file`() {
    val input = "payload blob"
    val activityInput = ReplicationActivityInput()
    val hydrated = ReplicationInput()
    val serialized = "serialized hydrated blob"

    every { deserializer.toReplicationActivityInput(input) } returns activityInput
    every { replicationInputHydrator.getHydratedReplicationInput(activityInput) } returns hydrated
    every { serializer.serialize(hydrated) } returns serialized
    every { fileClient.writeInputFile(OrchestratorConstants.INIT_FILE_INPUT, serialized) } returns Unit

    processor.process(input)

    verify { deserializer.toReplicationActivityInput(input) }
    verify { replicationInputHydrator.getHydratedReplicationInput(activityInput) }
    verify { serializer.serialize(hydrated) }
    verify { fileClient.writeInputFile(OrchestratorConstants.INIT_FILE_INPUT, serialized) }
  }
}
