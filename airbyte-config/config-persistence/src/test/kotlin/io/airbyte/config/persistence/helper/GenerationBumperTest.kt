package io.airbyte.config.persistence.helper

import io.airbyte.config.persistence.StreamGenerationRepository
import io.airbyte.config.persistence.domain.Generation
import io.airbyte.config.persistence.domain.StreamGeneration
import io.airbyte.config.persistence.domain.StreamRefresh
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.UUID

class GenerationBumperTest {
  val streamGenerationRepository: StreamGenerationRepository = mockk()

  val generationBumper = GenerationBumper(streamGenerationRepository)

  val connectionId = UUID.randomUUID()
  val jobId = 456L

  val generations =
    listOf(
      Generation(
        streamName = "name1",
        streamNamespace = "namespace1",
        generationId = 42,
      ),
      Generation(
        streamName = "name2",
        streamNamespace = "namespace2",
        generationId = 42,
      ),
    )

  @Test
  fun `increase the generation properly`() {
    every { streamGenerationRepository.getMaxGenerationOfStreamsForConnectionId(connectionId) } returns generations
    val generationSlot = slot<List<StreamGeneration>>()
    every { streamGenerationRepository.saveAll(capture(generationSlot)) } returns listOf()

    generationBumper.updateGenerationForStreams(
      connectionId,
      jobId,
      listOf(
        StreamRefresh(
          connectionId = connectionId,
          streamName = "name1",
          streamNamespace = "namespace1",
        ),
      ),
    )

    val capturedStreamGenerations = generationSlot.captured
    assertEquals(1, capturedStreamGenerations.size)

    val streamGeneration = capturedStreamGenerations[0]
    assertEquals("name1", streamGeneration.streamName)
    assertEquals("namespace1", streamGeneration.streamNamespace)
    assertEquals(43, streamGeneration.generationId)
    assertEquals(jobId, streamGeneration.startJobId)
  }

  @Test
  fun `increase the generation properly if generation is missing`() {
    every { streamGenerationRepository.getMaxGenerationOfStreamsForConnectionId(connectionId) } returns generations
    val generationSlot = slot<List<StreamGeneration>>()
    every { streamGenerationRepository.saveAll(capture(generationSlot)) } returns listOf()

    generationBumper.updateGenerationForStreams(
      connectionId,
      jobId,
      listOf(
        StreamRefresh(
          connectionId = connectionId,
          streamName = "name3",
          streamNamespace = "namespace3",
        ),
      ),
    )

    val capturedStreamGenerations = generationSlot.captured
    assertEquals(1, capturedStreamGenerations.size)

    val streamGeneration = capturedStreamGenerations[0]
    assertEquals("name3", streamGeneration.streamName)
    assertEquals("namespace3", streamGeneration.streamNamespace)
    assertEquals(1L, streamGeneration.generationId)
    assertEquals(jobId, streamGeneration.startJobId)
  }
}
