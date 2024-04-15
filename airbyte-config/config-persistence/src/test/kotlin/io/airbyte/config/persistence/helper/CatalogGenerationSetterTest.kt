package io.airbyte.config.persistence.helper

import io.airbyte.config.persistence.StreamGenerationRepository
import io.airbyte.config.persistence.domain.Generation
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.protocol.models.AirbyteStream
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.ConfiguredAirbyteStream
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class CatalogGenerationSetterTest {
  private val streamGenerationRepository: StreamGenerationRepository = mockk()

  private val catalogGenerationSetter = CatalogGenerationSetter(streamGenerationRepository)

  private val catalog =
    ConfiguredAirbyteCatalog().withStreams(
      listOf(
        ConfiguredAirbyteStream().withStream(
          AirbyteStream()
            .withName("name1")
            .withNamespace("namespace1"),
        ),
        ConfiguredAirbyteStream().withStream(
          AirbyteStream()
            .withName("name2")
            .withNamespace("namespace2"),
        ),
      ),
    )

  private val generations =
    listOf(
      Generation(
        streamName = "name1",
        streamNamespace = "namespace1",
        generationId = 1L,
      ),
      Generation(
        streamName = "name2",
        streamNamespace = "namespace2",
        generationId = 2L,
      ),
    )

  val jobId = 3L
  val connectionId = UUID.randomUUID()

  @BeforeEach
  fun init() {
    every { streamGenerationRepository.getMaxGenerationOfStreamsForConnectionId(connectionId) } returns generations
  }

  @Test
  fun `test that no refresh truncation is performed if there is no refresh`() {
    val updatedCatalog =
      catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformation(
        catalog = catalog,
        connectionId = connectionId,
        jobId = jobId,
        streamRefreshes = listOf(),
      )

    updatedCatalog.streams.forEach {
      assertEquals(0L, it.minimumGenerationId)
      assertEquals(jobId, it.syncId)
    }
  }

  @Test
  fun `test that truncation are properly requested`() {
    val updatedCatalog =
      catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformation(
        catalog = catalog,
        connectionId = connectionId,
        jobId = jobId,
        streamRefreshes =
          listOf(
            StreamRefresh(connectionId = connectionId, streamName = "name1", streamNamespace = "namespace1"),
            StreamRefresh(connectionId = connectionId, streamName = "name2", streamNamespace = "namespace2"),
          ),
      )

    updatedCatalog.streams.forEach {
      assertEquals(it.generationId, it.minimumGenerationId)
      assertEquals(jobId, it.syncId)
    }
  }

  @Test
  fun `test that truncation are properly requested when partial`() {
    val updatedCatalog =
      catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformation(
        catalog = catalog,
        connectionId = connectionId,
        jobId = jobId,
        streamRefreshes = listOf(StreamRefresh(connectionId = connectionId, streamName = "name1", streamNamespace = "namespace1")),
      )

    updatedCatalog.streams.forEach {
      if (it.stream.name == "name1" && it.stream.namespace == "namespace1") {
        assertEquals(it.generationId, it.minimumGenerationId)
        assertEquals(jobId, it.syncId)
        assertEquals(1L, it.generationId)
      } else {
        assertEquals(0L, it.minimumGenerationId)
        assertEquals(jobId, it.syncId)
        assertEquals(2L, it.generationId)
      }
    }
  }
}
