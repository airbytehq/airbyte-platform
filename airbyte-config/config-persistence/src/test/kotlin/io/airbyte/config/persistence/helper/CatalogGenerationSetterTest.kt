package io.airbyte.config.persistence.helper

import io.airbyte.config.persistence.domain.Generation
import io.airbyte.protocol.models.AirbyteStream
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.ConfiguredAirbyteStream
import io.airbyte.protocol.models.StreamDescriptor
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class CatalogGenerationSetterTest {
  private val catalogGenerationSetter = CatalogGenerationSetter()

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
  }

  @Test
  fun `test that no refresh truncation is performed if there is no refresh`() {
    val updatedCatalog =
      catalogGenerationSetter.updateCatalogWithGenerationAndSyncInformation(
        catalog = catalog,
        jobId = jobId,
        streamRefreshes = listOf(),
        generations = generations,
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
        jobId = jobId,
        streamRefreshes =
          listOf(
            StreamDescriptor().withName("name1").withNamespace("namespace1"),
            StreamDescriptor().withName("name2").withNamespace("namespace2"),
          ),
        generations = generations,
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
        jobId = jobId,
        streamRefreshes = listOf(StreamDescriptor().withName("name1").withNamespace("namespace1")),
        generations = generations,
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
