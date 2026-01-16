/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.state

import io.airbyte.commons.json.Jsons
import io.airbyte.container.orchestrator.bookkeeping.ParallelStreamStatsTracker
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateStats
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.mockk.every
import io.mockk.impl.annotations.MockK
import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

@ExtendWith(MockKExtension::class)
class StateEnricherTest {
  @MockK(relaxed = true)
  private lateinit var statsTracker: ParallelStreamStatsTracker

  private lateinit var stateEnricher: StateEnricher

  @BeforeEach
  fun setup() {
    stateEnricher = StateEnricher(statsTracker)
  }

  @ParameterizedTest
  @CsvSource("RECORD", "LOG", "SPEC", "CONNECTION_STATUS", "CATALOG", "TRACE", "CONTROL", "DESTINATION_CATALOG")
  fun `should only enrich state messages`(msgType: String) {
    val type = AirbyteMessage.Type.valueOf(msgType)
    val message =
      AirbyteMessage()
        .withType(type)

    val result = stateEnricher.enrich(message)
    assertEquals(message, result)
  }

  @Test
  fun `should add ID to state message`() {
    val stateMessage = Fixtures.streamState()
    val message =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(stateMessage)

    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns 100L
    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns null

    val result = stateEnricher.enrich(message)

    assertNotNull(result.state.additionalProperties)
    assertTrue(result.state.additionalProperties.containsKey(ID))
  }

  @Test
  fun `should add source stats when not present`() {
    val stateMessage =
      Fixtures
        .streamState()
        .withSourceStats(null)
    val message =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(stateMessage)

    val expectedRecordCount = 150L
    every { statsTracker.getEmittedCountForCurrentState(any<AirbyteStateMessage>()) } returns expectedRecordCount
    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns null

    val result = stateEnricher.enrich(message)

    assertNotNull(result.state.sourceStats)
    assertEquals(expectedRecordCount.toDouble(), result.state.sourceStats.recordCount)
  }

  @Test
  fun `should not override existing source stats`() {
    val existingStats =
      AirbyteStateStats()
        .withRecordCount(200.0)
    val stateMessage =
      Fixtures
        .streamState()
        .withSourceStats(existingStats)
    val message =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(stateMessage)

    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns 150L
    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns 10L

    val result = stateEnricher.enrich(message)

    assertEquals(190.0, result.state.sourceStats.recordCount) // 200 - 10 filtered
  }

  @Test
  fun `should default to 0 if missing record count`() {
    val stateMessage =
      Fixtures
        .streamState()
        .withSourceStats(null)
    val message =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(stateMessage)

    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns null
    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns null

    val result = stateEnricher.enrich(message)

    assertEquals(0.0, result.state.sourceStats.recordCount)
  }

  @Test
  fun `should subtract filtered records from count when present`() {
    val existingStats =
      AirbyteStateStats()
        .withRecordCount(100.0)
    val stateMessage =
      Fixtures
        .streamState()
        .withSourceStats(existingStats)
    val message =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(stateMessage)

    val filteredCount = 25L
    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns 100L
    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns filteredCount

    val result = stateEnricher.enrich(message)

    assertEquals(75.0, result.state.sourceStats.recordCount) // 100 - 25 filtered
  }

  @Test
  fun `should not modify record count when no filtered records`() {
    val existingStats =
      AirbyteStateStats()
        .withRecordCount(100.0)
    val stateMessage =
      Fixtures
        .streamState()
        .withSourceStats(existingStats)
    val message =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(stateMessage)

    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns 100L
    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns null

    val result = stateEnricher.enrich(message)

    assertEquals(100.0, result.state.sourceStats.recordCount)
  }

  @Test
  fun `should handle zero filtered records`() {
    val existingStats =
      AirbyteStateStats()
        .withRecordCount(100.0)
    val stateMessage =
      Fixtures
        .streamState()
        .withSourceStats(existingStats)
    val message =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(stateMessage)

    every { statsTracker.getEmittedCountForCurrentState(any<AirbyteStateMessage>()) } returns 100L
    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns 0L

    val result = stateEnricher.enrich(message)

    assertEquals(100.0, result.state.sourceStats.recordCount) // 100 - 0 filtered
  }

  @Test
  fun `should handle large record counts`() {
    val stateMessage =
      Fixtures
        .streamState()
        .withSourceStats(null)
    val message =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(stateMessage)

    val largeCount = Long.MAX_VALUE
    every { statsTracker.getEmittedCountForCurrentState(any<AirbyteStateMessage>()) } returns largeCount
    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns null

    val result = stateEnricher.enrich(message)

    assertEquals(largeCount.toDouble(), result.state.sourceStats.recordCount)
  }

  @Test
  fun `should throw when filtered count greater than record count`() {
    val existingStats =
      AirbyteStateStats()
        .withRecordCount(50.0)
    val stateMessage =
      Fixtures
        .streamState()
        .withSourceStats(existingStats)
    val message =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(stateMessage)

    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns 50L
    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns 75L

    val exception =
      assertThrows(IllegalArgumentException::class.java) {
        stateEnricher.enrich(message)
      }
    assertEquals("More records were filtered (75) than emitted (50.0).", exception.message)
  }

  @Test
  fun `should preserve existing state message fields`() {
    val streamState =
      AirbyteStreamState()
        .withStreamState(Jsons.jsonNode(mapOf("cursor" to "2025-01-01")))
        .withStreamDescriptor(StreamDescriptor().withName("test_stream").withNamespace("test_namespace"))
    val existingStats =
      AirbyteStateStats()
        .withRecordCount(100.0)
    val stateMessage =
      AirbyteStateMessage()
        .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
        .withStream(streamState)
        .withSourceStats(existingStats)
    val message =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(stateMessage)

    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns 100L
    every { statsTracker.getFilteredCountForCurrentState(any<AirbyteStateMessage>()) } returns 10L

    val result = stateEnricher.enrich(message)

    // Verify original fields are preserved
    assertEquals(AirbyteStateMessage.AirbyteStateType.STREAM, result.state.type)
    assertEquals("test_stream", result.state.stream.streamDescriptor.name)
    assertEquals("test_namespace", result.state.stream.streamDescriptor.namespace)
    assertEquals(
      "2025-01-01",
      result.state.stream.streamState
        .get("cursor")
        .asText(),
    )
  }

  object Fixtures {
    fun streamState(): AirbyteStateMessage {
      val streamState =
        AirbyteStreamState()
          .withStreamState(Jsons.jsonNode(mapOf("cursor" to "value")))
          .withStreamDescriptor(StreamDescriptor().withName("stream_name").withNamespace("namespace"))
      return AirbyteStateMessage()
        .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
        .withStream(streamState)
    }
  }
}
