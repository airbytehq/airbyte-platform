/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping

import com.google.common.hash.Hashing
import io.airbyte.commons.json.Jsons
import io.airbyte.config.FileTransferInformations
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteRecordMessage
import io.airbyte.protocol.models.v0.AirbyteRecordMessageFileReference
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateStats
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test

internal class StatsTrackerTest {
  @Test
  fun `test that state hash code generation ignores state stats fields`() {
    val hashFunction = Hashing.murmur3_32_fixed()
    val destinationStats = AirbyteStateStats().withRecordCount(14.0)
    val sourceStats = AirbyteStateStats().withRecordCount(24.0)
    val streamDescriptor = StreamDescriptor().withName("name").withNamespace("namespace")
    val sharedState = Jsons.jsonNode(mapOf("id" to "12345"))
    val streamState = Jsons.jsonNode(mapOf("col" to "9999"))
    val streamStates = listOf(AirbyteStreamState().withStreamState(streamState).withStreamDescriptor(streamDescriptor))
    val global: AirbyteGlobalState = AirbyteGlobalState().withSharedState(sharedState).withStreamStates(streamStates)

    val globalStateMessageWithStats: AirbyteStateMessage =
      AirbyteStateMessage()
        .withGlobal(global)
        .withSourceStats(sourceStats)
        .withDestinationStats(destinationStats)
    val globalStateMessageWithoutStats: AirbyteStateMessage =
      AirbyteStateMessage()
        .withGlobal(global)
    val legacyStateMessageWithStats: AirbyteStateMessage =
      AirbyteStateMessage()
        .withData(Jsons.jsonNode(mapOf("foo" to "bar")))
        .withSourceStats(sourceStats)
        .withDestinationStats(destinationStats)
    val legacyStateMessageWithoutStats: AirbyteStateMessage =
      AirbyteStateMessage()
        .withData(Jsons.jsonNode(mapOf("foo" to "bar")))
    val perStreamStateMessageWithStats: AirbyteStateMessage =
      AirbyteStateMessage()
        .withStream(
          AirbyteStreamState()
            .withStreamState(streamState)
            .withStreamDescriptor(streamDescriptor),
        ).withSourceStats(sourceStats)
        .withDestinationStats(destinationStats)
    val perStreamStateMessageWithoutStats: AirbyteStateMessage =
      AirbyteStateMessage()
        .withStream(
          AirbyteStreamState()
            .withStreamState(streamState)
            .withStreamDescriptor(streamDescriptor),
        )

    assertEquals(
      globalStateMessageWithoutStats.getStateHashCode(hashFunction),
      globalStateMessageWithStats.getStateHashCode(hashFunction),
    )
    assertEquals(
      legacyStateMessageWithoutStats.getStateHashCode(hashFunction),
      legacyStateMessageWithStats.getStateHashCode(hashFunction),
    )
    assertEquals(
      perStreamStateMessageWithoutStats.getStateHashCode(hashFunction),
      perStreamStateMessageWithStats.getStateHashCode(hashFunction),
    )
  }

  @Test
  fun `test file reference transfer stats`() {
    val streamStatsTracker =
      StreamStatsTracker(
        mockk(),
        mockk(),
        false,
      )

    val name = "name"
    val namespace = "namespace"
    val fileReferenceSize = 12L
    val oldFileSize = 11L
    val record =
      AirbyteRecordMessage()
        .withStream(name)
        .withNamespace(namespace)
        .withData(Jsons.jsonNode(mapOf("id" to 1)))
        .withFileReference(
          AirbyteRecordMessageFileReference()
            .withFileSizeBytes(fileReferenceSize),
        )
        // This is the old file transfer format, making sure prioritize the new format.
        // This should be deleted when we get rid of the old format
        .withAdditionalProperty("file", FileTransferInformations("", "", "", "", oldFileSize))

    streamStatsTracker.trackRecord(record)

    assertEquals(fileReferenceSize, streamStatsTracker.streamStats.emittedBytesCount.get())
  }

  @Test
  fun `test file transfer stats`() {
    val streamStatsTracker =
      StreamStatsTracker(
        mockk(),
        mockk(),
        false,
      )

    val name = "name"
    val namespace = "namespace"
    val size = 11L
    val record =
      AirbyteRecordMessage()
        .withStream(name)
        .withNamespace(namespace)
        .withData(Jsons.jsonNode(mapOf("id" to 1)))
        .withAdditionalProperty("file", FileTransferInformations("", "", "", "", size))

    streamStatsTracker.trackRecord(record)

    assertEquals(size, streamStatsTracker.streamStats.emittedBytesCount.get())
  }

  @Test
  fun `test not file transfer stats`() {
    val streamStatsTracker =
      StreamStatsTracker(
        mockk(),
        mockk(),
        false,
      )

    val name = "name"
    val namespace = "namespace"
    val size = 11L
    val record =
      AirbyteRecordMessage()
        .withStream(name)
        .withNamespace(namespace)
        .withData(Jsons.jsonNode(mapOf("id" to 1)))

    streamStatsTracker.trackRecord(record)

    assertNotEquals(size, streamStatsTracker.streamStats.emittedBytesCount.get())
  }

  // Test written by Claude Code
  @Test
  fun testMergingAdditionalStats() {
    val streamStatsTracker =
      StreamStatsTracker(
        mockk(),
        mockk(relaxed = true),
        false,
      )

    // Test that additional stats are properly accumulated
    val additionalStats1 = mapOf("stat1" to 100.toBigDecimal())
    val additionalStats2 = mapOf("stat1" to 50.toBigDecimal(), "stat2" to 200.toBigDecimal())

    streamStatsTracker.streamStats.mergeAdditionalStats(additionalStats1)
    streamStatsTracker.streamStats.mergeAdditionalStats(additionalStats2)

    // Verify that stat1 was accumulated (100 + 50 = 150) and stat2 was added
    assertEquals(150.toBigDecimal(), streamStatsTracker.streamStats.additionalStats["stat1"])
    assertEquals(200.toBigDecimal(), streamStatsTracker.streamStats.additionalStats["stat2"])
  }
}
