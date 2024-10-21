/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.internal.bookkeeping

import com.google.common.hash.Hashing
import io.airbyte.commons.json.Jsons
import io.airbyte.config.FileTransferInformations
import io.airbyte.protocol.models.AirbyteGlobalState
import io.airbyte.protocol.models.AirbyteRecordMessage
import io.airbyte.protocol.models.AirbyteStateMessage
import io.airbyte.protocol.models.AirbyteStateStats
import io.airbyte.protocol.models.AirbyteStreamState
import io.airbyte.protocol.models.StreamDescriptor
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test

class StatsTrackerTest {
  @Test
  internal fun `test that state hash code generation ignores state stats fields`() {
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
        )
        .withSourceStats(sourceStats)
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
  internal fun `test file transfer stats`() {
    val streamStatsTracker =
      StreamStatsTracker(
        mockk(),
        mockk(),
        true,
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
  internal fun `test not file transfer stats`() {
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

    assertNotEquals(size, streamStatsTracker.streamStats.emittedBytesCount.get())
  }
}
