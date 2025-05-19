/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.worker.model

import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateStats
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.Random
import java.util.UUID

internal class StateWithIdTest {
  @ParameterizedTest
  @ValueSource(
    strings = [
      (
        "{\"{\\\"schema\\\":null,\\\"payload\\\":[\\\"db_jagkjrgxhw\\\",{\\\"server\\\":\\\"db_jagkjrgxhw\\\"}]}\":" +
          "\"{\\\"last_snapshot_record\\\":true,\\\"lsn\\\":23896935,\\\"txId\\\":505,\\\"ts_usec\\\":1659422332985000,\\\"snapshot\\\":true}\"}"
      ), (
        "{\"[\\\"db_jagkjrgxhw\\\",{\\\"server\\\":\\\"db_jagkjrgxhw\\\"}]}\":\"{\\\"last_snapshot_record\\\":true," +
          "\\\"lsn\\\":23896935,\\\"txId\\\":505,\\\"ts_usec\\\":1659422332985000,\\\"snapshot\\\":true}\"}"
      ), (
        "{\"[\\\"db_jagkjrgxhw\\\",{\\\"server\\\":\\\"db_jagkjrgxhw\\\"}]\":\"{\\\"transaction_id\\\":null,\\\"lsn\\\":" +
          "23896935,\\\"txId\\\":505,\\\"ts_usec\\\":1677520006097984}\"}"
      ),
    ],
  )
  fun globalStateTest(cdcState: String?) {
    val random = Random()
    val recordCount = random.nextDouble()
    val cursorName = UUID.randomUUID().toString()

    val originalState = getAirbyteGlobalStateMessage(cdcState, recordCount, cursorName)
    val originalMessage =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(originalState)

    val copyOfOriginalState = getAirbyteGlobalStateMessage(cdcState, recordCount, cursorName)
    val copyOfOriginal =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(copyOfOriginalState)

    assertEquals(originalMessage, copyOfOriginal)
    val stateMessageWithIdAdded: AirbyteMessage = attachIdToStateMessageFromSource(copyOfOriginal)
    assertNotEquals(originalMessage, stateMessageWithIdAdded)
    assertEquals(originalMessage.state.global, stateMessageWithIdAdded.state.global)

    val serializedMessage = Jsons.serialize(stateMessageWithIdAdded)
    val deserializedMessage = Jsons.tryDeserializeExact(serializedMessage, AirbyteMessage::class.java)
    assertEquals(stateMessageWithIdAdded, deserializedMessage.orElseThrow())
    assertEquals(originalMessage.state.global, deserializedMessage.orElseThrow()!!.state.global)
    assertEquals(
      getIdFromStateMessage(stateMessageWithIdAdded),
      getIdFromStateMessage(deserializedMessage.orElseThrow()),
    )
  }

  @Test
  fun streamStateTest() {
    val random = Random()
    val recordCount = random.nextDouble()
    val cursorName = UUID.randomUUID().toString()

    val originalState = getAirbyteStreamStateMessage(recordCount, cursorName)
    val originalMessage =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(originalState)

    val copyOfOriginalState = getAirbyteStreamStateMessage(recordCount, cursorName)
    val copyOfOriginal =
      AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(copyOfOriginalState)

    assertEquals(originalMessage, copyOfOriginal)
    val stateMessageWithIdAdded: AirbyteMessage = attachIdToStateMessageFromSource(copyOfOriginal)
    assertNotEquals(originalMessage, stateMessageWithIdAdded)
    assertEquals(originalMessage.state.global, stateMessageWithIdAdded.state.global)

    val serializedMessage = Jsons.serialize(stateMessageWithIdAdded)
    val deserializedMessage = Jsons.tryDeserializeExact(serializedMessage, AirbyteMessage::class.java)
    assertEquals(stateMessageWithIdAdded, deserializedMessage.orElseThrow())
    assertEquals(originalMessage.state.stream, deserializedMessage.orElseThrow()!!.state.stream)
    assertEquals(
      getIdFromStateMessage(stateMessageWithIdAdded),
      getIdFromStateMessage(deserializedMessage.orElseThrow()),
    )
  }

  @Test
  fun testAttachStateId() {
    val stateMessage = AirbyteStateMessage()
    val updatedStateMessage = attachIdToStateMessageFromSource(stateMessage)
    assertTrue(updatedStateMessage.additionalProperties.contains(ID))
    val updatedStateMessage2 = attachIdToStateMessageFromSource(updatedStateMessage)
    assertEquals(updatedStateMessage.additionalProperties[ID], updatedStateMessage2.additionalProperties[ID])
  }

  @Test
  fun testMissingStateId() {
    val message = AirbyteStateMessage()
    assertThrows(IllegalStateException::class.java) {
      getIdFromStateMessage(message)
    }
  }

  private fun getAirbyteStreamStateMessage(
    recordCount: Double,
    cursorName: String,
  ): AirbyteStateMessage? {
    val streamState =
      AirbyteStreamState()
        .withStreamState(Jsons.jsonNode(mapOf(cursorName to 1)))
        .withStreamDescriptor(StreamDescriptor().withName(cursorName).withNamespace(cursorName))
    val airbyteStateStats = AirbyteStateStats().withRecordCount(recordCount)
    return AirbyteStateMessage()
      .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
      .withStream(streamState)
      .withSourceStats(airbyteStateStats)
  }

  private fun getAirbyteGlobalStateMessage(
    cdcState: String?,
    recordCount: Double,
    cursorName: String,
  ): AirbyteStateMessage? {
    val cdcStateAsJson = Jsons.deserialize(cdcState)
    val globalState =
      AirbyteGlobalState().withSharedState(cdcStateAsJson).withStreamStates(
        listOf<AirbyteStreamState?>(
          AirbyteStreamState()
            .withStreamState(Jsons.jsonNode(mapOf(cursorName to 1)))
            .withStreamDescriptor(StreamDescriptor().withName(cursorName).withNamespace(cursorName)),
        ),
      )
    val airbyteStateStats = AirbyteStateStats().withRecordCount(recordCount)
    return AirbyteStateMessage()
      .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
      .withGlobal(globalState)
      .withSourceStats(airbyteStateStats)
  }
}
