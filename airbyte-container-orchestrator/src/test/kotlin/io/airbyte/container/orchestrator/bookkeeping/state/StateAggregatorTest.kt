/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.container.orchestrator.bookkeeping.state

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.commons.json.Jsons
import io.airbyte.config.State
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.util.function.Consumer

internal class StateAggregatorTest {
  private lateinit var stateAggregator: StateAggregator

  @BeforeEach
  fun init() {
    stateAggregator =
      DefaultStateAggregator(
        StreamStateAggregator(),
        SingleStateAggregator(),
      )
  }

  @ParameterizedTest
  @EnumSource(AirbyteStateMessage.AirbyteStateType::class)
  fun testCantMixType(stateType: AirbyteStateMessage.AirbyteStateType) {
    val allTypes = AirbyteStateMessage.AirbyteStateType.entries.toTypedArray()

    stateAggregator.ingest(getEmptyMessage(stateType))

    val differentTypes = allTypes.filter { type: AirbyteStateMessage.AirbyteStateType -> type != stateType }.toList()
    differentTypes
      .forEach(
        Consumer { differentType: AirbyteStateMessage.AirbyteStateType ->
          assertThrows(
            IllegalArgumentException::class.java,
            { stateAggregator.ingest(getEmptyMessage(differentType)) },
          )
        },
      )
  }

  @Test
  fun testCantMixNullType() {
    val allIncompatibleTypes = listOf(AirbyteStateMessage.AirbyteStateType.GLOBAL, AirbyteStateMessage.AirbyteStateType.STREAM)

    stateAggregator.ingest(getEmptyMessage(null))

    allIncompatibleTypes
      .forEach(
        Consumer { differentType: AirbyteStateMessage.AirbyteStateType ->
          assertThrows(
            IllegalArgumentException::class.java,
            { stateAggregator.ingest(getEmptyMessage(differentType)) },
          )
        },
      )

    stateAggregator.ingest(getEmptyMessage(AirbyteStateMessage.AirbyteStateType.LEGACY))
  }

  @Test
  fun testNullState() {
    val state1 = getNullMessage(1)
    val state2 = getNullMessage(2)

    stateAggregator.ingest(state1)
    assertEquals(State().withState(state1.getData()), stateAggregator.getAggregated())

    stateAggregator.ingest(state2)
    assertEquals(State().withState(state2.getData()), stateAggregator.getAggregated())
  }

  @Test
  fun testLegacyState() {
    val state1 = getLegacyMessage(1)
    val state2 = getLegacyMessage(2)

    stateAggregator.ingest(state1)
    assertEquals(State().withState(state1.getData()), stateAggregator.getAggregated())

    stateAggregator.ingest(state2)
    assertEquals(State().withState(state2.getData()), stateAggregator.getAggregated())
  }

  @Test
  fun testGlobalState() {
    val state1 = getGlobalMessage(1)
    val state2 = getGlobalMessage(2)

    val state1NoData = getGlobalMessage(1).withData(null)
    val state2NoData = getGlobalMessage(2).withData(null)

    stateAggregator.ingest(Jsons.`object`(Jsons.jsonNode(state1), AirbyteStateMessage::class.java))
    assertEquals(
      State()
        .withState(Jsons.jsonNode(listOf(state1NoData))),
      stateAggregator.getAggregated(),
    )

    stateAggregator.ingest(Jsons.`object`(Jsons.jsonNode(state2), AirbyteStateMessage::class.java))
    assertEquals(
      State()
        .withState(Jsons.jsonNode(listOf(state2NoData))),
      stateAggregator.getAggregated(),
    )
  }

  @Test
  fun testStreamState() {
    val state1 = getStreamMessage("a", 1)
    val state2 = getStreamMessage("b", 2)
    val state3 = getStreamMessage("b", 3)

    val state1NoData = getStreamMessage("a", 1).withData(null)
    val state2NoData = getStreamMessage("b", 2).withData(null)
    val state3NoData = getStreamMessage("b", 3).withData(null)

    stateAggregator =
      DefaultStateAggregator(
        StreamStateAggregator(),
        SingleStateAggregator(),
      )

    stateAggregator.ingest(Jsons.`object`(Jsons.jsonNode(state1), AirbyteStateMessage::class.java))
    assertEquals(
      State()
        .withState(Jsons.jsonNode(listOf(state1NoData))),
      stateAggregator.getAggregated(),
    )

    stateAggregator.ingest(Jsons.`object`(Jsons.jsonNode(state2), AirbyteStateMessage::class.java))
    assertEquals(
      State()
        .withState(Jsons.jsonNode(listOf(state1NoData, state2NoData))),
      stateAggregator.getAggregated(),
    )

    stateAggregator.ingest(Jsons.`object`(Jsons.jsonNode(state3), AirbyteStateMessage::class.java))
    assertEquals(
      State()
        .withState(Jsons.jsonNode(listOf(state1NoData, state3NoData))),
      stateAggregator.getAggregated(),
    )
  }

  @Test
  fun testIngestFromAnotherStateAggregatorSingleState() {
    val stateG1 = getGlobalMessage(1)
    stateAggregator.ingest(stateG1)

    val stateG2 = getGlobalMessage(2)
    val otherStateAggregator: StateAggregator =
      DefaultStateAggregator(
        StreamStateAggregator(),
        SingleStateAggregator(),
      )
    otherStateAggregator.ingest(stateG2)

    stateAggregator.ingest(otherStateAggregator)
    assertEquals(listOf(stateG2), getStateMessages(stateAggregator.getAggregated()))
  }

  @Test
  fun testIngestFromAnotherStateAggregatorStreamStates() {
    stateAggregator =
      DefaultStateAggregator(
        StreamStateAggregator(),
        SingleStateAggregator(),
      )
    val stateA1 = getStreamMessage("a", 1)
    val stateB2 = getStreamMessage("b", 2)
    stateAggregator.ingest(stateA1)
    stateAggregator.ingest(stateB2)

    val stateA2 = getStreamMessage("a", 3)
    val stateC1 = getStreamMessage("c", 1)
    val otherStateAggregator: StateAggregator =
      DefaultStateAggregator(
        StreamStateAggregator(),
        SingleStateAggregator(),
      )
    otherStateAggregator.ingest(stateA2)
    otherStateAggregator.ingest(stateC1)

    stateAggregator.ingest(otherStateAggregator)
    assertEquals(listOf(stateA2, stateB2, stateC1), getStateMessages(stateAggregator.getAggregated()))
  }

  @Test
  fun testIngestFromAnotherStateAggregatorChecksStateType() {
    stateAggregator =
      DefaultStateAggregator(
        StreamStateAggregator(),
        SingleStateAggregator(),
      )
    val stateG1 = getGlobalMessage(1)
    stateAggregator.ingest(stateG1)

    val stateA2 = getStreamMessage("a", 3)
    val otherStateAggregator: StateAggregator =
      DefaultStateAggregator(
        StreamStateAggregator(),
        SingleStateAggregator(),
      )
    otherStateAggregator.ingest(stateA2)

    assertThrows(
      IllegalArgumentException::class.java,
    ) { stateAggregator.ingest(otherStateAggregator) }
    assertThrows(
      IllegalArgumentException::class.java,
    ) { otherStateAggregator.ingest(stateAggregator) }
  }

  @Test
  fun testIsEmptyForSingleStates() {
    stateAggregator =
      DefaultStateAggregator(
        StreamStateAggregator(),
        SingleStateAggregator(),
      )
    assertTrue(stateAggregator.isEmpty())

    val globalState = getGlobalMessage(1)
    stateAggregator.ingest(globalState)
    assertFalse(stateAggregator.isEmpty())
  }

  @Test
  fun testIsEmptyForStreamStates() {
    stateAggregator =
      DefaultStateAggregator(
        StreamStateAggregator(),
        SingleStateAggregator(),
      )
    assertTrue(stateAggregator.isEmpty())

    val streamState = getStreamMessage("woot", 1)
    stateAggregator.ingest(streamState)
    assertFalse(stateAggregator.isEmpty())
  }

  @Test
  fun testIsEmptyWhenIngestFromAggregatorSingle() {
    stateAggregator =
      DefaultStateAggregator(
        StreamStateAggregator(),
        SingleStateAggregator(),
      )
    assertTrue(stateAggregator.isEmpty())

    val globalState = getGlobalMessage(1)
    val otherAggregator: StateAggregator =
      DefaultStateAggregator(
        StreamStateAggregator(),
        SingleStateAggregator(),
      )
    otherAggregator.ingest(globalState)

    stateAggregator.ingest(otherAggregator)
    assertFalse(stateAggregator.isEmpty())
  }

  @Test
  fun testIsEmptyWhenIngestFromAggregatorStream() {
    stateAggregator =
      DefaultStateAggregator(
        StreamStateAggregator(),
        SingleStateAggregator(),
      )
    assertTrue(stateAggregator.isEmpty())

    val streamState = getStreamMessage("woot", 1)
    val otherAggregator: StateAggregator =
      DefaultStateAggregator(
        StreamStateAggregator(),
        SingleStateAggregator(),
      )
    otherAggregator.ingest(streamState)

    stateAggregator.ingest(otherAggregator)
    assertFalse(stateAggregator.isEmpty())
  }

  private fun getNullMessage(stateValue: Int): AirbyteStateMessage = AirbyteStateMessage().withData(Jsons.jsonNode(stateValue))

  private fun getLegacyMessage(stateValue: Int): AirbyteStateMessage =
    AirbyteStateMessage().withType(AirbyteStateMessage.AirbyteStateType.LEGACY).withData(Jsons.jsonNode(stateValue))

  private fun getGlobalMessage(stateValue: Int): AirbyteStateMessage =
    AirbyteStateMessage()
      .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
      .withGlobal(
        AirbyteGlobalState()
          .withStreamStates(
            listOf(
              AirbyteStreamState()
                .withStreamDescriptor(
                  StreamDescriptor()
                    .withName("test"),
                ).withStreamState(Jsons.jsonNode(stateValue)),
            ),
          ),
      ).withData(Jsons.jsonNode("HelloWorld"))

  private fun getStreamMessage(
    streamName: String?,
    stateValue: Int,
  ): AirbyteStateMessage =
    AirbyteStateMessage()
      .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
      .withStream(
        AirbyteStreamState()
          .withStreamDescriptor(
            StreamDescriptor()
              .withName(streamName),
          ).withStreamState(Jsons.jsonNode(stateValue)),
      ).withData(Jsons.jsonNode("Hello"))

  private fun getEmptyMessage(stateType: AirbyteStateMessage.AirbyteStateType?): AirbyteStateMessage {
    if (stateType == AirbyteStateMessage.AirbyteStateType.STREAM) {
      return AirbyteStateMessage()
        .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
        .withStream(
          AirbyteStreamState()
            .withStreamDescriptor(StreamDescriptor()),
        )
    }

    return AirbyteStateMessage().withType(stateType)
  }

  private fun getStateMessages(state: State) =
    state.state
      .elements()
      .asSequence()
      .map { s: JsonNode -> Jsons.`object`(s, AirbyteStateMessage::class.java) }
      .toList()
}
