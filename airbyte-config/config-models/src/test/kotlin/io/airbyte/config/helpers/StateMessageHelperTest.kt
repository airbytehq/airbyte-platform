/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import io.airbyte.commons.json.Jsons
import io.airbyte.commons.json.Jsons.arrayNode
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.json.Jsons.jsonNode
import io.airbyte.config.State
import io.airbyte.config.StateType
import io.airbyte.config.StateWrapper
import io.airbyte.config.helpers.StateMessageHelper.getState
import io.airbyte.config.helpers.StateMessageHelper.getTypedState
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor
import org.assertj.core.api.Assertions
import org.assertj.core.api.ThrowableAssert
import org.junit.jupiter.api.Test
import java.util.Arrays
import java.util.List
import java.util.Map
import java.util.Optional

internal class StateMessageHelperTest {
  @Test
  fun testEmpty() {
    val stateWrapper: Optional<StateWrapper> = getTypedState(null)
    Assertions.assertThat(stateWrapper).isEmpty()
  }

  @Test
  fun testEmptyList() {
    val stateWrapper: Optional<StateWrapper> = getTypedState(arrayNode())
    Assertions.assertThat(stateWrapper).isEmpty()
  }

  @Test
  fun testLegacy() {
    val stateWrapper: Optional<StateWrapper> = getTypedState(emptyObject())
    Assertions.assertThat(stateWrapper).isNotEmpty()
    Assertions.assertThat(stateWrapper.get().getStateType()).isEqualTo(StateType.LEGACY)
  }

  @Test
  fun testLegacyInList() {
    val jsonState =
      Jsons.jsonNode(List.of(Map.of("Any", "value")))

    val stateWrapper: Optional<StateWrapper> = getTypedState(jsonState)
    Assertions.assertThat(stateWrapper).isNotEmpty()
    Assertions.assertThat(stateWrapper.get().getStateType()).isEqualTo(StateType.LEGACY)
    Assertions.assertThat(stateWrapper.get().getLegacyState()).isEqualTo(jsonState)
  }

  @Test
  fun testLegacyInNewFormat() {
    val stateMessage =
      AirbyteStateMessage()
        .withType(AirbyteStateType.LEGACY)
        .withData(emptyObject())
    val stateWrapper: Optional<StateWrapper> =
      getTypedState(jsonNode(List.of(stateMessage)))
    Assertions.assertThat(stateWrapper).isNotEmpty()
    Assertions.assertThat(stateWrapper.get().getStateType()).isEqualTo(StateType.LEGACY)
  }

  @Test
  fun testGlobal() {
    val stateMessage =
      AirbyteStateMessage()
        .withType(AirbyteStateType.GLOBAL)
        .withGlobal(
          AirbyteGlobalState()
            .withSharedState(emptyObject())
            .withStreamStates(
              List.of<AirbyteStreamState?>(
                AirbyteStreamState().withStreamDescriptor(StreamDescriptor().withName("a")).withStreamState(emptyObject()),
                AirbyteStreamState().withStreamDescriptor(StreamDescriptor().withName("b")).withStreamState(emptyObject()),
              ),
            ),
        )
    val stateWrapper: Optional<StateWrapper> =
      getTypedState(jsonNode(List.of(stateMessage)))
    Assertions.assertThat(stateWrapper).isNotEmpty()
    Assertions.assertThat(stateWrapper.get().getStateType()).isEqualTo(StateType.GLOBAL)
    Assertions.assertThat(stateWrapper.get().getGlobal()).isEqualTo(stateMessage)
  }

  @Test
  fun testStream() {
    val stateMessage1 =
      AirbyteStateMessage()
        .withType(AirbyteStateType.STREAM)
        .withStream(
          AirbyteStreamState().withStreamDescriptor(StreamDescriptor().withName("a")).withStreamState(emptyObject()),
        )
    val stateMessage2 =
      AirbyteStateMessage()
        .withType(AirbyteStateType.STREAM)
        .withStream(
          AirbyteStreamState().withStreamDescriptor(StreamDescriptor().withName("b")).withStreamState(emptyObject()),
        )
    val stateWrapper: Optional<StateWrapper> =
      getTypedState(jsonNode(List.of(stateMessage1, stateMessage2)))
    Assertions.assertThat(stateWrapper).isNotEmpty()
    Assertions.assertThat(stateWrapper.get().getStateType()).isEqualTo(StateType.STREAM)
    Assertions.assertThat(stateWrapper.get().getStateMessages()).containsExactlyInAnyOrder(stateMessage1, stateMessage2)
  }

  @Test
  fun testInvalidMixedState() {
    val stateMessage1 =
      AirbyteStateMessage()
        .withType(AirbyteStateType.STREAM)
        .withStream(
          AirbyteStreamState().withStreamDescriptor(StreamDescriptor().withName("a")).withStreamState(emptyObject()),
        )
    val stateMessage2 =
      AirbyteStateMessage()
        .withType(AirbyteStateType.GLOBAL)
        .withGlobal(
          AirbyteGlobalState()
            .withSharedState(emptyObject())
            .withStreamStates(
              List.of<AirbyteStreamState?>(
                AirbyteStreamState().withStreamDescriptor(StreamDescriptor().withName("a")).withStreamState(emptyObject()),
                AirbyteStreamState().withStreamDescriptor(StreamDescriptor().withName("b")).withStreamState(emptyObject()),
              ),
            ),
        )
    Assertions
      .assertThatThrownBy(
        ThrowableAssert.ThrowingCallable {
          getTypedState(
            jsonNode<MutableList<AirbyteStateMessage>?>(
              List.of<AirbyteStateMessage?>(
                stateMessage1,
                stateMessage2,
              ),
            ),
          )
        },
      ).isInstanceOf(IllegalStateException::class.java)
  }

  @Test
  fun testDuplicatedGlobalState() {
    val stateMessage1 =
      AirbyteStateMessage()
        .withType(AirbyteStateType.GLOBAL)
        .withGlobal(
          AirbyteGlobalState()
            .withSharedState(emptyObject())
            .withStreamStates(
              List.of<AirbyteStreamState?>(
                AirbyteStreamState().withStreamDescriptor(StreamDescriptor().withName("a")).withStreamState(emptyObject()),
                AirbyteStreamState().withStreamDescriptor(StreamDescriptor().withName("b")).withStreamState(emptyObject()),
              ),
            ),
        )
    val stateMessage2 =
      AirbyteStateMessage()
        .withType(AirbyteStateType.GLOBAL)
        .withGlobal(
          AirbyteGlobalState()
            .withSharedState(emptyObject())
            .withStreamStates(
              List.of<AirbyteStreamState?>(
                AirbyteStreamState().withStreamDescriptor(StreamDescriptor().withName("a")).withStreamState(emptyObject()),
                AirbyteStreamState().withStreamDescriptor(StreamDescriptor().withName("b")).withStreamState(emptyObject()),
              ),
            ),
        )
    Assertions
      .assertThatThrownBy(
        ThrowableAssert.ThrowingCallable {
          getTypedState(
            jsonNode<MutableList<AirbyteStateMessage>?>(
              List.of<AirbyteStateMessage?>(
                stateMessage1,
                stateMessage2,
              ),
            ),
          )
        },
      ).isInstanceOf(IllegalStateException::class.java)
  }

  @Test
  fun testLegacyStateConversion() {
    val stateWrapper =
      StateWrapper()
        .withStateType(StateType.LEGACY)
        .withLegacyState(deserialize("{\"json\": \"blob\"}"))
    val expectedState = State().withState(deserialize("{\"json\": \"blob\"}"))

    val convertedState = getState(stateWrapper)
    Assertions.assertThat<State?>(convertedState).isEqualTo(expectedState)
  }

  @Test
  fun testGlobalStateConversion() {
    val stateWrapper =
      StateWrapper()
        .withStateType(StateType.GLOBAL)
        .withGlobal(
          AirbyteStateMessage().withType(AirbyteStateType.GLOBAL).withGlobal(
            AirbyteGlobalState()
              .withSharedState(deserialize("\"shared\""))
              .withStreamStates(
                mutableListOf<AirbyteStreamState?>(
                  AirbyteStreamState()
                    .withStreamDescriptor(StreamDescriptor().withNamespace("ns").withName("name"))
                    .withStreamState(deserialize("\"stream state\"")),
                ),
              ),
          ),
        )
    val expectedState =
      State().withState(
        deserialize(
          """
          [{
            "type":"GLOBAL",
            "global":{
               "shared_state":"shared",
               "stream_states":[
                 {"stream_descriptor":{"name":"name","namespace":"ns"},"stream_state":"stream state"}
               ]
            }
          }]
          
          """.trimIndent(),
        ),
      )

    val convertedState = getState(stateWrapper)
    Assertions.assertThat<State?>(convertedState).isEqualTo(expectedState)
  }

  @Test
  fun testStreamStateConversion() {
    val stateWrapper =
      StateWrapper()
        .withStateType(StateType.STREAM)
        .withStateMessages(
          Arrays.asList<AirbyteStateMessage?>(
            AirbyteStateMessage().withType(AirbyteStateType.STREAM).withStream(
              AirbyteStreamState()
                .withStreamDescriptor(StreamDescriptor().withNamespace("ns1").withName("name1"))
                .withStreamState(deserialize("\"state1\"")),
            ),
            AirbyteStateMessage().withType(AirbyteStateType.STREAM).withStream(
              AirbyteStreamState()
                .withStreamDescriptor(StreamDescriptor().withNamespace("ns2").withName("name2"))
                .withStreamState(deserialize("\"state2\"")),
            ),
          ),
        )
    val expectedState =
      State().withState(
        deserialize(
          """
          [
            {"type":"STREAM","stream":{"stream_descriptor":{"name":"name1","namespace":"ns1"},"stream_state":"state1"}},
            {"type":"STREAM","stream":{"stream_descriptor":{"name":"name2","namespace":"ns2"},"stream_state":"state2"}}
          ]
          
          """.trimIndent(),
        ),
      )

    val convertedState = getState(stateWrapper)
    Assertions.assertThat<State?>(convertedState).isEqualTo(expectedState)
  }
}
