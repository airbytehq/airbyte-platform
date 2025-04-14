/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helper

import io.airbyte.commons.json.Jsons
import io.airbyte.config.AirbyteStream
import io.airbyte.config.ConfiguredAirbyteCatalog
import io.airbyte.config.ConfiguredAirbyteStream
import io.airbyte.config.DestinationSyncMode
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.State
import io.airbyte.config.StateType
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.StreamSyncStats
import io.airbyte.config.SyncMode
import io.airbyte.config.SyncStats
import io.airbyte.config.helpers.ProtocolConverters.Companion.toProtocol
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStreamState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class ResumableFullRefreshStatsHelperTest {
  @ParameterizedTest
  @ValueSource(strings = ["STREAM", "GLOBAL"])
  fun `test we mark streams as resumed if they had a state in the input`(stateType: String) {
    val input =
      replicationInputWithStates(
        StateType.valueOf(stateType),
        Stream(namespace = null, name = "s0"),
        Stream(namespace = "ns", name = "s1"),
      )
    val output =
      syncOutputWithStats(
        streamSyncStats(namespace = null, name = "s0"),
        streamSyncStats(namespace = null, name = "s1"),
        streamSyncStats(namespace = "ns", name = "s0"),
        streamSyncStats(namespace = "ns", name = "s1"),
      )

    val expected =
      syncOutputWithStats(
        streamSyncStats(namespace = null, name = "s0", resumed = true),
        streamSyncStats(namespace = null, name = "s1"),
        streamSyncStats(namespace = "ns", name = "s0"),
        streamSyncStats(namespace = "ns", name = "s1", resumed = true),
      )

    ResumableFullRefreshStatsHelper().markResumedStreams(input, output)
    assertEquals(expected, output)
  }

  @ParameterizedTest
  @ValueSource(strings = ["STREAM", "GLOBAL"])
  fun `test get streams with states`(stateType: String) {
    val input =
      replicationInputWithStates(
        StateType.valueOf(stateType),
        Stream(namespace = null, name = "s0"),
        Stream(namespace = "ns", name = "s1"),
      )

    val expected =
      setOf(
        io.airbyte.config
          .StreamDescriptor()
          .withName("s0"),
        io.airbyte.config
          .StreamDescriptor()
          .withName("s1")
          .withNamespace("ns"),
      )

    val streamsWithStates = ResumableFullRefreshStatsHelper().getStreamsWithStates(input.state)
    assertEquals(expected, streamsWithStates)
  }

  @ParameterizedTest
  @ValueSource(strings = ["STREAM", "GLOBAL"])
  fun `test we correctly return only full refresh streams with states`(stateType: String) {
    val input =
      replicationInputWithStates(
        StateType.valueOf(stateType),
        Stream(namespace = null, name = "s0"),
        Stream(namespace = "ns", name = "s1"),
      )

    val catalog =
      ConfiguredAirbyteCatalog().withStreams(
        listOf(
          ConfiguredAirbyteStream(
            AirbyteStream(name = "s0", jsonSchema = Jsons.emptyObject(), supportedSyncModes = listOf(SyncMode.FULL_REFRESH)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
          ConfiguredAirbyteStream(
            AirbyteStream(name = "s0", jsonSchema = Jsons.emptyObject(), supportedSyncModes = listOf(SyncMode.INCREMENTAL)),
            SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND,
          ),
        ),
      )

    assertEquals(
      setOf(
        io.airbyte.config
          .StreamDescriptor()
          .withName("s0"),
      ),
      ResumableFullRefreshStatsHelper().getResumedFullRefreshStreams(catalog, input.state),
    )
  }

  @Test
  fun `test empty state is handled correctly when getting full refresh streams`() {
    val input = ReplicationInput()

    val catalog =
      ConfiguredAirbyteCatalog().withStreams(
        listOf(
          ConfiguredAirbyteStream(
            AirbyteStream(name = "s0", jsonSchema = Jsons.emptyObject(), supportedSyncModes = listOf(SyncMode.FULL_REFRESH)),
            SyncMode.FULL_REFRESH,
            DestinationSyncMode.APPEND,
          ),
          ConfiguredAirbyteStream(
            AirbyteStream(name = "s1", namespace = "ns", jsonSchema = Jsons.emptyObject(), supportedSyncModes = listOf(SyncMode.FULL_REFRESH)),
            SyncMode.INCREMENTAL,
            DestinationSyncMode.APPEND,
          ),
        ),
      )

    assertEquals(emptySet<StreamDescriptor>(), ResumableFullRefreshStatsHelper().getResumedFullRefreshStreams(catalog, input.state))
  }

  @Test
  fun `test we do not fail if there are no states`() {
    val input = ReplicationInput()
    val output =
      syncOutputWithStats(
        streamSyncStats(namespace = null, name = "s0"),
        streamSyncStats(namespace = null, name = "s1"),
        streamSyncStats(namespace = "ns", name = "s0"),
        streamSyncStats(namespace = "ns", name = "s1"),
      )

    // We create a new object because markResumedStreams mutates output
    val expected =
      syncOutputWithStats(
        streamSyncStats(namespace = null, name = "s0"),
        streamSyncStats(namespace = null, name = "s1"),
        streamSyncStats(namespace = "ns", name = "s0"),
        streamSyncStats(namespace = "ns", name = "s1"),
      )

    ResumableFullRefreshStatsHelper().markResumedStreams(input, output)
    assertEquals(expected, output)
  }

  private fun streamSyncStats(
    namespace: String?,
    name: String,
    resumed: Boolean = false,
  ): StreamSyncStats =
    StreamSyncStats()
      .withStreamNamespace(namespace)
      .withStreamName(name)
      .withStats(SyncStats())
      .withWasResumed(resumed)

  private fun syncOutputWithStats(vararg streamStats: StreamSyncStats): StandardSyncOutput =
    StandardSyncOutput()
      .withStandardSyncSummary(
        StandardSyncSummary()
          .withStreamStats(streamStats.toList()),
      )

  private fun replicationInputWithStates(
    stateType: StateType,
    vararg streams: Stream,
  ): ReplicationInput {
    val state: State =
      when (stateType) {
        StateType.STREAM ->
          State().withState(
            Jsons.jsonNode(
              streams
                .map { s ->
                  AirbyteStateMessage()
                    .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
                    .withStream(
                      AirbyteStreamState()
                        .withStreamState(Jsons.jsonNode("some state"))
                        .withStreamDescriptor(s.streamDescriptor.toProtocol()),
                    )
                }.toList(),
            ),
          )
        StateType.GLOBAL ->
          State().withState(
            Jsons.jsonNode(
              listOf(
                AirbyteStateMessage()
                  .withType(AirbyteStateMessage.AirbyteStateType.GLOBAL)
                  .withGlobal(
                    AirbyteGlobalState()
                      .withSharedState(Jsons.jsonNode("shared state"))
                      .withStreamStates(
                        streams
                          .map { s ->
                            AirbyteStreamState()
                              .withStreamState(Jsons.jsonNode("some state"))
                              .withStreamDescriptor(s.streamDescriptor.toProtocol())
                          }.toList(),
                      ),
                  ),
              ),
            ),
          )
        else -> throw NotImplementedError("Unsupported state type: $stateType")
      }
    return ReplicationInput().withState(state)
  }

  class Stream(
    val name: String,
    val namespace: String?,
  ) {
    val streamDescriptor: StreamDescriptor
      get() = StreamDescriptor().withName(name).withNamespace(namespace)
  }
}
