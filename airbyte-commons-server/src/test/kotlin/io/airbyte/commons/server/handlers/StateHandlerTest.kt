/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionState
import io.airbyte.api.model.generated.ConnectionStateCreateOrUpdate
import io.airbyte.api.model.generated.ConnectionStateType
import io.airbyte.api.model.generated.GlobalState
import io.airbyte.api.model.generated.JobRead
import io.airbyte.api.model.generated.StreamState
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons.deserialize
import io.airbyte.commons.server.errors.SyncIsRunningException
import io.airbyte.config.StateType
import io.airbyte.config.StateWrapper
import io.airbyte.config.persistence.StatePersistence
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage.AirbyteStateType
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.StreamDescriptor
import jakarta.validation.Valid
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.mockito.Mockito.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.io.IOException
import java.util.List
import java.util.Optional
import java.util.UUID

internal class StateHandlerTest {
  private lateinit var stateHandler: StateHandler
  private lateinit var statePersistence: StatePersistence
  private lateinit var jobHistoryHandler: JobHistoryHandler

  @BeforeEach
  fun setup() {
    statePersistence = mock()
    jobHistoryHandler = mock()
    stateHandler = StateHandler(statePersistence, jobHistoryHandler)
  }

  @Test
  @Throws(IOException::class)
  fun testGetCurrentStateEmpty() {
    whenever<Optional<StateWrapper>?>(statePersistence.getCurrentState(CONNECTION_ID)).thenReturn(Optional.empty<StateWrapper>())

    val expected = ConnectionState().connectionId(CONNECTION_ID).stateType(ConnectionStateType.NOT_SET).streamState(null)
    val actual = stateHandler.getState(ConnectionIdRequestBody().connectionId(CONNECTION_ID))
    Assertions.assertEquals(expected, actual)
  }

  @Test
  @Throws(IOException::class)
  fun testGetLegacyState() {
    whenever<Optional<StateWrapper>?>(statePersistence.getCurrentState(CONNECTION_ID)).thenReturn(
      Optional.of(
        StateWrapper()
          .withStateType(StateType.LEGACY)
          .withLegacyState(JSON_BLOB),
      ),
    )

    val expected =
      ConnectionState()
        .connectionId(CONNECTION_ID)
        .stateType(ConnectionStateType.LEGACY)
        .streamState(null)
        .state(JSON_BLOB)
    val actual = stateHandler.getState(ConnectionIdRequestBody().connectionId(CONNECTION_ID))
    Assertions.assertEquals(expected, actual)
  }

  @Test
  @Throws(IOException::class)
  fun testGetGlobalState() {
    whenever<Optional<StateWrapper>?>(statePersistence.getCurrentState(CONNECTION_ID)).thenReturn(
      Optional.of<StateWrapper>(
        StateWrapper()
          .withStateType(StateType.GLOBAL)
          .withGlobal(
            AirbyteStateMessage().withType(AirbyteStateType.GLOBAL).withGlobal(
              AirbyteGlobalState()
                .withSharedState(JSON_BLOB)
                .withStreamStates(
                  List.of<AirbyteStreamState?>(
                    AirbyteStreamState().withStreamDescriptor(STREAM_DESCRIPTOR1).withStreamState(JSON_BLOB),
                    AirbyteStreamState().withStreamDescriptor(STREAM_DESCRIPTOR2).withStreamState(JSON_BLOB),
                  ),
                ),
            ),
          ),
      ),
    )

    val expected =
      ConnectionState()
        .connectionId(CONNECTION_ID)
        .stateType(ConnectionStateType.GLOBAL)
        .streamState(null)
        .globalState(
          GlobalState().sharedState(JSON_BLOB).streamStates(
            List.of<@Valid StreamState?>(
              StreamState().streamDescriptor(toApi(STREAM_DESCRIPTOR1)).streamState(JSON_BLOB),
              StreamState().streamDescriptor(toApi(STREAM_DESCRIPTOR2)).streamState(JSON_BLOB),
            ),
          ),
        )
    val actual = stateHandler.getState(ConnectionIdRequestBody().connectionId(CONNECTION_ID))
    Assertions.assertEquals(expected, actual)
  }

  @Test
  @Throws(IOException::class)
  fun testGetStreamState() {
    whenever<Optional<StateWrapper>?>(statePersistence.getCurrentState(CONNECTION_ID)).thenReturn(
      Optional.of<StateWrapper>(
        StateWrapper()
          .withStateType(StateType.STREAM)
          .withStateMessages(
            List.of<AirbyteStateMessage?>(
              AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(AirbyteStreamState().withStreamDescriptor(STREAM_DESCRIPTOR1).withStreamState(JSON_BLOB)),
              AirbyteStateMessage()
                .withType(AirbyteStateType.STREAM)
                .withStream(AirbyteStreamState().withStreamDescriptor(STREAM_DESCRIPTOR2).withStreamState(JSON_BLOB)),
            ),
          ),
      ),
    )

    val expected =
      ConnectionState()
        .connectionId(CONNECTION_ID)
        .stateType(ConnectionStateType.STREAM)
        .streamState(
          List.of<@Valid StreamState?>(
            StreamState().streamDescriptor(toApi(STREAM_DESCRIPTOR1)).streamState(JSON_BLOB),
            StreamState().streamDescriptor(toApi(STREAM_DESCRIPTOR2)).streamState(JSON_BLOB),
          ),
        )
    val actual = stateHandler.getState(ConnectionIdRequestBody().connectionId(CONNECTION_ID))
    Assertions.assertEquals(expected, actual)
  }

  // the api type has an extra type, so the verifying the compatibility of the type conversion is more
  // involved
  @Test
  fun testEnumConversion() {
    Assertions.assertEquals(3, AirbyteStateType::class.java.getEnumConstants().size)
    Assertions.assertEquals(4, ConnectionStateType::class.java.getEnumConstants().size)

    // to AirbyteStateType => ConnectionStateType
    Assertions.assertEquals(
      ConnectionStateType.GLOBAL,
      AirbyteStateType.GLOBAL.convertTo<ConnectionStateType>(),
    )
    Assertions.assertEquals(
      ConnectionStateType.STREAM,
      AirbyteStateType.STREAM.convertTo<ConnectionStateType>(),
    )
    Assertions.assertEquals(
      ConnectionStateType.LEGACY,
      AirbyteStateType.LEGACY.convertTo<ConnectionStateType>(),
    )

    // to ConnectionStateType => AirbyteStateType
    Assertions.assertEquals(
      AirbyteStateType.GLOBAL,
      ConnectionStateType.GLOBAL.convertTo<AirbyteStateType>(),
    )
    Assertions.assertEquals(
      AirbyteStateType.STREAM,
      ConnectionStateType.STREAM.convertTo<AirbyteStateType>(),
    )
    Assertions.assertEquals(
      AirbyteStateType.LEGACY,
      ConnectionStateType.LEGACY.convertTo<AirbyteStateType>(),
    )
  }

  @Test
  @Throws(IOException::class)
  fun testCreateOrUpdateState() {
    val input =
      ConnectionStateCreateOrUpdate()
        .connectionId(CONNECTION_ID)
        .connectionState(ConnectionState().stateType(ConnectionStateType.LEGACY).state(JSON_BLOB))
    stateHandler.createOrUpdateState(input)
    verify(statePersistence, times(1)).updateOrCreateState(
      CONNECTION_ID,
      StateWrapper().withStateType(StateType.LEGACY).withLegacyState(JSON_BLOB).withStateMessages(null),
    )
  }

  @Test
  @Throws(IOException::class)
  fun testCreateOrUpdateStateSafe() {
    val input =
      ConnectionStateCreateOrUpdate()
        .connectionId(CONNECTION_ID)
        .connectionState(ConnectionState().stateType(ConnectionStateType.LEGACY).state(JSON_BLOB))
    whenever<Optional<JobRead>?>(jobHistoryHandler.getLatestRunningSyncJob(CONNECTION_ID)).thenReturn(Optional.empty<JobRead>())
    stateHandler.createOrUpdateStateSafe(input)
    verify(statePersistence, times(1)).updateOrCreateState(
      CONNECTION_ID,
      StateWrapper().withStateType(StateType.LEGACY).withLegacyState(JSON_BLOB).withStateMessages(null),
    )
  }

  @Test
  @Throws(IOException::class)
  fun testCreateOrUpdateStateSafeThrowsWhenSyncRunning() {
    val input =
      ConnectionStateCreateOrUpdate()
        .connectionId(CONNECTION_ID)
        .connectionState(ConnectionState().stateType(ConnectionStateType.LEGACY).state(JSON_BLOB))
    whenever<Optional<JobRead>?>(jobHistoryHandler.getLatestRunningSyncJob(CONNECTION_ID)).thenReturn(Optional.of<JobRead>(JobRead()))
    Assertions.assertThrows(
      SyncIsRunningException::class.java,
      Executable { stateHandler.createOrUpdateStateSafe(input) },
    )
  }

  companion object {
    val CONNECTION_ID: UUID = UUID.randomUUID()
    private val JSON_BLOB = deserialize("{\"users\": 10}")
    val STREAM_DESCRIPTOR1: StreamDescriptor = StreamDescriptor().withName("coffee")
    val STREAM_DESCRIPTOR2: StreamDescriptor = StreamDescriptor().withName("tea")

    private fun toApi(protocolStreamDescriptor: StreamDescriptor): io.airbyte.api.model.generated.StreamDescriptor? =
      io.airbyte.api.model.generated
        .StreamDescriptor()
        .name(protocolStreamDescriptor.getName())
        .namespace(protocolStreamDescriptor.getNamespace())
  }
}
