/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.WorkspaceApi
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.airbyte.commons.temporal.scheduling.retries.RetryManager
import io.airbyte.workers.helpers.RetryStateClient
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.HydrateInput
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.PersistInput
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mock
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.whenever
import java.io.IOException
import java.util.UUID
import java.util.stream.Stream

internal class RetryStatePersistenceActivityTest {
  @Mock
  private lateinit var mAirbyteApiClient: AirbyteApiClient

  @Mock
  private lateinit var mRetryStateClient: RetryStateClient

  @Mock
  private lateinit var mWorkspaceApi: WorkspaceApi

  @BeforeEach
  @Throws(Exception::class)
  fun setup() {
    mAirbyteApiClient = org.mockito.Mockito.mock<AirbyteApiClient>(AirbyteApiClient::class.java)
    mRetryStateClient = org.mockito.Mockito.mock<RetryStateClient>(RetryStateClient::class.java)
    mWorkspaceApi = org.mockito.Mockito.mock<WorkspaceApi>(WorkspaceApi::class.java)
    whenever(mWorkspaceApi.getWorkspaceByConnectionId(any<ConnectionIdRequestBody>())).thenReturn(
      WorkspaceRead(
        UUID.randomUUID(),
        UUID.randomUUID(),
        "name",
        "slug",
        false,
        UUID.randomUUID(),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
      ),
    )
    whenever(mAirbyteApiClient.workspaceApi).thenReturn(mWorkspaceApi)
  }

  @ParameterizedTest
  @ValueSource(longs = [124, 541, 12, 2, 1])
  fun hydrateDelegatesToRetryStatePersistence(jobId: Long) {
    val manager = RetryManager(null, null, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE)
    val activity = RetryStatePersistenceActivityImpl(mAirbyteApiClient, mRetryStateClient)
    whenever(mRetryStateClient.hydrateRetryState(eq(jobId), any<UUID>())).thenReturn(manager)

    val input = HydrateInput(jobId, UUID.randomUUID())
    val result = activity.hydrateRetryState(input)

    org.mockito.Mockito
      .verify(mRetryStateClient, org.mockito.Mockito.times(1))
      .hydrateRetryState(eq(jobId), any<UUID>())

    Assertions.assertEquals(manager, result.manager)
  }

  @ParameterizedTest
  @MethodSource("persistMatrix")
  @Throws(IOException::class)
  fun persistDelegatesToRetryStatePersistence(
    jobId: Long,
    connectionId: UUID,
  ) {
    val success = true
    val manager = RetryManager(null, null, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE)
    val activity = RetryStatePersistenceActivityImpl(mAirbyteApiClient, mRetryStateClient)
    whenever(mRetryStateClient.persistRetryState(jobId, connectionId, manager)).thenReturn(success)

    val input = PersistInput(jobId, connectionId, manager)
    val result = activity.persistRetryState(input)

    org.mockito.Mockito
      .verify(mRetryStateClient, org.mockito.Mockito.times(1))
      .persistRetryState(jobId, connectionId, manager)

    Assertions.assertEquals(success, result.success)
  }

  companion object {
    @JvmStatic
    fun persistMatrix(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(1L, UUID.randomUUID()),
        Arguments.of(134512351235L, UUID.randomUUID()),
        Arguments.of(8L, UUID.randomUUID()),
        Arguments.of(999L, UUID.randomUUID()),
      )
  }
}
