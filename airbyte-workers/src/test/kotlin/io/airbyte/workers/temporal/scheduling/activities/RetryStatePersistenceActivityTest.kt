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
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import java.util.UUID

internal class RetryStatePersistenceActivityTest {
  private lateinit var mAirbyteApiClient: AirbyteApiClient
  private lateinit var mRetryStateClient: RetryStateClient
  private lateinit var mWorkspaceApi: WorkspaceApi

  @BeforeEach
  fun setup() {
    mAirbyteApiClient = mockk<AirbyteApiClient>()
    mRetryStateClient = mockk<RetryStateClient>()
    mWorkspaceApi = mockk<WorkspaceApi>()
    every { mWorkspaceApi.getWorkspaceByConnectionId(any<ConnectionIdRequestBody>()) } returns
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
      )
    every { mAirbyteApiClient.workspaceApi } returns mWorkspaceApi
  }

  @ParameterizedTest
  @ValueSource(longs = [124, 541, 12, 2, 1])
  fun hydrateDelegatesToRetryStatePersistence(jobId: Long) {
    val manager = RetryManager(null, null, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE)
    val activity = RetryStatePersistenceActivityImpl(mAirbyteApiClient, mRetryStateClient)
    every { mRetryStateClient.hydrateRetryState(jobId, any<UUID>()) } returns manager

    val input = HydrateInput(jobId, UUID.randomUUID())
    val result = activity.hydrateRetryState(input)

    verify(exactly = 1) { mRetryStateClient.hydrateRetryState(jobId, any<UUID>()) }

    Assertions.assertEquals(manager, result.manager)
  }

  @ParameterizedTest
  @MethodSource("persistMatrix")
  fun persistDelegatesToRetryStatePersistence(
    jobId: Long,
    connectionId: UUID,
  ) {
    val success = true
    val manager = RetryManager(null, null, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE)
    val activity = RetryStatePersistenceActivityImpl(mAirbyteApiClient, mRetryStateClient)
    every { mRetryStateClient.persistRetryState(jobId, connectionId, manager) } returns success

    val input = PersistInput(jobId, connectionId, manager)
    val result = activity.persistRetryState(input)

    verify(exactly = 1) { mRetryStateClient.persistRetryState(jobId, connectionId, manager) }

    Assertions.assertEquals(success, result.success)
  }

  companion object {
    @JvmStatic
    fun persistMatrix() =
      listOf<Arguments?>(
        Arguments.of(1L, UUID.randomUUID()),
        Arguments.of(134512351235L, UUID.randomUUID()),
        Arguments.of(8L, UUID.randomUUID()),
        Arguments.of(999L, UUID.randomUUID()),
      )
  }
}
