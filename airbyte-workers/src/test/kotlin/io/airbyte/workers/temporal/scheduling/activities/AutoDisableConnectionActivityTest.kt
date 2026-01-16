/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectionApi
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.InternalOperationResult
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionActivityInput
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class AutoDisableConnectionActivityTest {
  private lateinit var mAirbyteApiClient: AirbyteApiClient
  private lateinit var connectionApi: ConnectionApi
  private lateinit var activityInput: AutoDisableConnectionActivityInput
  private lateinit var autoDisableActivity: AutoDisableConnectionActivityImpl

  @BeforeEach
  fun setUp() {
    mAirbyteApiClient = mockk()
    connectionApi = mockk()

    activityInput = AutoDisableConnectionActivityInput()
    activityInput.connectionId = CONNECTION_ID

    autoDisableActivity = AutoDisableConnectionActivityImpl(mAirbyteApiClient)
  }

  @Test
  fun testConnectionAutoDisabled() {
    every { mAirbyteApiClient.connectionApi } returns connectionApi
    every { connectionApi.autoDisableConnection(any<ConnectionIdRequestBody>()) } returns InternalOperationResult(true)
    val output = autoDisableActivity.autoDisableFailingConnection(activityInput)
    Assertions.assertTrue(output.isDisabled)
  }

  @Test
  fun testConnectionNotAutoDisabled() {
    every { mAirbyteApiClient.connectionApi } returns connectionApi
    every { connectionApi.autoDisableConnection(any<ConnectionIdRequestBody>()) } returns InternalOperationResult(false)
    val output = autoDisableActivity.autoDisableFailingConnection(activityInput)
    Assertions.assertFalse(output.isDisabled)
  }

  @Test
  fun testConnectionNotAutoDisabledNullConnectionId() {
    val output = autoDisableActivity.autoDisableFailingConnection(AutoDisableConnectionActivityInput())
    Assertions.assertFalse(output.isDisabled)
  }

  companion object {
    private val CONNECTION_ID: UUID = UUID.randomUUID()
  }
}
