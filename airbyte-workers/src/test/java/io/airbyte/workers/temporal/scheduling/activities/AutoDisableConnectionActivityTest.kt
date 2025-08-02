/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.ConnectionApi
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.InternalOperationResult
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionActivityInput
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.mockito.kotlin.any
import org.mockito.kotlin.whenever
import java.io.IOException
import java.util.UUID

@ExtendWith(MockitoExtension::class)
internal class AutoDisableConnectionActivityTest {
  @Mock
  private lateinit var mAirbyteApiClient: AirbyteApiClient

  @Mock
  private lateinit var connectionApi: ConnectionApi

  private lateinit var activityInput: AutoDisableConnectionActivityInput

  private lateinit var autoDisableActivity: AutoDisableConnectionActivityImpl

  @BeforeEach
  fun setUp() {
    activityInput = AutoDisableConnectionActivityInput()
    activityInput.connectionId = CONNECTION_ID

    autoDisableActivity = AutoDisableConnectionActivityImpl(mAirbyteApiClient)
  }

  @Test
  @Throws(IOException::class)
  fun testConnectionAutoDisabled() {
    whenever(mAirbyteApiClient.connectionApi).thenReturn(connectionApi)
    whenever(connectionApi.autoDisableConnection(any<ConnectionIdRequestBody>()))
      .thenReturn(InternalOperationResult(true))
    val output = autoDisableActivity.autoDisableFailingConnection(activityInput)
    Assertions.assertTrue(output.isDisabled)
  }

  @Test
  @Throws(IOException::class)
  fun testConnectionNotAutoDisabled() {
    whenever(mAirbyteApiClient.connectionApi).thenReturn(connectionApi)
    whenever(connectionApi.autoDisableConnection(any<ConnectionIdRequestBody>()))
      .thenReturn(InternalOperationResult(false))
    val output = autoDisableActivity.autoDisableFailingConnection(activityInput)
    Assertions.assertFalse(output.isDisabled)
  }

  companion object {
    private val CONNECTION_ID: UUID = UUID.randomUUID()
  }
}
