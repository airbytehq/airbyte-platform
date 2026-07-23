/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.WorkspaceApi
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.WorkspaceRead
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * Test for the CustomerIoEmailConfigFetcher.
 */
internal class CustomerIoEmailConfigFetcherTest {
  private lateinit var airbyteApiClient: AirbyteApiClient
  private lateinit var workspaceApi: WorkspaceApi
  private lateinit var cloudCustomerIoEmailConfigFetcher: CustomerIoEmailConfigFetcher

  @BeforeEach
  fun setup() {
    airbyteApiClient = mockk()
    workspaceApi = mockk()
    every { airbyteApiClient.workspaceApi } returns workspaceApi
    cloudCustomerIoEmailConfigFetcher = CustomerIoEmailConfigFetcherImpl(airbyteApiClient)
  }

  @Test
  fun testReturnTheRightConfig() {
    val connectionId = UUID.randomUUID()
    val email = "em@il.com"
    every { workspaceApi.getWorkspaceByConnectionId(ConnectionIdRequestBody(connectionId)) } returns
      WorkspaceRead(
        UUID.randomUUID(),
        UUID.randomUUID(),
        "name",
        "slug",
        true,
        UUID.randomUUID(),
        email,
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

    val customerIoEmailConfig = cloudCustomerIoEmailConfigFetcher.fetchConfig(connectionId)
    Assertions.assertEquals(email, customerIoEmailConfig!!.to)
  }
}
