/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.notification

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.generated.WorkspaceApi
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.WorkspaceRead
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import java.io.IOException
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
    airbyteApiClient = Mockito.mock(AirbyteApiClient::class.java)
    workspaceApi = Mockito.mock(WorkspaceApi::class.java)
    Mockito.`when`(airbyteApiClient.workspaceApi).thenReturn(workspaceApi)
    cloudCustomerIoEmailConfigFetcher = CustomerIoEmailConfigFetcherImpl(airbyteApiClient)
  }

  @Test
  @Throws(IOException::class)
  fun testReturnTheRightConfig() {
    val connectionId = UUID.randomUUID()
    val email = "em@il.com"
    Mockito
      .`when`(workspaceApi.getWorkspaceByConnectionId(ConnectionIdRequestBody(connectionId)))
      .thenReturn(
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
        ),
      )

    val customerIoEmailConfig = cloudCustomerIoEmailConfigFetcher.fetchConfig(connectionId)
    Assertions.assertEquals(email, customerIoEmailConfig!!.to)
  }
}
