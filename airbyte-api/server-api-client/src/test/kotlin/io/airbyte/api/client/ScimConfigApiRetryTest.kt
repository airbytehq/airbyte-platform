/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.client

import dev.failsafe.RetryPolicy
import io.airbyte.api.client.generated.ScimConfigApi
import io.airbyte.api.client.model.generated.EnableScimRequestBody
import io.airbyte.api.client.model.generated.OrganizationIdRequestBody
import io.airbyte.api.client.model.generated.ScimIdpProvider
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.IOException
import java.util.UUID

class ScimConfigApiRetryTest {
  private lateinit var server: MockWebServer
  private lateinit var client: ScimConfigApi

  @BeforeEach
  fun setUp() {
    server = MockWebServer()
    server.start()
    val sharedHttpClient = OkHttpClient.Builder().retryOnConnectionFailure(true).build()
    client =
      AirbyteApiClient(
        basePath = server.url("/").toString(),
        policy = RetryPolicy.ofDefaults(),
        httpClient = sharedHttpClient,
      ).scimConfigApi
  }

  @AfterEach
  fun tearDown() {
    server.shutdown()
  }

  @Test
  fun `initial enable is not retried after the response is lost`() {
    enqueueLostResponses()

    assertThrows<IOException> {
      client.enableScim(EnableScimRequestBody(UUID.randomUUID(), ScimIdpProvider.OKTA))
    }

    assertThat(server.requestCount).isEqualTo(1)
  }

  @Test
  fun `token rotation is not retried after the response is lost`() {
    enqueueLostResponses()

    assertThrows<IOException> {
      client.rotateScimToken(OrganizationIdRequestBody(UUID.randomUUID()))
    }

    assertThat(server.requestCount).isEqualTo(1)
  }

  @Test
  fun `directly constructed client disables OkHttp retries`() {
    val httpClient = directClient().client as OkHttpClient

    assertThat(httpClient.retryOnConnectionFailure).isFalse()
  }

  @Test
  fun `directly constructed client does not retry initial enable after the response is lost`() {
    enqueueLostResponses()

    assertThrows<IOException> {
      directClient().enableScim(EnableScimRequestBody(UUID.randomUUID(), ScimIdpProvider.OKTA))
    }

    assertThat(server.requestCount).isEqualTo(1)
  }

  @Test
  fun `directly constructed client does not retry token rotation after the response is lost`() {
    enqueueLostResponses()

    assertThrows<IOException> {
      directClient().rotateScimToken(OrganizationIdRequestBody(UUID.randomUUID()))
    }

    assertThat(server.requestCount).isEqualTo(1)
  }

  private fun directClient(): ScimConfigApi = ScimConfigApi(basePath = server.url("/").toString())

  private fun enqueueLostResponses() {
    repeat(3) {
      server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST))
    }
  }
}
