/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.api.client

import com.fasterxml.jackson.core.JsonProcessingException
import io.airbyte.api.client.ApiException
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.metrics.MetricClient
import io.airbyte.workload.api.domain.GcsDownscopedOAuthLogUploadAuthorization
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import retrofit2.Retrofit
import retrofit2.converter.jackson.JacksonConverterFactory
import retrofit2.create
import java.time.OffsetDateTime
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds

class LogUploadAuthorizationClientTest {
  private lateinit var server: MockWebServer
  private lateinit var meterRegistry: SimpleMeterRegistry
  private lateinit var client: WorkloadApiClient

  @BeforeEach
  fun setUp() {
    server = MockWebServer()
    server.start()
    meterRegistry = SimpleMeterRegistry()
    val api =
      Retrofit
        .Builder()
        .client(OkHttpClient.Builder().retryOnConnectionFailure(false).build())
        .baseUrl(server.url("/api/v1/workload/"))
        .addConverterFactory(JacksonConverterFactory.create(MoreMappers.initMapper()))
        .build()
        .create<WorkloadApi>()
    client =
      WorkloadApiClient(
        metricClient = MetricClient(meterRegistry),
        api = api,
        retryConfig = RetryPolicyConfig(delay = 1.milliseconds, maxRetries = 1, jitterFactor = 0.0),
      )
  }

  @AfterEach
  fun tearDown() {
    server.shutdown()
    meterRegistry.close()
  }

  @Test
  fun `HTTP 200 returns typed GCS authorization`() {
    server.enqueue(jsonResponse(200, GCS_AUTHORIZATION_JSON))

    val authorization = client.workloadLogUploadAuthorization(WORKLOAD_ID)

    assertThat(authorization)
      .isEqualTo(
        GcsDownscopedOAuthLogUploadAuthorization(
          accessToken = "opaque-token",
          expiresAt = OffsetDateTime.parse("2026-09-02T12:34:56Z"),
          bucketName = "customer-logs",
          objectKeyPrefix = "workspace/workload/",
        ),
      )
  }

  @Test
  fun `bodyless HTTP 200 is a protocol error`() {
    server.enqueue(MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json"))
    server.enqueue(jsonResponse(200, GCS_AUTHORIZATION_JSON))

    assertThrows<JsonProcessingException> { client.workloadLogUploadAuthorization(WORKLOAD_ID) }

    assertThat(server.requestCount).isEqualTo(1)
  }

  @Test
  fun `malformed authorization response is not retried`() {
    server.enqueue(jsonResponse(200, "{not-json"))
    server.enqueue(jsonResponse(200, GCS_AUTHORIZATION_JSON))

    assertThrows<JsonProcessingException> { client.workloadLogUploadAuthorization(WORKLOAD_ID) }

    assertThat(server.requestCount).isEqualTo(1)
  }

  @Test
  fun `unknown authorization response variant is not retried`() {
    server.enqueue(jsonResponse(200, """{"type":"AZURE_SAS"}"""))
    server.enqueue(jsonResponse(200, GCS_AUTHORIZATION_JSON))

    assertThrows<JsonProcessingException> { client.workloadLogUploadAuthorization(WORKLOAD_ID) }

    assertThat(server.requestCount).isEqualTo(1)
  }

  @Test
  fun `HTTP 204 returns no authorization and sends no request body`() {
    server.enqueue(MockResponse().setResponseCode(204))

    assertThat(client.workloadLogUploadAuthorization(WORKLOAD_ID)).isNull()

    val request = server.takeRequest()
    assertThat(request.method).isEqualTo("POST")
    assertThat(request.path).isEqualTo("/api/v1/workload/$WORKLOAD_ID/log-upload-authorization")
    assertThat(request.bodySize).isZero()
    assertThat(server.requestCount).isEqualTo(1)
  }

  @Test
  fun `HTTP 400 is not retried and surfaces as ApiException`() {
    server.enqueue(MockResponse().setResponseCode(400))

    val exception = assertThrows<ApiException> { client.workloadLogUploadAuthorization(WORKLOAD_ID) }

    assertThat(exception.statusCode).isEqualTo(400)
    assertThat(server.requestCount).isEqualTo(1)
  }

  @Test
  fun `authorization retry policy retries server errors without changing generic response retries`() {
    server.enqueue(MockResponse().setResponseCode(500))

    assertThrows<ApiException> { client.workloadGet(WORKLOAD_ID) }
    assertThat(server.requestCount).isEqualTo(1)

    server.enqueue(MockResponse().setResponseCode(503))
    server.enqueue(jsonResponse(200, GCS_AUTHORIZATION_JSON))

    assertThat(client.workloadLogUploadAuthorization(WORKLOAD_ID)).isNotNull()
    assertThat(server.requestCount).isEqualTo(3)
  }

  @Test
  fun `authorization retry policy retries transport exceptions`() {
    server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST))
    server.enqueue(jsonResponse(200, GCS_AUTHORIZATION_JSON))

    assertThat(client.workloadLogUploadAuthorization(WORKLOAD_ID)).isNotNull()
    assertThat(server.requestCount).isEqualTo(2)
  }

  @Test
  fun `authorization metrics use normalized route without workload identifier`() {
    server.enqueue(MockResponse().setResponseCode(503))
    server.enqueue(jsonResponse(200, GCS_AUTHORIZATION_JSON))

    client.workloadLogUploadAuthorization(WORKLOAD_ID)

    val requestMetrics = meterRegistry.meters.filter { it.id.name.startsWith("workload-api-client.") }
    assertThat(requestMetrics).isNotEmpty()
    requestMetrics.forEach { metric ->
      assertThat(metric.id.getTag("url")).isEqualTo("/api/v1/workload/{workloadId}/log-upload-authorization")
      assertThat(metric.id.getTag("workload-id")).isNull()
      assertThat(metric.id.tags.map { it.value }).noneMatch { it.contains(WORKLOAD_ID) }
    }
  }

  private fun jsonResponse(
    status: Int,
    body: String,
  ): MockResponse =
    MockResponse()
      .setResponseCode(status)
      .setHeader("Content-Type", "application/json")
      .setBody(body)

  companion object {
    private val WORKLOAD_ID = UUID.fromString("2aa689c6-203c-4e9c-b3b9-2953b20f1a25").toString()
    private const val GCS_AUTHORIZATION_JSON =
      """{"type":"GCS_DOWNSCOPED_OAUTH","accessToken":"opaque-token","expiresAt":"2026-09-02T12:34:56Z","bucketName":"customer-logs","objectKeyPrefix":"workspace/workload/"}"""
  }
}
