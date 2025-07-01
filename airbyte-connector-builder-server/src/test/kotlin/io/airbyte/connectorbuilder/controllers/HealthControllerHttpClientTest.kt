/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.controllers

import io.airbyte.connectorbuilder.api.model.generated.HealthCheckRead
import io.airbyte.connectorbuilder.handlers.HealthHandler
import io.micronaut.context.annotation.Property
import io.micronaut.http.HttpRequest
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.http.client.exceptions.HttpClientResponseException
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

private const val AVAILABLE = true
private const val CDK_VERSION = "cdk 0.1.3.4-56_78"
private const val CUSTOM_CODE_EXECUTION = false

@MicronautTest
@Property(name = "CDK_PYTHON", value = "python")
@Property(name = "CDK_ENTRYPOINT", value = "entry")
class HealthControllerHttpClientTest {
  @MockBean(HealthHandler::class)
  fun healthHandler(): HealthHandler =
    mockk {
      every { getHealthCheck() } returns
        mockk {
          every { available } returns AVAILABLE
          every { cdkVersion } returns CDK_VERSION
          every { capabilities } returns
            mockk {
              every { customCodeExecution } returns CUSTOM_CODE_EXECUTION
            }
        }
    }

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Test
  fun `verify health endpoint`() {
    val request = HttpRequest.GET<Any>("/v1/health")
    val response = client.toBlocking().exchange(request, HealthCheckRead::class.java)

    assertEquals(200, response.status.code)

    val body = response.body()
    assertEquals(AVAILABLE, body.available)
    assertEquals(CDK_VERSION, body.cdkVersion)
    assertEquals(CUSTOM_CODE_EXECUTION, body.capabilities.customCodeExecution)
  }

  @Test
  fun `verify missing endpoint`() {
    val request = HttpRequest.GET<Any>("/v1/health/missing")
    val response = assertThrows<HttpClientResponseException> { client.toBlocking().exchange(request, HealthCheckRead::class.java) }

    assertEquals(404, response.status.code)
  }
}
