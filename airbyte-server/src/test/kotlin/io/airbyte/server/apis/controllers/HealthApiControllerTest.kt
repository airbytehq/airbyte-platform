/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.HealthCheckRead
import io.airbyte.commons.server.handlers.HealthCheckHandler
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
import jakarta.inject.Inject
import org.junit.jupiter.api.Test

@MicronautTest
internal class HealthApiControllerTest {
  @Inject
  lateinit var healthCheckHandler: HealthCheckHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(HealthCheckHandler::class)
  fun healthCheckHandler(): HealthCheckHandler = mockk()

  @Test
  fun testHealth() {
    every { healthCheckHandler.health() } returns HealthCheckRead()
    assertStatus(HttpStatus.OK, client.status(HttpRequest.GET<Any>("/api/v1/health")))
  }
}
