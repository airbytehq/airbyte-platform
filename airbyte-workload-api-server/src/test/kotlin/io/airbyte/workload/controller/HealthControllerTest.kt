/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.controller

import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import jakarta.inject.Inject
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest
class HealthControllerTest {
  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @Test
  fun `endpoint returns ok status`() {
    val response = client.toBlocking().exchange("/health", HealthResponse::class.java)
    assertEquals(200, response.status.code)
    assertEquals("UP", response.body().status)
  }
}
