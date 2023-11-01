/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.api.server

import io.micronaut.http.HttpRequest
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

@MicronautTest
class HealthControllerTest(
  @Client("/") val client: HttpClient,
) {
  @Test
  fun testHealthEndpoint() {
    val request: HttpRequest<String> = HttpRequest.GET("/health")
    val body = client.toBlocking().retrieve(request)
    assertEquals("Successful operation", body)
  }
}
