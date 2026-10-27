/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.commons.server.handlers.AttemptHandler
import io.airbyte.server.assertStatus
import io.airbyte.server.statusException
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.MediaType
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.mockk
import io.mockk.verify
import jakarta.inject.Inject
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest
internal class AttemptApiControllerTest {
  @Inject
  lateinit var context: ApplicationContext

  lateinit var attemptHandler: AttemptHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @BeforeAll
  fun setupMock() {
    attemptHandler = mockk()
    context.registerSingleton(AttemptHandler::class.java, attemptHandler)
  }

  @Test
  fun `missing job and attempt reject before the handler`() {
    val path = "/api/v1/attempt/save_stats"

    assertStatus(HttpStatus.BAD_REQUEST, client.statusException(HttpRequest.POST(path, "{}").contentType(MediaType.APPLICATION_JSON_TYPE)))
    verify(exactly = 0) { attemptHandler.saveStats(any()) }
  }
}
