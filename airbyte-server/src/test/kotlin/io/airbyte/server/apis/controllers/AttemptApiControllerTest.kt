/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.InternalOperationResult
import io.airbyte.api.model.generated.SaveStatsRequestBody
import io.airbyte.commons.server.handlers.AttemptHandler
import io.airbyte.server.assertStatus
import io.airbyte.server.status
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.mockk.every
import io.mockk.mockk
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
  fun testSaveState() {
    every { attemptHandler.saveStats(any()) } returns InternalOperationResult()

    val path = "/api/v1/attempt/save_stats"

    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SaveStatsRequestBody())))
  }
}
