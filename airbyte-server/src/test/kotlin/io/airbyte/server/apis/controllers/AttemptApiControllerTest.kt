/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.InternalOperationResult
import io.airbyte.api.model.generated.SaveStatsRequestBody
import io.airbyte.commons.server.handlers.AttemptHandler
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
internal class AttemptApiControllerTest {
  @Inject
  lateinit var attemptHandler: AttemptHandler

  @Inject
  @Client("/")
  lateinit var client: HttpClient

  @MockBean(AttemptHandler::class)
  fun attemptHandler(): AttemptHandler = mockk()

  @Test
  fun testSaveState() {
    every { attemptHandler.saveStats(any()) } returns InternalOperationResult()

    val path = "/api/v1/attempt/save_stats"

    assertStatus(HttpStatus.OK, client.status(HttpRequest.POST(path, SaveStatsRequestBody())))
  }
}
