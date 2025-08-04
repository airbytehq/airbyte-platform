/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.HealthCheckRead
import io.airbyte.data.services.HealthCheckService
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.mockito.Mockito

internal class HealthCheckHandlerTest {
  @Test
  fun testDbHealthSucceed() {
    val healthCheckService = Mockito.mock(HealthCheckService::class.java)
    Mockito.`when`(healthCheckService.healthCheck()).thenReturn(true)

    val healthCheckHandler = HealthCheckHandler(healthCheckService)
    Assertions.assertEquals(HealthCheckRead().available(true), healthCheckHandler.health())
  }

  @Test
  fun testDbHealthFail() {
    val healthCheckService = Mockito.mock(HealthCheckService::class.java)
    Mockito.`when`(healthCheckService.healthCheck()).thenReturn(false)

    val healthCheckHandler = HealthCheckHandler(healthCheckService)
    Assertions.assertEquals(HealthCheckRead().available(false), healthCheckHandler.health())
  }
}
