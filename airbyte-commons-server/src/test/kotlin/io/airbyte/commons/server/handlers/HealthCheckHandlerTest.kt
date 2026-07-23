/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.model.generated.HealthCheckRead
import io.airbyte.data.services.HealthCheckService
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class HealthCheckHandlerTest {
  @Test
  fun testDbHealthSucceed() {
    val healthCheckService = mockk<HealthCheckService>()
    every { healthCheckService.healthCheck() } returns true

    val healthCheckHandler = HealthCheckHandler(healthCheckService)
    Assertions.assertEquals(HealthCheckRead().available(true), healthCheckHandler.health())
  }

  @Test
  fun testDbHealthFail() {
    val healthCheckService = mockk<HealthCheckService>()
    every { healthCheckService.healthCheck() } returns false

    val healthCheckHandler = HealthCheckHandler(healthCheckService)
    Assertions.assertEquals(HealthCheckRead().available(false), healthCheckHandler.health())
  }
}
