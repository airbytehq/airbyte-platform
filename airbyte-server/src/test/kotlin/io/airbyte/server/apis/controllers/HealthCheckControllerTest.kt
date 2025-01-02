/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.server.apis.controllers

import io.airbyte.api.model.generated.HealthCheckRead
import io.airbyte.commons.server.handlers.HealthCheckHandler
import io.airbyte.server.apis.HealthApiController
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class HealthCheckControllerTest {
  @Test
  fun testImportDefinitions() {
    val healthCheckHandler: HealthCheckHandler =
      mockk {
        every { health() } returns HealthCheckRead().available(false)
      }

    val configurationApi = HealthApiController(healthCheckHandler)
    Assertions.assertFalse(configurationApi.healthCheck.available)
  }
}
