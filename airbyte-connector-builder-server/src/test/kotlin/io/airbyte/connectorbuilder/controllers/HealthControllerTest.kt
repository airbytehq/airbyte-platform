/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.controllers

import io.airbyte.connectorbuilder.api.model.generated.HealthCheckRead
import io.airbyte.connectorbuilder.handlers.HealthHandler
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class HealthControllerTest {
  @Test
  fun `healthCheckDeprecated should return HealthCheckRead`() {
    val healthCheckRead = mockk<HealthCheckRead>()
    val handler = mockk<HealthHandler> { every { getHealthCheck() } returns healthCheckRead }

    val controller = HealthController(healthHandler = handler)

    assertEquals(healthCheckRead, controller.healthCheckDeprecated)
  }
}
