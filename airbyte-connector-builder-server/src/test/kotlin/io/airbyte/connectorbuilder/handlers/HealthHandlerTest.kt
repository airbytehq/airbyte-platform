/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.handlers

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

internal class HealthHandlerTest {
  @ParameterizedTest
  @ValueSource(booleans = [true, false])
  fun testHealthCheckReturnsCdkVersionFromProvider(enableUnsafeCode: Boolean) {
    val healthHandler = HealthHandler(CDK_VERSION, enableUnsafeCode)
    val healthCheck = healthHandler.getHealthCheck()

    Assertions.assertTrue(healthCheck.available)
    Assertions.assertEquals(CDK_VERSION, healthCheck.cdkVersion)
    Assertions.assertEquals(enableUnsafeCode, healthCheck.capabilities.customCodeExecution)
  }

  companion object {
    private const val CDK_VERSION = "0.0.0"
  }
}
