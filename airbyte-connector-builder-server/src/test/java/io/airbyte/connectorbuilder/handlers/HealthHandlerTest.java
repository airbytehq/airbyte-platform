/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connectorbuilder.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.connectorbuilder.api.model.generated.HealthCheckRead;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HealthHandlerTest {

  private static final String CDK_VERSION = "0.0.0";

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHealthCheckReturnsCdkVersionFromProvider(boolean enableUnsafeCode) {
    final HealthHandler healthHandler = new HealthHandler(CDK_VERSION, enableUnsafeCode);
    final HealthCheckRead healthCheck = healthHandler.getHealthCheck();

    assertTrue(healthCheck.getAvailable());
    assertEquals(CDK_VERSION, healthCheck.getCdkVersion());
    assertEquals(enableUnsafeCode, healthCheck.getCapabilities().getCustomCodeExecution());
  }

}
