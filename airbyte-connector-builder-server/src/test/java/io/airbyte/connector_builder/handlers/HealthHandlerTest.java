/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.airbyte.connector_builder.api.model.generated.HealthCheckRead;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HealthHandlerTest {

  private static final String CDK_VERSION = "0.0.0";
  private HealthHandler healthHandler;

  @BeforeEach
  void setup() {
    healthHandler = new HealthHandler(CDK_VERSION);
  }

  @Test
  void testHealthCheckReturnsCdkVersionFromProvider() {
    final HealthCheckRead healthCheck = healthHandler.getHealthCheck();

    assertTrue(healthCheck.getAvailable());
    assertEquals(CDK_VERSION, healthCheck.getCdkVersion());
  }

}
