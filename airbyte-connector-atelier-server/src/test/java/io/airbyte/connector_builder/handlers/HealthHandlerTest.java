/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.handlers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.connector_builder.api.model.generated.HealthCheckRead;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HealthHandlerTest {

  private static final String CDK_VERSION = "0.0.0";
  private HealthHandler healthHandler;
  private CachedCdkVersionProviderDecorator cdkVersionProvider;

  @BeforeEach
  void setup() {
    cdkVersionProvider = mock(CachedCdkVersionProviderDecorator.class);
    healthHandler = new HealthHandler(cdkVersionProvider);
  }

  @Test
  void testHealthCheckReturnsCdkVersionFromProvider() {
    when(cdkVersionProvider.getCdkVersion()).thenReturn(CDK_VERSION);

    final HealthCheckRead healthCheck = healthHandler.getHealthCheck();

    assertTrue(healthCheck.getAvailable());
    assertEquals(CDK_VERSION, healthCheck.getCdkVersion());
  }

  @Test
  void testServerIsNotAvailableIfCdkVersionProviderThrowsAnException() {
    doThrow(new RuntimeException()).when(cdkVersionProvider).getCdkVersion();

    final HealthCheckRead healthCheck = healthHandler.getHealthCheck();

    assertFalse(healthCheck.getAvailable());
    assertNull(healthCheck.getCdkVersion());
  }

}
