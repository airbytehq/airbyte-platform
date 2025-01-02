/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.model.generated.HealthCheckRead;
import io.airbyte.data.services.HealthCheckService;
import org.junit.jupiter.api.Test;

class HealthCheckHandlerTest {

  @Test
  void testDbHealthSucceed() {
    final var healthCheckService = mock(HealthCheckService.class);
    when(healthCheckService.healthCheck()).thenReturn(true);

    final HealthCheckHandler healthCheckHandler = new HealthCheckHandler(healthCheckService);
    assertEquals(new HealthCheckRead().available(true), healthCheckHandler.health());
  }

  @Test
  void testDbHealthFail() {
    final var healthCheckService = mock(HealthCheckService.class);
    when(healthCheckService.healthCheck()).thenReturn(false);

    final HealthCheckHandler healthCheckHandler = new HealthCheckHandler(healthCheckService);
    assertEquals(new HealthCheckRead().available(false), healthCheckHandler.health());
  }

}
