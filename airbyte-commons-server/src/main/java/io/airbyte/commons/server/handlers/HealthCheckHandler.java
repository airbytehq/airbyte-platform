/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.HealthCheckRead;
import io.airbyte.data.services.HealthCheckService;
import jakarta.inject.Singleton;

/**
 * HealthCheckHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
public class HealthCheckHandler {

  private final HealthCheckService healthCheckService;

  public HealthCheckHandler(HealthCheckService healthCheckService) {
    this.healthCheckService = healthCheckService;
  }

  public HealthCheckRead health() {
    return new HealthCheckRead().available(healthCheckService.healthCheck());
  }

}
