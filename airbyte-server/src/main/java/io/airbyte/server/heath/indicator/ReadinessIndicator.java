/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.heath.indicator;

import io.airbyte.commons.server.handlers.HealthCheckHandler;
import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.AbstractHealthIndicator;
import io.micronaut.management.health.indicator.annotation.Readiness;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Map;

/**
 * ReadinessIndicator is a Micronaut health indicator that indicates if the server is ready to serve
 * traffic. A server is ready when it is listening for connections, and it is not in the process of
 * shutting down.
 */
@Readiness
@Singleton
public class ReadinessIndicator extends AbstractHealthIndicator<Map<String, Object>> {

  @Inject
  private HealthCheckHandler healthCheckHandler;

  @Override
  protected Map<String, Object> getHealthInformation() {
    var ready = healthCheckHandler.isReady();
    Map<String, Object> details = Map.of("ready", ready);
    if (ready) {
      this.healthStatus = HealthStatus.UP;
    } else {
      this.healthStatus = HealthStatus.DOWN;
    }
    return details;
  }

  @Override
  protected String getName() {
    return "readiness";
  }

}
