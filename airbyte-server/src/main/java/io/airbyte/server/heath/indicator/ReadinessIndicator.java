/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.heath.indicator;

import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.AbstractHealthIndicator;
import io.micronaut.management.health.indicator.annotation.Readiness;
import java.util.Map;

/**
 * ReadinessIndicator is a Micronaut health indicator that indicates if the server is ready to serve
 * traffic. A server is ready when it is listening for connections, and it is not in the process of
 * shutting down.
 */
@Readiness
public class ReadinessIndicator extends AbstractHealthIndicator<Map<String, Object>> {

  @Override
  protected Map<String, Object> getHealthInformation() {
    Map<String, Object> details = Map.of("ready", true);
    this.healthStatus = HealthStatus.UP;
    return details;
  }

  @Override
  protected String getName() {
    return "readiness";
  }

}
