/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.heath.indicator;

import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.AbstractHealthIndicator;
import io.micronaut.management.health.indicator.annotation.Liveness;
import java.util.Map;

/**
 * LivenessIndicator is a Micronaut health indicator that indicates if the server is alive. When the
 * server is not alive, it indicates that it should be restarted by the platform orchestrator (e.g.
 * Kubernetes)
 */
@Liveness
public class LivenessIndicator extends AbstractHealthIndicator<Map<String, Object>> {

  @Override
  protected Map<String, Object> getHealthInformation() {
    Map<String, Object> details = Map.of("alive", true);
    this.healthStatus = HealthStatus.UP;
    return details;
  }

  @Override
  protected String getName() {
    return "liveness";
  }

}
