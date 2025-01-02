/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector_builder.config;

import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricEmittingApps;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;

/**
 * Micronaut singleton factory for the creation and initialization of the {@link MetricClient}.
 */
@Factory
public class MetricClientFactory {

  /**
   * Creates and initializes the {@link MetricClient}.
   *
   * @return The initialized {@link MetricClient}.
   */
  @Singleton
  @Replaces(MetricClient.class)
  public MetricClient metricClient() {
    // Initialize the metric client
    io.airbyte.metrics.lib.MetricClientFactory.initialize(MetricEmittingApps.CONNECTOR_BUILDER_SERVER);
    return io.airbyte.metrics.lib.MetricClientFactory.getMetricClient();
  }

}
