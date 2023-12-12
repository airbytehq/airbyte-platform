/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib;

import lombok.AllArgsConstructor;

/**
 * Enum containing all applications metrics are emitted for. Used to initialize
 * MetricClientFactory.initialize(...).
 *
 * Application Name Conventions:
 * <p>
 * - Drop the airbyte prefix when naming applications, e.g airbyte-server -> server.
 * <p>
 * - Use dashes to delimit application names with multiple words.
 * <p>
 * - Use lowercase.
 * <p>
 * Note: These names are used as metric name prefixes. Changing these names will affect
 * dashboard/alerts and our public Datadog integration. Please consult the platform teams if unsure.
 */
@AllArgsConstructor
public enum MetricEmittingApps implements MetricEmittingApp {

  CRON("cron"),
  METRICS_REPORTER("metrics-reporter"),
  ORCHESTRATOR("orchestrator"),
  SERVER("server"),
  WORKER("worker"),
  WORKLOAD_API("workload-api"),
  WORKLOAD_LAUNCHER("workload-launcher");

  private final String applicationName;

  @Override
  public String getApplicationName() {
    return this.applicationName;
  }

}
