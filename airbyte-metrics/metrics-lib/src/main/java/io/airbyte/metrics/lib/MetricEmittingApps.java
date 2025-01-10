/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.lib;

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
public enum MetricEmittingApps implements MetricEmittingApp {

  BILLING("billing"),
  BOOTLOADER("bootloader"),
  CRON("cron"),
  METRICS_REPORTER("metrics-reporter"),
  ORCHESTRATOR("orchestrator"),
  SERVER("server"),
  SIDECAR_ORCHESTRATOR("sidecar-orchestrator"),
  WORKER("worker"),
  WORKLOAD_API("workload-api"),
  WORKLOAD_INIT("workload-init"),
  WORKLOAD_LAUNCHER("workload-launcher"),
  CONNECTOR_BUILDER_SERVER("connector-builder-server");

  private final String applicationName;

  MetricEmittingApps(final String applicationName) {
    this.applicationName = applicationName;
  }

  @Override
  public String getApplicationName() {
    return this.applicationName;
  }

}
