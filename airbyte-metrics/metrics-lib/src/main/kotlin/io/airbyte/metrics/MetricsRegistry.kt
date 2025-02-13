/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics

/**
 * Interface representing metrics collected an Airbyte Application. This interface is present as
 * Java doesn't support enum inheritance as of Java 17. Not a registry, rather metric metadata/info
 */

interface MetricsRegistry {
  fun getApplication(): MetricEmittingApp

  fun getMetricName(): String

  fun getMetricDescription(): String

  fun getMetricVisibility(): MetricVisibility = MetricVisibility.INTERNAL
}

interface MetricsResolver {
  fun resolve(metricId: String): MetricsRegistry?
}

/**
 * Interface representing an Airbyte Application to collect metrics for. This interface is present
 * as Java doesn't support enum inheritance as of Java 17. We use a shared interface so this
 * interface can be used in the [MetricsRegistry] enum.
 */
interface MetricEmittingApp {
  val applicationName: String
}

/**
 * Enum containing all applications metrics are emitted for.
 * Application Name Conventions:
 * - Drop the airbyte prefix when naming applications, e.g. airbyte-server -> server.
 * - Use dashes to delimit application names with multiple words.
 * - Use lowercase.
 * Note: These names are used as metric name prefixes. Changing these names will affect
 * dashboard/alerts and our public Datadog integration. Please consult the platform teams if unsure.
 */
enum class MetricEmittingApps(
  override val applicationName: String,
) : MetricEmittingApp {
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
  CONNECTOR_BUILDER_SERVER("connector-builder-server"),
}

enum class MetricVisibility(
  val level: Int,
) {
  INTERNAL(0),
  PUBLIC(1),
}
