/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics

/**
 * Interface representing metrics collected an Airbyte Application. This interface is present as
 * Java doesn't support enum inheritance as of Java 17. Not a registry, rather metric metadata/info
 */

interface MetricsRegistry {
  fun getMetricName(): String

  fun getMetricDescription(): String

  fun getMetricVisibility(): MetricVisibility = MetricVisibility.INTERNAL
}

interface MetricsResolver {
  fun resolve(metricId: String): MetricsRegistry?
}

enum class MetricVisibility(
  val level: Int,
) {
  INTERNAL(0),
  PUBLIC(1),
}
