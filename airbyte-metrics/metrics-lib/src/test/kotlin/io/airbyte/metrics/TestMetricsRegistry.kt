/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics

internal enum class TestMetricsRegistry(
  private val metricName: String,
  private val metricDescription: String,
  private val metricVisibility: MetricVisibility = MetricVisibility.INTERNAL,
) : MetricsRegistry {
  TEST_PUBLIC_METRIC(
    metricName = "test-public",
    metricDescription = "test public metric description",
    metricVisibility = MetricVisibility.PUBLIC,
  ),
  TEST_INTERNAL_METRIC(
    metricName = "test-internal",
    metricDescription = "test internal metric description",
    metricVisibility = MetricVisibility.INTERNAL,
  ),
  ;

  override fun getMetricName(): String = metricName

  override fun getMetricDescription(): String = metricDescription

  override fun getMetricVisibility(): MetricVisibility = metricVisibility
}

class TestMetricsResolver : MetricsResolver {
  override fun resolve(metricId: String): MetricsRegistry? = TestMetricsRegistry.entries.find { it.getMetricName() == metricId }
}
