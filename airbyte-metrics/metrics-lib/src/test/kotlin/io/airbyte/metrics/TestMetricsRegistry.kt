/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics

enum class TestMetricEmittingApp(
  override val applicationName: String,
) : MetricEmittingApp {
  TEST("test"),
}

internal enum class TestMetricsRegistry(
  private val application: MetricEmittingApp,
  private val metricName: String,
  private val metricDescription: String,
  private val metricVisibility: MetricVisibility = MetricVisibility.INTERNAL,
) : MetricsRegistry {
  TEST_PUBLIC_METRIC(
    application = TestMetricEmittingApp.TEST,
    metricName = "test-public",
    metricDescription = "test public metric description",
    metricVisibility = MetricVisibility.PUBLIC,
  ),
  TEST_INTERNAL_METRIC(
    application = TestMetricEmittingApp.TEST,
    metricName = "test-internal",
    metricDescription = "test internal metric description",
    metricVisibility = MetricVisibility.INTERNAL,
  ),
  ;

  override fun getApplication(): MetricEmittingApp = application

  override fun getMetricName(): String = metricName

  override fun getMetricDescription(): String = metricDescription

  override fun getMetricVisibility(): MetricVisibility = metricVisibility
}

class TestMetricsResolver : MetricsResolver {
  override fun resolve(metricId: String): MetricsRegistry? = TestMetricsRegistry.entries.find { it.getMetricName() == metricId }
}
