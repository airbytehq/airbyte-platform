/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.config

import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.TestMetricsRegistry
import io.airbyte.metrics.TestMetricsResolver
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.config.MeterFilterReply
import io.micronaut.configuration.metrics.binder.web.config.HttpServerMeterConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

internal class AirbyteMetricMeterFilterTest {
  @Test
  fun testAddingPrefix() {
    val metricsResolvers = listOf(TestMetricsResolver())
    val metricName = "metric_name"
    val meterId = Meter.Id(metricName, Tags.of(emptyList()), null, null, Meter.Type.COUNTER)
    val filter =
      AirbyteMetricMeterFilter(
        airbyteEdition = AirbyteEdition.CLOUD,
        metricsResolvers = metricsResolvers,
      )
    assertEquals("$METRIC_PREFIX${meterId.name}", filter.map(meterId).name)
  }

  @Test
  fun testSkipAddingPrefix() {
    val metricsResolvers = listOf(TestMetricsResolver())
    val metricName = "${METRIC_PREFIX}metric_name"
    val meterId = Meter.Id(metricName, Tags.of(emptyList()), null, null, Meter.Type.COUNTER)
    val filter =
      AirbyteMetricMeterFilter(
        airbyteEdition = AirbyteEdition.CLOUD,
        metricsResolvers = metricsResolvers,
      )
    assertEquals(meterId.name, filter.map(meterId).name)
  }

  @Test
  fun testMappingMicronautMetricsToPublicMetrics() {
    val metricsResolvers = listOf(TestMetricsResolver())
    val metricName = HttpServerMeterConfig.REQUESTS_METRIC
    val meterId = Meter.Id(metricName, Tags.of(emptyList()), null, null, Meter.Type.COUNTER)
    val filter =
      AirbyteMetricMeterFilter(
        airbyteEdition = AirbyteEdition.CLOUD,
        metricsResolvers = metricsResolvers,
      )
    assertEquals("$METRIC_PREFIX${OssMetricsRegistry.API_REQUESTS.getMetricName()}", filter.map(meterId).name)
  }

  @Test
  fun testApplyingFilterMultipleTimes() {
    val metricsResolvers = listOf(TestMetricsResolver())
    val metricName = "metric_name"
    val meterId = Meter.Id(metricName, Tags.of(emptyList()), null, null, Meter.Type.COUNTER)
    val filter =
      AirbyteMetricMeterFilter(
        airbyteEdition = AirbyteEdition.CLOUD,
        metricsResolvers = metricsResolvers,
      )
    assertEquals("$METRIC_PREFIX${meterId.name}", filter.map(filter.map(meterId)).name)
  }

  @ParameterizedTest
  @CsvSource(
    "CLOUD,TEST_INTERNAL_METRIC,ACCEPT",
    "CLOUD,TEST_PUBLIC_METRIC,ACCEPT",
    "COMMUNITY,TEST_INTERNAL_METRIC,DENY",
    "COMMUNITY,TEST_PUBLIC_METRIC,DENY",
    "ENTERPRISE,TEST_INTERNAL_METRIC,DENY",
    "ENTERPRISE,TEST_PUBLIC_METRIC,ACCEPT",
  )
  fun testMetricFiltering(
    airbyteEdition: AirbyteEdition,
    metric: TestMetricsRegistry,
    expectedResult: MeterFilterReply,
  ) {
    val metricsResolvers = listOf(TestMetricsResolver())
    val metricName = "${METRIC_PREFIX}${metric.getMetricName()}"
    val meterId = Meter.Id(metricName, Tags.of(emptyList()), null, null, Meter.Type.COUNTER)
    val filter =
      AirbyteMetricMeterFilter(
        airbyteEdition = airbyteEdition,
        metricsResolvers = metricsResolvers,
      )
    val result = filter.accept(meterId)
    assertEquals(expectedResult, result)
  }

  @ParameterizedTest
  @CsvSource(
    "CLOUD,ACCEPT",
    "COMMUNITY,DENY",
    "ENTERPRISE,DENY",
  )
  fun testMetricFilteringUnknownMetric(
    airbyteEdition: AirbyteEdition,
    expectedResult: MeterFilterReply,
  ) {
    val metricsResolvers = listOf(TestMetricsResolver())
    val metricName = "${METRIC_PREFIX}unknown-metric"
    val meterId = Meter.Id(metricName, Tags.of(emptyList()), null, null, Meter.Type.COUNTER)
    val filter =
      AirbyteMetricMeterFilter(
        airbyteEdition = airbyteEdition,
        metricsResolvers = metricsResolvers,
      )
    val result = filter.accept(meterId)
    assertEquals(expectedResult, result)
  }
}
