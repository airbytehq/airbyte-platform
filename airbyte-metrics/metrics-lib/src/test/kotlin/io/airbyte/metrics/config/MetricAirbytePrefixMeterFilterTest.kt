/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.config

import io.airbyte.metrics.config.MetricAirbytePrefixMeterFilter.Companion.PREFIX
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.Tags
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class MetricAirbytePrefixMeterFilterTest {
  @Test
  internal fun testAddingPrefix() {
    val metricName = "metric_name"
    val meterId = Meter.Id(metricName, Tags.of(emptyList()), null, null, Meter.Type.COUNTER)
    val filter = MetricAirbytePrefixMeterFilter()
    assertEquals("$PREFIX.${meterId.name}", filter.map(meterId).name)
  }

  @Test
  internal fun testSkipAddingPrefix() {
    val metricName = "$PREFIX.metric_name"
    val meterId = Meter.Id(metricName, Tags.of(emptyList()), null, null, Meter.Type.COUNTER)
    val filter = MetricAirbytePrefixMeterFilter()
    assertEquals("$PREFIX.${meterId.name}", filter.map(meterId).name)
  }
}
