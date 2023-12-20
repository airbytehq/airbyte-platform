/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.metrics

import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.workload.metrics.CustomMetricPublisher.Companion.toTags
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.util.Optional

class CustomMetricPublisherTest {
  @Test
  internal fun `test publishing count metrics`() {
    val metricName = "metric-name"
    val metricAttribute = MetricAttribute("key", "value")
    val counter: Counter = mockk()
    val meterRegistry: MeterRegistry = mockk()

    every { counter.increment() } returns Unit
    every { meterRegistry.counter(metricName, toTags(metricAttribute)) } returns counter

    val publisher = CustomMetricPublisher(Optional.of(meterRegistry))

    publisher.count(metricName, metricAttribute)

    verify(exactly = 1) { counter.increment() }
  }

  @Test
  internal fun `test publishing guage metrics`() {
    val metricName = "metric-name"
    val metricAttribute = MetricAttribute("key", "value")
    val stateObject = 5
    val valueFunction = { v: Int -> (v * 2).toDouble() }
    val meterRegistry: MeterRegistry = mockk()

    every { meterRegistry.gauge(metricName, toTags(metricAttribute), stateObject, any()) } returns stateObject

    val publisher = CustomMetricPublisher(Optional.of(meterRegistry))

    publisher.gauge(metricName, stateObject, valueFunction, metricAttribute)

    verify(exactly = 1) { meterRegistry.gauge(metricName, toTags(metricAttribute), stateObject, any()) }
  }
}
