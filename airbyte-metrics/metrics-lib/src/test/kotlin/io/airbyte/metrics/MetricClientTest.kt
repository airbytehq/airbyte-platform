/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow

internal class MetricClientTest {
  @Test
  fun testCount() {
    val attributes = arrayOf(MetricAttribute("key", "value"), MetricAttribute("key2", "value2"))
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = 12345L

    val micrometerCounter: Counter =
      mockk {
        every { increment(value.toDouble()) } returns Unit
      }
    val meterRegistry: MeterRegistry =
      mockk {
        every { counter(metric.getMetricName(), any<Iterable<Tag>>()) } returns micrometerCounter
      }

    val client = MetricClient(meterRegistry)
    client.count(metric = metric, value = value, attributes = attributes)

    verify(exactly = 1) { meterRegistry.counter(metric.getMetricName(), attributes.map { a -> Tag.of(a.key, a.value) }.toMutableList()) }
    verify(exactly = 1) { micrometerCounter.increment(value.toDouble()) }
  }

  @Test
  fun testCountWithoutAttributes() {
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = 12345L

    val micrometerCounter: Counter =
      mockk {
        every { increment(value.toDouble()) } returns Unit
      }
    val meterRegistry: MeterRegistry =
      mockk {
        every { counter(metric.getMetricName(), any<Iterable<Tag>>()) } returns micrometerCounter
      }

    val client = MetricClient(meterRegistry)
    client.count(metric = metric, value = value)

    verify(exactly = 1) { meterRegistry.counter(metric.getMetricName(), emptyList()) }
    verify(exactly = 1) { micrometerCounter.increment(value.toDouble()) }
  }

  @Test
  fun testCountWithoutMeterRegistry() {
    val attributes = arrayOf(MetricAttribute("key", "value"), MetricAttribute("key2", "value2"))
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = 12345L

    val client = MetricClient(null)

    assertDoesNotThrow {
      client.count(metric = metric, value = value, attributes = attributes)
    }
  }

  @Test
  fun testCreateCounter() {
    val attributes = arrayOf(MetricAttribute("key", "value"), MetricAttribute("key2", "value2"))
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC

    val micrometerCounter: Counter = mockk()
    val meterRegistry: MeterRegistry =
      mockk {
        every { counter(metric.getMetricName(), any<Iterable<Tag>>()) } returns micrometerCounter
      }

    val client = MetricClient(meterRegistry)
    val counter = client.counter(metric = metric, attributes = attributes)

    assertEquals(micrometerCounter, counter)
  }

  @Test
  fun testCreateCounterCountWithoutAttributes() {
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC

    val micrometerCounter: Counter = mockk()
    val meterRegistry: MeterRegistry =
      mockk {
        every { counter(metric.getMetricName(), any<Iterable<Tag>>()) } returns micrometerCounter
      }

    val client = MetricClient(meterRegistry)
    val counter = client.counter(metric = metric)

    assertEquals(micrometerCounter, counter)
  }

  @Test
  fun testCreateCounterWithoutMeterRegistry() {
    val attributes = arrayOf(MetricAttribute("key", "value"), MetricAttribute("key2", "value2"))
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC

    val client = MetricClient(null)

    assertDoesNotThrow {
      val counter = client.counter(metric = metric, attributes = attributes)
      assertNull(counter)
    }
  }

  @Test
  fun testGaugeWithStateObject() {
    val attributes = arrayOf(MetricAttribute("key", "value"), MetricAttribute("key2", "value2"))
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = listOf(1, 2, 3, 4, 5)
    val toDoubleFunction: (List<Int>) -> Double = { value.size.toDouble() }

    val meterRegistry: MeterRegistry =
      mockk {
        every { gauge(metric.getMetricName(), any<Iterable<Tag>>(), value, any()) } returns value
      }

    val client = MetricClient(meterRegistry)
    val wrapped = client.gauge(metric = metric, stateObject = value, function = toDoubleFunction, attributes = attributes)

    assertEquals(value, wrapped)
    verify(exactly = 1) { meterRegistry.gauge(metric.getMetricName(), attributes.map { a -> Tag.of(a.key, a.value) }.toMutableList(), value, any()) }
  }

  @Test
  fun testGaugeWithStateObjectWithoutAttributes() {
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = listOf(1, 2, 3, 4, 5)
    val toDoubleFunction: (List<Int>) -> Double = { value.size.toDouble() }

    val meterRegistry: MeterRegistry =
      mockk {
        every { gauge(metric.getMetricName(), any<Iterable<Tag>>(), value, any()) } returns value
      }

    val client = MetricClient(meterRegistry)
    val wrapped = client.gauge(metric = metric, stateObject = value, function = toDoubleFunction)

    assertEquals(value, wrapped)
    verify(exactly = 1) { meterRegistry.gauge(metric.getMetricName(), emptyList(), value, any()) }
  }

  @Test
  fun testGaugeWithStateObjectWithoutMeterRegistry() {
    val attributes = arrayOf(MetricAttribute("key", "value"), MetricAttribute("key2", "value2"))
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = listOf(1, 2, 3, 4, 5)
    val toDoubleFunction: (List<Int>) -> Double = { value.size.toDouble() }

    val client = MetricClient(null)

    assertDoesNotThrow {
      val wrapped = client.gauge(metric = metric, stateObject = value, function = toDoubleFunction, attributes = attributes)
      assertEquals(value, wrapped)
    }
  }

  @Test
  fun testGauge() {
    val attributes = arrayOf(MetricAttribute("key", "value"), MetricAttribute("key2", "value2"))
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = 12345.0

    val meterRegistry: MeterRegistry =
      mockk {
        every { gauge(metric.getMetricName(), any<Iterable<Tag>>(), value) } returns mockk()
      }

    val client = MetricClient(meterRegistry)
    client.gauge(metric = metric, value = value, attributes = attributes)

    verify(exactly = 1) { meterRegistry.gauge(metric.getMetricName(), attributes.map { a -> Tag.of(a.key, a.value) }.toMutableList(), value) }
  }

  @Test
  fun testGaugeWithoutAttributes() {
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = 12345.0

    val meterRegistry: MeterRegistry =
      mockk {
        every { gauge(metric.getMetricName(), any<Iterable<Tag>>(), value) } returns mockk()
      }

    val client = MetricClient(meterRegistry)
    client.gauge(metric = metric, value = value)

    verify(exactly = 1) { meterRegistry.gauge(metric.getMetricName(), emptyList(), value) }
  }

  @Test
  fun testGaugeWithoutMeterRegistry() {
    val attributes = arrayOf(MetricAttribute("key", "value"), MetricAttribute("key2", "value2"))
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = 12345.0

    val client = MetricClient(null)

    assertDoesNotThrow {
      client.gauge(metric = metric, value = value, attributes = attributes)
    }
  }

  @Test
  fun testDistribution() {
    val attributes = arrayOf(MetricAttribute("key", "value"), MetricAttribute("key2", "value2"))
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = 12345L

    val distributionSummary: DistributionSummary =
      mockk {
        every { record(value.toDouble()) } returns Unit
      }
    val meterRegistry: MeterRegistry =
      mockk {
        every { summary(metric.getMetricName(), any<Iterable<Tag>>()) } returns distributionSummary
      }

    val client = MetricClient(meterRegistry)
    client.distribution(metric = metric, value = value.toDouble(), attributes = attributes)

    verify(exactly = 1) { meterRegistry.summary(metric.getMetricName(), attributes.map { a -> Tag.of(a.key, a.value) }.toMutableList()) }
    verify(exactly = 1) { distributionSummary.record(value.toDouble()) }
  }

  @Test
  fun testDistributionWithoutAttributes() {
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = 12345L

    val distributionSummary: DistributionSummary =
      mockk {
        every { record(value.toDouble()) } returns Unit
      }
    val meterRegistry: MeterRegistry =
      mockk {
        every { summary(metric.getMetricName(), any<Iterable<Tag>>()) } returns distributionSummary
      }

    val client = MetricClient(meterRegistry)
    client.distribution(metric = metric, value = value.toDouble())

    verify(exactly = 1) { meterRegistry.summary(metric.getMetricName(), emptyList()) }
    verify(exactly = 1) { distributionSummary.record(value.toDouble()) }
  }

  @Test
  fun testDistributionWithoutMeterRegistry() {
    val attributes = arrayOf(MetricAttribute("key", "value"), MetricAttribute("key2", "value2"))
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = 12345L

    val client = MetricClient(null)

    assertDoesNotThrow {
      client.distribution(metric = metric, value = value.toDouble(), attributes = attributes)
    }
  }

  @Test
  fun testTimer() {
    val attributes = arrayOf(MetricAttribute("key", "value"), MetricAttribute("key2", "value2"))
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC

    val micrometerTimer: Timer = mockk()
    val meterRegistry: MeterRegistry =
      mockk {
        every { timer(metric.getMetricName(), any<Iterable<Tag>>()) } returns micrometerTimer
      }

    val client = MetricClient(meterRegistry)
    client.timer(metric = metric, attributes = attributes)

    verify(exactly = 1) { meterRegistry.timer(metric.getMetricName(), attributes.map { a -> Tag.of(a.key, a.value) }.toMutableList()) }
  }

  @Test
  fun testTimerWithoutAttributes() {
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC

    val micrometerTimer: Timer = mockk()
    val meterRegistry: MeterRegistry =
      mockk {
        every { timer(metric.getMetricName(), any<Iterable<Tag>>()) } returns micrometerTimer
      }

    val client = MetricClient(meterRegistry)
    client.timer(metric = metric)

    verify(exactly = 1) { meterRegistry.timer(metric.getMetricName(), emptyList()) }
  }

  @Test
  fun testTimerWithoutMeterRegistry() {
    val attributes = arrayOf(MetricAttribute("key", "value"), MetricAttribute("key2", "value2"))
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC

    val client = MetricClient(null)

    assertDoesNotThrow {
      client.timer(metric = metric, attributes = attributes)
    }
  }

  @Test
  fun testNullMetricAttributes() {
    val attributes = arrayOf(MetricAttribute("key", "value"), null, MetricAttribute("key2", "value2"), null)
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = 12345L

    val micrometerCounter: Counter =
      mockk {
        every { increment(value.toDouble()) } returns Unit
      }
    val meterRegistry: MeterRegistry =
      mockk {
        every { counter(metric.getMetricName(), any<Iterable<Tag>>()) } returns micrometerCounter
      }

    val client = MetricClient(meterRegistry)

    assertDoesNotThrow {
      client.count(metric = metric, value = value, attributes = attributes)
      verify(exactly = 1) { meterRegistry.counter(metric.getMetricName(), any<Iterable<Tag>>()) }
      verify(exactly = 1) { micrometerCounter.increment(value.toDouble()) }
    }
  }

  @Test
  fun testClose() {
    val meterRegistry: MeterRegistry =
      mockk {
        every { close() } returns Unit
      }
    val client = MetricClient(meterRegistry)
    client.close()
    verify(exactly = 1) { meterRegistry.close() }
  }

  @Test
  fun testCloseWithoutMeterRegistry() {
    val client = MetricClient(null)
    assertDoesNotThrow {
      client.close()
    }
  }
}
