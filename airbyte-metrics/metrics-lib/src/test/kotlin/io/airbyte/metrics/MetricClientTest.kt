/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow

internal class MetricClientTest {
  @Test
  fun testCount() {
    val attributes = arrayOf(MetricAttribute("key", "value"), MetricAttribute("key2", "value2"))
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = 12345L
    val meterRegistry: MeterRegistry = SimpleMeterRegistry()
    val expectedTags = attributes.map { a -> Tag.of(a.key, a.value) }.toMutableList()
    val client = MetricClient(meterRegistry)
    val counter = client.count(metric = metric, value = value, attributes = attributes)

    assertNotNull(counter)
    assertEquals(metric.getMetricName(), counter?.id?.name)
    assertEquals(metric.getMetricDescription(), counter?.id?.description)
    assertEquals(expectedTags, counter?.id?.tags)
    assertEquals(value.toDouble(), counter?.count())
  }

  @Test
  fun testCountWithoutAttributes() {
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = 12345L
    val meterRegistry: MeterRegistry = SimpleMeterRegistry()
    val client = MetricClient(meterRegistry)
    val counter = client.count(metric = metric, value = value)

    assertNotNull(counter)
    assertEquals(metric.getMetricName(), counter?.id?.name)
    assertEquals(metric.getMetricDescription(), counter?.id?.description)
    assertEquals(emptyList<Tag>(), counter?.id?.tags)
    assertEquals(value.toDouble(), counter?.count())
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
    val expectedTags = attributes.map { a -> Tag.of(a.key, a.value) }.toMutableList()
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val meterRegistry: MeterRegistry = SimpleMeterRegistry()
    val client = MetricClient(meterRegistry)
    val counter = client.counter(metric = metric, attributes = attributes)

    assertNotNull(counter)
    assertEquals(metric.getMetricName(), counter?.id?.name)
    assertEquals(metric.getMetricDescription(), counter?.id?.description)
    assertEquals(expectedTags, counter?.id?.tags)
  }

  @Test
  fun testCreateCounterCountWithoutAttributes() {
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val meterRegistry: MeterRegistry = SimpleMeterRegistry()
    val client = MetricClient(meterRegistry)
    val counter = client.counter(metric = metric)

    assertNotNull(counter)
    assertEquals(metric.getMetricName(), counter?.id?.name)
    assertEquals(metric.getMetricDescription(), counter?.id?.description)
    assertEquals(emptyList<Tag>(), counter?.id?.tags)
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
    val meterRegistry: MeterRegistry = SimpleMeterRegistry()
    val expectedTags = attributes.map { a -> Tag.of(a.key, a.value) }.toMutableList()
    val client = MetricClient(meterRegistry)
    val distributionSummary = client.distribution(metric = metric, value = value.toDouble(), attributes = attributes)

    assertNotNull(distributionSummary)
    assertEquals(metric.getMetricName(), distributionSummary?.id?.name)
    assertEquals(metric.getMetricDescription(), distributionSummary?.id?.description)
    assertEquals(expectedTags, distributionSummary?.id?.tags)
    assertEquals(value.toDouble(), distributionSummary?.mean())
  }

  @Test
  fun testDistributionWithoutAttributes() {
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = 12345L
    val meterRegistry: MeterRegistry = SimpleMeterRegistry()
    val client = MetricClient(meterRegistry)
    val distributionSummary = client.distribution(metric = metric, value = value.toDouble())

    assertNotNull(distributionSummary)
    assertEquals(metric.getMetricName(), distributionSummary?.id?.name)
    assertEquals(metric.getMetricDescription(), distributionSummary?.id?.description)
    assertEquals(emptyList<Tag>(), distributionSummary?.id?.tags)
    assertEquals(value.toDouble(), distributionSummary?.mean())
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
    val meterRegistry: MeterRegistry = SimpleMeterRegistry()
    val expectedTags = attributes.map { a -> Tag.of(a.key, a.value) }.toMutableList()
    val client = MetricClient(meterRegistry)
    val timer = client.timer(metric = metric, attributes = attributes)

    assertNotNull(timer)
    assertEquals(metric.getMetricName(), timer?.id?.name)
    assertEquals(metric.getMetricDescription(), timer?.id?.description)
    assertEquals(expectedTags, timer?.id?.tags)
  }

  @Test
  fun testTimerWithoutAttributes() {
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val meterRegistry: MeterRegistry = SimpleMeterRegistry()
    val client = MetricClient(meterRegistry)
    val timer = client.timer(metric = metric)

    assertNotNull(timer)
    assertEquals(metric.getMetricName(), timer?.id?.name)
    assertEquals(metric.getMetricDescription(), timer?.id?.description)
    assertEquals(emptyList<Tag>(), timer?.id?.tags)
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
    val expectedTags = attributes.filterNotNull().map { a -> Tag.of(a.key, a.value) }.toMutableList()
    val metric = TestMetricsRegistry.TEST_INTERNAL_METRIC
    val value = 12345L

    val meterRegistry: MeterRegistry = SimpleMeterRegistry()

    val client = MetricClient(meterRegistry)

    assertDoesNotThrow {
      val counter = client.count(metric = metric, value = value, attributes = attributes)
      assertNotNull(counter)
      assertEquals(metric.getMetricName(), counter?.id?.name)
      assertEquals(metric.getMetricDescription(), counter?.id?.description)
      assertEquals(expectedTags, counter?.id?.tags)
      assertEquals(value.toDouble(), counter?.count())
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
