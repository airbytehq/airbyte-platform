/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import jakarta.annotation.PreDestroy
import jakarta.inject.Singleton
import java.util.function.ToDoubleFunction

/**
 * Converts an array of [MetricAttribute]s to an [Iterable] of Micrometer [Tag] objects.
 *
 * @param attributes An array of [MetricAttribute]s.
 * @return An [Iterable] of Micrometer [Tag]s.
 */
fun toTags(attributes: Array<out MetricAttribute?>): Iterable<Tag> = toList(attributes).map { m -> Tag.of(m.key, m.value) }.toMutableList()

/**
 * Converts an array of [MetricAttribute]s to a [List] of [MetricAttribute]s.
 *
 * @param attributes An array of [MetricAttribute]s.
 * @returns A [List] of [MetricAttribute]s.
 */
fun toList(attributes: Array<out MetricAttribute?>): List<MetricAttribute> = attributes.mapNotNull { it }

@Singleton
class MetricClient(
  private val meterRegistry: MeterRegistry?,
) {
  // Internal cache to avoid counter lookups
  private val counterCache: Cache<String, Meter.MeterProvider<Counter>?> =
    Caffeine
      .newBuilder()
      .maximumSize(10000)
      .build<String, Meter.MeterProvider<Counter>?>()

  // Internal cache to avoid distribution summary lookups
  private val distributionSummaryCache: Cache<String, Meter.MeterProvider<DistributionSummary>?> =
    Caffeine
      .newBuilder()
      .maximumSize(10000)
      .build<String, Meter.MeterProvider<DistributionSummary>?>()

  // Internal cache to avoid timer lookups
  private val timerCache: Cache<String, Meter.MeterProvider<Timer>?> =
    Caffeine
      .newBuilder()
      .maximumSize(10000)
      .build<String, Meter.MeterProvider<Timer>?>()

  /**
   * Increment or decrement a counter.
   *
   * @param metric The [MetricsRegistry] defined metric to record
   * @param value The value to record.
   * @param attributes additional attributes
   */
  @JvmOverloads
  fun count(
    metric: MetricsRegistry,
    value: Long = 1L,
    vararg attributes: MetricAttribute?,
  ): Counter? {
    val provider = counterProvider(metric = metric)
    val counter = provider?.withTags(toTags(attributes))
    counter?.increment(value.toDouble())
    return counter
  }

  /**
   * Creates a counter.
   *
   * @param metric The [MetricsRegistry] defined metric to record
   * @param attributes additional attributes
   */
  fun counter(
    metric: MetricsRegistry,
    vararg attributes: MetricAttribute?,
  ): Counter? {
    val provider = counterProvider(metric = metric)
    val counter = provider?.withTags(toTags(attributes))
    return counter
  }

  /**
   * Record the latest value of a state object for a gauge.
   *
   * @param metric The [MetricsRegistry] defined metric to record
   * @param stateObject The object to monitor as part of the gauge.
   * @param function The function used to extract the double value when reporting the gauge's current value.
   * @param attributes additional attributes.
   *
   * @return The gauge-wrapped state object.
   */
  fun <T> gauge(
    metric: MetricsRegistry,
    stateObject: T,
    function: ToDoubleFunction<T>,
    vararg attributes: MetricAttribute?,
  ): T =
    meterRegistry?.gauge(
      metric.getMetricName(),
      toTags(attributes),
      stateObject,
      function,
    ) ?: stateObject

  /**
   * Record the latest value for a gauge.
   *
   * @param metric The [MetricsRegistry] defined metric to record
   * @param value The value to record.
   * @param attributes additional attributes
   */
  fun gauge(
    metric: MetricsRegistry,
    value: Double,
    vararg attributes: MetricAttribute?,
  ) = meterRegistry?.gauge(metric.getMetricName(), toTags(attributes), value)

  /**
   * Accepts value on the metrics, and report the distribution of these values. Useful to analysis how
   * much time have elapsed, and percentile of a series of records.
   *
   * @param metric The [MetricsRegistry] defined metric to record
   * @param value The value to record.
   * @param attributes additional attributes
   */
  fun distribution(
    metric: MetricsRegistry,
    value: Double,
    vararg attributes: MetricAttribute?,
  ): DistributionSummary? {
    val provider = distributionSummaryProvider(metric = metric)
    val summary = provider?.withTags(toTags(attributes))
    summary?.record(value)
    return summary
  }

  /**
   * Creates a [Timer].
   *
   * @param metric The [MetricsRegistry] defined metric to record as a timer
   * @param attributes additional attributes.
   * @return A [Timer] or null if metrics are not configured.
   */
  fun timer(
    metric: MetricsRegistry,
    vararg attributes: MetricAttribute?,
  ): Timer? {
    val provider = timerProvider(metric = metric)
    return provider?.withTags(toTags(attributes))
  }

  /**
   * Closes the client and any underlying resources.  This is important to ensure that any remaining metric values are published
   * prior to the shutdown of the containing application
   */
  @PreDestroy
  fun close() = meterRegistry?.close()

  /**
   * Builds a [Counter] [Meter.MeterProvider] to reduce meter allocations for repeated calls.
   *
   * @param metric The metric definition
   * @return A [Meter.MeterProvider] wrapping the [Counter].
   */
  private fun counterProvider(metric: MetricsRegistry): Meter.MeterProvider<Counter>? =
    counterCache.get(metric.getMetricName()) { k ->
      meterRegistry?.let { registry ->
        Counter.builder(k).description(metric.getMetricDescription()).withRegistry(registry)
      }
    }

  /**
   * Builds a [DistributionSummary] [Meter.MeterProvider] to reduce meter allocations for repeated calls.
   *
   * @param metric The metric definition
   * @return A [Meter.MeterProvider] wrapping the [DistributionSummary].
   */
  private fun distributionSummaryProvider(metric: MetricsRegistry): Meter.MeterProvider<DistributionSummary>? =
    distributionSummaryCache.get(metric.getMetricName()) { k ->
      meterRegistry?.let { registry ->
        DistributionSummary.builder(k).description(metric.getMetricDescription()).withRegistry(registry)
      }
    }

  /**
   * Builds a [Timer] [Meter.MeterProvider] to reduce meter allocations for repeated calls.
   *
   * @param metric The metric definition
   * @return A [Meter.MeterProvider] wrapping the [Timer].
   */
  private fun timerProvider(metric: MetricsRegistry): Meter.MeterProvider<Timer>? =
    timerCache.get(metric.getMetricName()) { k ->
      meterRegistry?.let { registry ->
        Timer.builder(k).description(metric.getMetricDescription()).withRegistry(registry)
      }
    }
}

/**
 * Custom tuple that represents a key/value pair to be included with a metric.
 */
data class MetricAttribute(
  val key: String = "",
  val value: String = "",
)
