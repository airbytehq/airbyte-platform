/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import jakarta.annotation.PreDestroy
import jakarta.inject.Singleton
import java.util.function.ToDoubleFunction

@Singleton
class MetricClient(
  private val meterRegistry: MeterRegistry?,
) {
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
  ) = count(metricName = metric.getMetricName(), attributes = attributes, value = value)

  /**
   * Increment or decrement a counter.
   *
   * @param metricName The name of the metric to record.
   * @param value The value to record.
   * @param attributes additional attributes
   */
  @JvmOverloads
  fun count(
    metricName: String,
    value: Long = 1L,
    vararg attributes: MetricAttribute?,
  ) = meterRegistry?.counter(metricName, toTags(attributes))?.increment(value.toDouble())

  /**
   * Creates a counter.
   *
   * @param metric The [MetricsRegistry] defined metric to record
   * @param attributes additional attributes
   */
  fun counter(
    metric: MetricsRegistry,
    vararg attributes: MetricAttribute?,
  ): Counter? = counter(metricName = metric.getMetricName(), attributes = attributes)

  /**
   * Creates a counter.
   *
   * @param metricName The name of the metric to record.
   * @param attributes additional attributes
   */
  fun counter(
    metricName: String,
    vararg attributes: MetricAttribute?,
  ): Counter? = meterRegistry?.counter(metricName, toTags(attributes))

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
  ): T = gauge(metricName = metric.getMetricName(), stateObject = stateObject, function = function, attributes = attributes)

  /**
   * Record the latest value of a state object for a gauge.
   *
   * @param metricName The name of the metric to record.
   * @param stateObject The object to monitor as part of the gauge.
   * @param function The function used to extract the double value when reporting the gauge's current value.
   * @param attributes additional attributes.
   *
   * @return The gauge-wrapped state object.
   */
  fun <T> gauge(
    metricName: String,
    stateObject: T,
    function: ToDoubleFunction<T>,
    vararg attributes: MetricAttribute?,
  ): T =
    meterRegistry?.gauge(
      metricName,
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
  ) = gauge(metricName = metric.getMetricName(), attributes = attributes, value = value)

  /**
   * Record the latest value for a gauge.
   *
   * @param metricName The name of the metric to record.
   * @param value The value to record.
   * @param attributes additional attributes
   */
  fun gauge(
    metricName: String,
    value: Double,
    vararg attributes: MetricAttribute?,
  ) = meterRegistry?.gauge(metricName, toTags(attributes), value)

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
  ) = distribution(metricName = metric.getMetricName(), attributes = attributes, value = value)

  /**
   * Accepts value on the metrics, and report the distribution of these values. Useful to analysis how
   * much time have elapsed, and percentile of a series of records.
   *
   * @param metricName The name of the metric to record.
   * @param value The value to record.
   * @param attributes additional attributes
   */
  fun distribution(
    metricName: String,
    value: Double,
    vararg attributes: MetricAttribute?,
  ) = meterRegistry?.summary(metricName, toTags(attributes))?.record(value)

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
  ): Timer? = timer(metricName = metric.getMetricName(), attributes = attributes)

  /**
   * Creates a [Timer].
   *
   * @param metricName The name of the timer.
   * @param attributes additional attributes.
   * @return A [Timer] or null if metrics are not configured.
   */
  fun timer(
    metricName: String,
    vararg attributes: MetricAttribute?,
  ): Timer? = meterRegistry?.timer(metricName, toTags(attributes))

  /**
   * Closes the client and any underlying resources.  This is important to ensure that any remaining metric values are published
   * prior to the shutdown of the containing application
   */
  @PreDestroy
  fun close() = meterRegistry?.close()

  private fun toTags(attributes: Array<out MetricAttribute?>): Iterable<Tag> = toList(attributes).map { m -> Tag.of(m.key, m.value) }.toMutableList()

  private fun toList(attributes: Array<out MetricAttribute?>): List<MetricAttribute> = attributes.mapNotNull { it }
}

/**
 * Custom tuple that represents a key/value pair to be included with a metric.
 */
data class MetricAttribute(
  val key: String = "",
  val value: String = "",
)
