/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.metrics

import io.airbyte.metrics.lib.MetricAttribute
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import jakarta.inject.Singleton
import java.util.Optional
import java.util.function.ToDoubleFunction
import java.util.stream.Collectors
import java.util.stream.Stream

@Singleton
class CustomMetricPublisher(
  private val maybeMeterRegistry: Optional<MeterRegistry>,
) {
  fun count(
    metricName: String,
    vararg attributes: MetricAttribute,
  ) {
    maybeMeterRegistry.ifPresent { it.counter(metricName, toTags(*attributes)).increment() }
  }

  fun <T> gauge(
    metricName: String,
    stateObject: T,
    valueFunction: ToDoubleFunction<T>,
    vararg attributes: MetricAttribute,
  ) {
    maybeMeterRegistry.ifPresent { it.gauge(metricName, toTags(*attributes), stateObject, valueFunction) }
  }

  companion object {
    fun toTags(vararg attributes: MetricAttribute): List<Tag> {
      return Stream.of(*attributes).map { a: MetricAttribute -> Tag.of(a.key, a.value) }.collect(Collectors.toList())
    }
  }
}
