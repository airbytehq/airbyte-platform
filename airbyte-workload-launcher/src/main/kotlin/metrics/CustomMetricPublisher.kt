package io.airbyte.workload.launcher.metrics

import io.airbyte.metrics.lib.MetricAttribute
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import jakarta.inject.Singleton
import java.util.function.ToDoubleFunction
import java.util.stream.Collectors
import java.util.stream.Stream

@Singleton
class CustomMetricPublisher(
  private val meterRegistry: MeterRegistry,
) {
  fun count(
    workloadLauncherMetricMetadata: WorkloadLauncherMetricMetadata,
    vararg attributes: MetricAttribute,
  ) {
    meterRegistry.counter(workloadLauncherMetricMetadata.metricName, toTags(*attributes)).increment()
  }

  fun <T> gauge(
    workloadLauncherMetricMetadata: WorkloadLauncherMetricMetadata,
    stateObject: T,
    valueFunction: ToDoubleFunction<T>,
    vararg attributes: MetricAttribute,
  ) {
    meterRegistry.gauge(workloadLauncherMetricMetadata.metricName, toTags(*attributes), stateObject, valueFunction)
  }

  private fun toTags(vararg attributes: MetricAttribute): List<Tag> {
    return Stream.of(*attributes).map { a: MetricAttribute -> Tag.of(a.key, a.value) }.collect(Collectors.toList())
  }
}
