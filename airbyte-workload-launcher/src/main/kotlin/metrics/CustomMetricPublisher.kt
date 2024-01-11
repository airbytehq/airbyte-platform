package io.airbyte.workload.launcher.metrics

import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import jakarta.inject.Singleton
import java.time.Duration
import java.util.Optional
import java.util.function.ToDoubleFunction
import java.util.stream.Collectors
import java.util.stream.Stream

@Singleton
class CustomMetricPublisher(
  private val maybeMeterRegistry: Optional<MeterRegistry>,
  private val metricClient: MetricClient,
) {
  fun count(
    workloadLauncherMetricMetadata: WorkloadLauncherMetricMetadata,
    vararg attributes: MetricAttribute,
  ) {
    maybeMeterRegistry.ifPresent { it.counter(workloadLauncherMetricMetadata.metricName, toTags(*attributes)).increment() }
  }

  fun timer(
    workloadLauncherMetricMetadata: WorkloadLauncherMetricMetadata,
    duration: Duration,
    vararg attributes: MetricAttribute,
  ) {
    maybeMeterRegistry.ifPresent { it.timer(workloadLauncherMetricMetadata.metricName, toTags(*attributes)).record(duration) }
  }

  fun <T> gauge(
    workloadLauncherMetricMetadata: WorkloadLauncherMetricMetadata,
    stateObject: T,
    valueFunction: ToDoubleFunction<T>,
    vararg attributes: MetricAttribute,
  ) {
    maybeMeterRegistry.ifPresent { it.gauge(workloadLauncherMetricMetadata.metricName, toTags(*attributes), stateObject, valueFunction) }
  }

  private fun toTags(vararg attributes: MetricAttribute): List<Tag> {
    return Stream.of(*attributes).map { a: MetricAttribute -> Tag.of(a.key, a.value) }.collect(Collectors.toList())
  }

  /**
   * I'm proxying the MetricClient for now instead of doing a proper implementation based off the meter registry.
   * TODO: translate this to use the MeterRegistry instead of MetricClient.
   */
  fun distribution(
    workloadLauncherMetricMetadata: WorkloadLauncherMetricMetadata,
    value: Double,
    vararg attributes: MetricAttribute,
  ) {
    metricClient.distribution(
      workloadLauncherMetricMetadata,
      value,
      *attributes,
    )
  }
}
