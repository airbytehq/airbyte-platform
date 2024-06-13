package io.airbyte.workload.launcher.metrics

import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.interceptors.InstrumentInterceptorBase
import io.airbyte.metrics.interceptors.MetricClientInstrumentInterceptor
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricTags
import io.micronaut.aop.InterceptorBean
import io.micronaut.context.annotation.Replaces
import jakarta.inject.Singleton
import kotlin.time.Duration
import kotlin.time.toJavaDuration

@Singleton
@InterceptorBean(Instrument::class)
@Replaces(MetricClientInstrumentInterceptor::class)
class CustomMetricPublisherInstrumentInterceptor(private val customMetricPublisher: CustomMetricPublisher) : InstrumentInterceptorBase() {
  override fun emitStartMetric(
    startMetricName: String,
    tags: Array<MetricAttribute>,
  ) {
    customMetricPublisher.count(WorkloadLauncherMetricMetadata.valueOf(startMetricName), *tags)
  }

  override fun emitEndMetric(
    endMetricName: String,
    success: Boolean,
    tags: Array<MetricAttribute>,
  ) {
    customMetricPublisher.count(
      WorkloadLauncherMetricMetadata.valueOf(endMetricName),
      MetricAttribute(MetricTags.STATUS, if (success) SUCCESS_STATUS else FAILURE_STATUS),
      *tags,
    )
  }

  override fun emitDurationMetric(
    durationMetricName: String,
    duration: Duration,
    success: Boolean,
    tags: Array<MetricAttribute>,
  ) {
    customMetricPublisher.timer(
      WorkloadLauncherMetricMetadata.valueOf(durationMetricName),
      duration.toJavaDuration(),
      MetricAttribute(MetricTags.STATUS, if (success) SUCCESS_STATUS else FAILURE_STATUS),
      *tags,
    )
  }
}
