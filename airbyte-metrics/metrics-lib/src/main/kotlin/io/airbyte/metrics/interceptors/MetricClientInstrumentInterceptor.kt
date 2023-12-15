package io.airbyte.metrics.interceptors

import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.lib.MetricAttribute
import io.airbyte.metrics.lib.MetricClient
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.metrics.lib.OssMetricsRegistry
import io.micronaut.aop.InterceptorBean
import jakarta.inject.Singleton
import kotlin.time.Duration
import kotlin.time.DurationUnit

@Singleton
@InterceptorBean(Instrument::class)
class MetricClientInstrumentInterceptor(private val metricClient: MetricClient) : InstrumentInterceptorBase() {
  override fun emitStartMetric(
    startMetricName: String,
    tags: Array<MetricAttribute>,
  ) {
    metricClient.count(OssMetricsRegistry.valueOf(startMetricName), 1, *tags)
  }

  override fun emitEndMetric(
    endMetricName: String,
    success: Boolean,
    tags: Array<MetricAttribute>,
  ) {
    metricClient.count(
      OssMetricsRegistry.valueOf(endMetricName),
      1,
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
    metricClient.distribution(
      OssMetricsRegistry.valueOf(durationMetricName),
      duration.toDouble(DurationUnit.MILLISECONDS),
      MetricAttribute(MetricTags.STATUS, if (success) SUCCESS_STATUS else FAILURE_STATUS),
      *tags,
    )
  }
}
