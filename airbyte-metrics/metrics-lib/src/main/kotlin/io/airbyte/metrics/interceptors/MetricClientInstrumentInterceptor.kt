/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.interceptors

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.lib.MetricTags
import io.micronaut.aop.InterceptorBean
import jakarta.inject.Singleton
import kotlin.time.Duration
import kotlin.time.DurationUnit

@Singleton
@InterceptorBean(Instrument::class)
class MetricClientInstrumentInterceptor(
  private val metricClient: MetricClient,
) : InstrumentInterceptorBase() {
  override fun emitStartMetric(
    startMetricName: String,
    tags: Array<MetricAttribute>,
  ) {
    metricClient.count(metric = OssMetricsRegistry.valueOf(startMetricName), attributes = tags)
  }

  override fun emitEndMetric(
    endMetricName: String,
    success: Boolean,
    tags: Array<MetricAttribute>,
  ) {
    metricClient.count(
      metric = OssMetricsRegistry.valueOf(endMetricName),
      attributes =
        arrayOf(
          MetricAttribute(MetricTags.STATUS, if (success) SUCCESS_STATUS else FAILURE_STATUS),
        ) + tags,
    )
  }

  override fun emitDurationMetric(
    durationMetricName: String,
    duration: Duration,
    success: Boolean,
    tags: Array<MetricAttribute>,
  ) {
    metricClient.distribution(
      metric = OssMetricsRegistry.valueOf(durationMetricName),
      value = duration.toDouble(DurationUnit.MILLISECONDS),
      attributes =
        arrayOf(
          MetricAttribute(MetricTags.STATUS, if (success) SUCCESS_STATUS else FAILURE_STATUS),
        ) + tags,
    )
  }
}
