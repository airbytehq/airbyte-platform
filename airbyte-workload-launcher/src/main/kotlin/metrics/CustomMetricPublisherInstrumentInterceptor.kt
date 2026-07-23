/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.metrics

import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.interceptors.InstrumentInterceptorBase
import io.airbyte.metrics.interceptors.MetricClientInstrumentInterceptor
import io.airbyte.metrics.lib.MetricTags
import io.micronaut.aop.InterceptorBean
import io.micronaut.context.annotation.Replaces
import jakarta.inject.Singleton
import kotlin.time.Duration
import kotlin.time.toJavaDuration

@Singleton
@InterceptorBean(Instrument::class)
@Replaces(MetricClientInstrumentInterceptor::class)
class CustomMetricPublisherInstrumentInterceptor(
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
        tags + arrayOf(MetricAttribute(MetricTags.STATUS, if (success) SUCCESS_STATUS else FAILURE_STATUS)),
    )
  }

  override fun emitDurationMetric(
    durationMetricName: String,
    duration: Duration,
    success: Boolean,
    tags: Array<MetricAttribute>,
  ) {
    metricClient
      .timer(
        metric = OssMetricsRegistry.valueOf(durationMetricName),
        attributes =
          tags + arrayOf(MetricAttribute(MetricTags.STATUS, if (success) SUCCESS_STATUS else FAILURE_STATUS)),
      )?.record(duration.toJavaDuration())
  }
}
