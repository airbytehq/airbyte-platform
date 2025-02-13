/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.http.filter

import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.micronaut.core.order.Ordered
import io.micronaut.http.HttpRequest
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.annotation.RequestFilter
import io.micronaut.http.annotation.ServerFilter
import io.micronaut.http.annotation.ServerFilter.MATCH_ALL_PATTERN
import io.micronaut.http.filter.FilterContinuation
import io.micronaut.http.filter.ServerFilterPhase

@ServerFilter(patterns = [MATCH_ALL_PATTERN])
class ApiMetricFilter(
  private val metricClient: MetricClient,
  private val airbyteVersion: AirbyteVersion,
) : Ordered {
  @RequestFilter
  fun countApiMetrics(
    request: HttpRequest<*>,
    continuation: FilterContinuation<MutableHttpResponse<*>>,
  ) {
    val response = continuation.proceed()

    metricClient.count(
      metric = OssMetricsRegistry.API_REQUESTS,
      attributes =
        arrayOf(
          MetricAttribute("endpoint", request.uri.toString()),
          MetricAttribute("status", response.status.code.toString()),
          MetricAttribute("version", airbyteVersion.serialize()),
        ),
    )
  }

  override fun getOrder() = ServerFilterPhase.FIRST.order()
}
