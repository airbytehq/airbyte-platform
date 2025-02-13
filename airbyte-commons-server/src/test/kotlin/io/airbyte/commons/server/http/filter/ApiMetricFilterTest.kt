/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.http.filter

import io.airbyte.commons.version.AirbyteVersion
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpStatus
import io.micronaut.http.MutableHttpResponse
import io.micronaut.http.filter.FilterContinuation
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.net.URI

internal class ApiMetricFilterTest {
  @Test
  fun testRecordMetric() {
    val metricClient =
      mockk<MetricClient> {
        every { count(metric = OssMetricsRegistry.API_REQUESTS, value = 1L, attributes = anyVararg()) } returns Unit
      }
    val statusCode = HttpStatus.BAD_REQUEST
    val testUri = URI.create("/api/v1/test")
    val version = "1.2.3"
    val request =
      mockk<HttpRequest<*>> {
        every { uri } returns testUri
      }
    val response =
      mockk<MutableHttpResponse<*>> {
        every { status } returns statusCode
      }
    val continuation =
      mockk<FilterContinuation<MutableHttpResponse<*>>> {
        every { proceed() } returns response
      }
    val airbyteVersion =
      mockk<AirbyteVersion> {
        every { serialize() } returns version
      }

    val apiMetricFilter = ApiMetricFilter(metricClient = metricClient, airbyteVersion = airbyteVersion)

    apiMetricFilter.countApiMetrics(request = request, continuation = continuation)

    verify(exactly = 1) { continuation.proceed() }
    verify(exactly = 1) {
      metricClient.count(
        metric = OssMetricsRegistry.API_REQUESTS,
        value = 1L,
        attributes =
          arrayOf(
            MetricAttribute("endpoint", testUri.toString()),
            MetricAttribute("status", statusCode.code.toString()),
            MetricAttribute("version", version),
          ),
      )
    }
  }
}
