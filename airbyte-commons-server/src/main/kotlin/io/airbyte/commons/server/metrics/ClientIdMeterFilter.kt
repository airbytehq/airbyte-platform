/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.metrics

import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.config.MeterFilter
import io.micronaut.configuration.metrics.annotation.RequiresMetrics
import io.micronaut.core.annotation.Order
import io.micronaut.core.order.Ordered
import io.micronaut.http.context.ServerRequestContext
import jakarta.inject.Singleton

private const val CLIENT_ID_HEADER = "X-Airbyte-Client-ID"
private const val SONAR_CLIENT_ID_PREFIX = "sonar-"

// Match on the renamed metric name (see AirbyteMetricMeterFilter).
private val HTTP_REQUEST_METRIC_NAME = OssMetricsRegistry.API_REQUESTS.getMetricName()

/**
 * Adds a [MetricTags.CLIENT_ID] tag to the HTTP server request metric
 * by extracting the X-Airbyte-Client-ID header from the current request.
 *
 * This allows us to scope API metrics to specific clients (e.g., ADP/Sonar).
 *
 * Note on order: we ensure this runs after AirbyteMetricMeterFilter renames the metric.
 */
@Order(Ordered.LOWEST_PRECEDENCE)
@Singleton
@RequiresMetrics
class ClientIdMeterFilter : MeterFilter {
  override fun map(id: Meter.Id): Meter.Id {
    if (id.name != HTTP_REQUEST_METRIC_NAME) {
      return id
    }

    return ServerRequestContext
      .currentRequest<Any>()
      .map { request ->
        val clientId = request.headers.get(CLIENT_ID_HEADER)
        // Only add sonar client IDs for now to prevent cardinality explosion.
        if (clientId != null && clientId.startsWith(SONAR_CLIENT_ID_PREFIX)) {
          id.withTags(id.tags + Tag.of(MetricTags.CLIENT_ID, clientId))
        } else {
          id
        }
      }.orElse(id)
  }
}
