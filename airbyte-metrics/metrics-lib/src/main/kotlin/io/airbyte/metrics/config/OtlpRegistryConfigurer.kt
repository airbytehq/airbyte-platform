/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.config

import io.airbyte.commons.version.AirbyteVersion
import io.micrometer.registry.otlp.OtlpMeterRegistry
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Order
import io.micronaut.core.order.Ordered
import io.micronaut.runtime.ApplicationConfiguration
import io.opentelemetry.semconv.ServiceAttributes
import jakarta.inject.Named
import jakarta.inject.Singleton

const val OTEL_SERVICE_NAME_ENV_VAR = "OTEL_SERVICE_NAME"

@Order(Int.MAX_VALUE)
@Singleton
@Named("otlpRegistryConfigurer")
@io.micronaut.configuration.metrics.annotation.RequiresMetrics
@Requires(property = "micronaut.metrics.export.otlp.enabled", value = "true", defaultValue = "false")
class OtlpRegistryConfigurer(
  private val airbyteVersion: AirbyteVersion,
  private val applicationConfiguration: ApplicationConfiguration,
) : io.micronaut.configuration.metrics.aggregator.MeterRegistryConfigurer<OtlpMeterRegistry>,
  Ordered {
  override fun configure(meterRegistry: OtlpMeterRegistry?) {
    val serviceName = System.getenv().getOrDefault(OTEL_SERVICE_NAME_ENV_VAR, applicationConfiguration.name.get())
    val version = airbyteVersion.serialize()

    meterRegistry?.config()?.commonTags(
      ServiceAttributes.SERVICE_NAME.key,
      serviceName,
      SERVICE_TAG,
      serviceName,
      ServiceAttributes.SERVICE_VERSION.key,
      version,
      VERSION_TAG,
      version,
    )
  }

  override fun getType(): Class<OtlpMeterRegistry> = OtlpMeterRegistry::class.java
}
