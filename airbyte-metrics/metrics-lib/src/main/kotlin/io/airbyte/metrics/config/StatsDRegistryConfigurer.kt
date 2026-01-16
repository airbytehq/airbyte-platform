/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.metrics.config

import io.airbyte.commons.version.AirbyteVersion
import io.micrometer.statsd.StatsdMeterRegistry
import io.micronaut.context.annotation.Requires
import io.micronaut.core.annotation.Order
import io.micronaut.core.order.Ordered
import io.micronaut.runtime.ApplicationConfiguration
import jakarta.inject.Named
import jakarta.inject.Singleton

const val DATA_DOG_SERVICE_TAG = "DD_SERVICE"
const val DATA_DOG_VERSION_TAG = "DD_VERSION"

/**
 * Custom Micronaut {@link MeterRegistryConfigurer} used to ensure that a common set of tags are
 * added to every Micrometer registry. Specifically, this class ensures that the tags for the
 * DataDog environment, service name and deployment version are added to the metrics produced by
 * Micrometer, if those values are in the current environment.
 */
@Order(Int.MAX_VALUE)
@Singleton
@Named("statsDRegistryConfigurer")
@io.micronaut.configuration.metrics.annotation.RequiresMetrics
@Requires(property = "micronaut.metrics.export.statsd.enabled", value = "true", defaultValue = "false")
class StatsDRegistryConfigurer(
  private val airbyteVersion: AirbyteVersion,
  private val applicationConfiguration: ApplicationConfiguration,
) : io.micronaut.configuration.metrics.aggregator.MeterRegistryConfigurer<StatsdMeterRegistry>,
  Ordered {
  override fun configure(meterRegistry: StatsdMeterRegistry?) {
    val serviceName = System.getenv().getOrDefault(DATA_DOG_SERVICE_TAG, applicationConfiguration.name.get())
    val version = System.getenv().getOrDefault(DATA_DOG_VERSION_TAG, airbyteVersion.serialize())

    meterRegistry?.config()?.commonTags(
      SERVICE_TAG,
      serviceName,
      VERSION_TAG,
      version,
    )
  }

  override fun getType(): Class<StatsdMeterRegistry> = StatsdMeterRegistry::class.java
}
