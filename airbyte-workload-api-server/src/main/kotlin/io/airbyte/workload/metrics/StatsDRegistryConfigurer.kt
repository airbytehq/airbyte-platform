/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.metrics

import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.statsd.StatsdMeterRegistry
import io.micronaut.configuration.metrics.aggregator.MeterRegistryConfigurer
import io.micronaut.configuration.metrics.annotation.RequiresMetrics
import io.micronaut.context.annotation.Replaces
import io.micronaut.core.annotation.Order
import io.micronaut.core.order.Ordered
import io.micronaut.core.util.StringUtils
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

/**
 * Custom Micronaut {@link MeterRegistryConfigurer} used to ensure that a common set of tags are
 * added to every Micrometer registry. Specifically, this class ensures that the tags for the
 * DataDog environment, service name and deployment version are added to the metrics produced by
 * Micrometer, if those values are in the current environment.
 */
@Order(Int.MAX_VALUE)
@Singleton
@RequiresMetrics
@Replaces(named = "statsDRegistryConfigurer")
class StatsDRegistryConfigurer : MeterRegistryConfigurer<StatsdMeterRegistry>, Ordered {
  override fun configure(meterRegistry: StatsdMeterRegistry?) {
        /*
         * Use a LinkedHashSet to maintain order as items are added to the set. This ensures that the items
         * are output as key1, value1, key2, value2, etc in order to maintain the relationship between key
         * value pairs.
         */
    val tags: MutableSet<String> = LinkedHashSet()

    possiblyAddTag(DATA_DOG_SERVICE_TAG, "service", tags)
    possiblyAddTag(DATA_DOG_VERSION_TAG, "version", tags)

    logger.debug { "Adding common tags to the StatsD Micrometer meter registry configuration: $tags" }

    meterRegistry!!.config().commonTags(*tags.toTypedArray())
  }

  override fun getType(): Class<StatsdMeterRegistry> {
    return StatsdMeterRegistry::class.java
  }

  /**
   * Safely adds the value associated with the provided environment variable if it exists and is not
   * blank.
   *
   * @param envVar The name of the environment variable.
   * @param tagName The name of the tag to add to the set if the environment variable is not blank.
   * @param tags The set of tags.
   */
  private fun possiblyAddTag(
    envVar: String,
    tagName: String,
    tags: MutableSet<String>,
  ) {
    val envVarValue = System.getenv(envVar)
    if (StringUtils.isNotEmpty(envVarValue)) {
      tags.add(tagName)
      tags.add(envVarValue)
    }
  }

  companion object {
    const val DATA_DOG_SERVICE_TAG = "DD_SERVICE"
    const val DATA_DOG_VERSION_TAG = "DD_VERSION"
    const val DATA_PLANE_ID_TAG = "data_plane_id"
    const val GEOGRAPHY_TAG = "geography"
    const val MUTEX_KEY_TAG = "mutex_key"
    const val QUEUE_NAME_TAG = "queue_name"
    const val WORKLOAD_ID_TAG = "workload_id"
    const val WORKLOAD_TYPE_TAG = "workload_type"
    const val WORKLOAD_CANCEL_REASON_TAG = "cancel_reason"
    const val WORKLOAD_CANCEL_SOURCE_TAG = "cancel_source"
    const val WORKLOAD_PUBLISHER_OPERATION_NAME = "workload_publisher"
  }
}
