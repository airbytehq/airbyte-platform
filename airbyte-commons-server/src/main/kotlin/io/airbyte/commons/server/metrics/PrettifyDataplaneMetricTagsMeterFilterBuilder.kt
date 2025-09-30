/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.metrics

import io.airbyte.config.WorkloadConstants.Companion.PUBLIC_ORG_ID
import io.airbyte.metrics.lib.MetricTags
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.config.MeterFilter
import io.micronaut.configuration.metrics.annotation.RequiresMetrics
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.runtime.event.ApplicationStartupEvent
import jakarta.inject.Singleton
import java.util.UUID
import javax.annotation.PostConstruct

/**
 * This singleton automatically builds the [PrettifyDataplaneMetricTagsMeterFilter] and registers it with the [MeterRegistry].
 */
class PrettifyDataplaneMetricTagsMeterFilterBuilder(
  private val cache: MetricTagsPrettifierCache,
  private val meterRegistry: MeterRegistry? = null,
) : ApplicationEventListener<ApplicationStartupEvent> {
  /**
   * This Filter will automatically add the names of dataplane and dataplane group if their ids are present in the tags.
   */
  class PrettifyDataplaneMetricTagsMeterFilter(
    private val cache: MetricTagsPrettifierCache,
  ) : MeterFilter {
    override fun map(id: Meter.Id): Meter.Id {
      val newTags = mutableListOf<Tag>()
      id.getTag(MetricTags.DATA_PLANE_ID_TAG)?.asUuid()?.let { dataplaneId ->
        if (id.getTag(MetricTags.DATA_PLANE_NAME_TAG) == null) {
          newTags.add(Tag.of(MetricTags.DATA_PLANE_NAME_TAG, cache.dataplaneNameById(dataplaneId)))
        }
      }
      id.getTag(MetricTags.DATA_PLANE_GROUP_TAG)?.asUuid()?.let { dataplaneGroupId ->
        if (id.getTag(MetricTags.DATA_PLANE_GROUP_NAME_TAG) == null) {
          newTags.add(Tag.of(MetricTags.DATA_PLANE_GROUP_NAME_TAG, cache.dataplaneGroupNameById(dataplaneGroupId)))
        }
        cache.orgIdForDataplaneGroupId(dataplaneGroupId)?.let { orgId ->
          newTags.add(Tag.of(MetricTags.DATA_PLANE_VISIBILITY, getDataplaneVisibility(orgId)))
        }
      }
      return if (newTags.isNotEmpty()) id.withTags(newTags) else id
    }

    fun getDataplaneVisibility(dataplaneGroupId: UUID): String = if (dataplaneGroupId == PUBLIC_ORG_ID) MetricTags.PUBLIC else MetricTags.PRIVATE
  }

  // Register the filter ASAP
  @PostConstruct
  fun registerFilter() {
    meterRegistry?.config()?.meterFilter(PrettifyDataplaneMetricTagsMeterFilter(cache))
  }

  // Using an ApplicationEventListener to ensure the Filter is automatically built.
  // We cannot inherit MeterFilter directly here because it creates a dependency cycle.
  override fun onApplicationEvent(event: ApplicationStartupEvent) {}
}

// In some cases, a workload will have null dataplane ID / dataplane group ID.
// In those cases, we emit a metric tagged with `undefined` in place of the ID.
// So we have to parse the UUID defensively.
private fun String.asUuid(): UUID? =
  try {
    UUID.fromString(this)
  } catch (e: Exception) {
    null
  }
