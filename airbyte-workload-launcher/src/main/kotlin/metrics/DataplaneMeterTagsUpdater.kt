/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.metrics

import io.airbyte.config.WorkloadConstants.Companion.PUBLIC_ORG_ID
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workload.launcher.model.DataplaneConfig
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.config.MeterFilter
import io.micronaut.context.event.ApplicationEventListener
import jakarta.inject.Singleton
import java.util.UUID

@Singleton
@io.micronaut.configuration.metrics.annotation.RequiresMetrics
class DataplaneMeterTagsUpdater(
  private val registry: MeterRegistry,
) : ApplicationEventListener<DataplaneConfig> {
  override fun onApplicationEvent(event: DataplaneConfig) {
    registry
      .config()
      .meterFilter(MeterFilter.replaceTagValues(MetricTags.DATA_PLANE_ID_TAG, { event.dataplaneId.toString() }))
      .meterFilter(MeterFilter.replaceTagValues(MetricTags.DATA_PLANE_NAME_TAG, { event.dataplaneName }))
      .meterFilter(MeterFilter.replaceTagValues(MetricTags.DATA_PLANE_GROUP_TAG, { event.dataplaneGroupId.toString() }))
      .meterFilter(MeterFilter.replaceTagValues(MetricTags.DATA_PLANE_GROUP_NAME_TAG, { event.dataplaneGroupName }))
      .meterFilter(MeterFilter.replaceTagValues(MetricTags.DATA_PLANE_VISIBILITY, { getDataplaneVisibility(event.organizationId) }))
      .meterFilter(MeterFilter.replaceTagValues(MetricTags.DATA_PLANE_ORG_ID, { event.organizationId?.toString() ?: MetricTags.UNKNOWN }))
  }

  private fun getDataplaneVisibility(dataplaneGroupId: UUID?): String =
    when (dataplaneGroupId) {
      null -> MetricTags.UNKNOWN
      PUBLIC_ORG_ID -> MetricTags.PUBLIC
      else -> MetricTags.PRIVATE
    }
}
