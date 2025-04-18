/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.metrics

import io.airbyte.metrics.lib.MetricTags
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.config.MeterFilter
import io.micronaut.context.annotation.Factory
import jakarta.inject.Named
import jakarta.inject.Singleton

@Factory
class MeterFilterFactory {
  @Singleton
  @Named("dataplaneMeterFilter")
  @io.micronaut.configuration.metrics.annotation.RequiresMetrics
  fun addCommonTagFilter(): MeterFilter =
    MeterFilter.commonTags(
      mutableListOf(
        Tag.of(MetricTags.DATA_PLANE_ID_TAG, UNDEFINED),
        Tag.of(MetricTags.DATA_PLANE_NAME_TAG, UNDEFINED),
        Tag.of(MetricTags.DATA_PLANE_GROUP_TAG, UNDEFINED),
        Tag.of(MetricTags.DATA_PLANE_GROUP_NAME_TAG, UNDEFINED),
      ),
    )

  companion object {
    const val LAUNCH_PIPELINE_OPERATION_NAME = "launch-pipeline"
    const val LAUNCH_PIPELINE_STAGE_OPERATION_NAME = "launch-pipeline-stage"
    const val LAUNCH_REPLICATION_OPERATION_NAME = "launch-replication"
    const val LAUNCH_RESET_OPERATION_NAME = "launch-reset"
    const val RESUME_CLAIMED_OPERATION_NAME = "resume_claimed"
    const val SUCCESS_STATUS = "ok"
    const val FAILURE_STATUS = "error"
    const val RUNNING_STATUS = "running"
    const val STOPPED_STATUS = "stopped"
    const val UNDEFINED = "undefined"
  }
}
