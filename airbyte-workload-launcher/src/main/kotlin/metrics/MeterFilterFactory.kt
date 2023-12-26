package io.airbyte.workload.launcher.metrics

import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.config.MeterFilter
import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

@Factory
class MeterFilterFactory(
  @Value("\${airbyte.data-plane-id}") private val dataplaneId: String,
) {
  @Bean
  @Singleton
  fun addCommonTagFilter(): MeterFilter {
    // Add all the common application-specific tags
    val commonTags =
      mutableListOf(
        Tag.of(DATA_PLANE_ID_TAG, dataplaneId),
      )

    return MeterFilter.commonTags(commonTags)
  }

  companion object {
    const val DATA_PLANE_ID_TAG = "data_plane_id"
    const val STAGE_NAME_TAG = "stage_name"
    const val STATUS_TAG = "status"
    const val WORKLOAD_ID_TAG = "workload_id"
    const val MUTEX_KEY_TAG = "mutex_key"

    const val LAUNCH_PIPELINE_OPERATION_NAME = "launch-pipeline"
    const val LAUNCH_PIPELINE_STAGE_OPERATION_NAME = "launch-pipeline-stage"
    const val RESUME_CLAIMED_OPERATION_NAME = "resume_claimed"
    const val KUBERNETES_RESOURCE_MONITOR_NAME = "kubernetes-resource-monitor"
    const val SUCCESS_STATUS = "ok"
    const val FAILURE_STATUS = "error"
    const val RUNNING_STATUS = "running"
    const val STOPPED_STATUS = "stopped"
  }
}
