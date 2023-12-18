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
  @Value("\${micronaut.application.name}") private val applicationName: String,
) {
  @Bean
  @Singleton
  fun addCommonTagFilter(): MeterFilter {
    // TODO add all the common tags

    val commonTags =
      mutableListOf(
        Tag.of(DATA_PLANE_ID_TAG, dataplaneId),
        Tag.of(DATA_DOG_SERVICE_TAG, applicationName),
      )

    if (!System.getenv(DATA_DOG_ENVIRONMENT_TAG).isNullOrBlank()) {
      commonTags.add(Tag.of(DATA_DOG_ENVIRONMENT_TAG, System.getenv(DATA_DOG_ENVIRONMENT_TAG)))
    }

    if (!System.getenv(DATA_DOG_AGENT_HOST_TAG).isNullOrBlank()) {
      commonTags.add(Tag.of(DATA_DOG_AGENT_HOST_TAG, System.getenv(DATA_DOG_AGENT_HOST_TAG)))
    }

    if (!System.getenv(DATA_DOG_VERSION_TAG).isNullOrBlank()) {
      commonTags.add(Tag.of(DATA_DOG_VERSION_TAG, System.getenv(DATA_DOG_VERSION_TAG)))
    }
    return MeterFilter.commonTags(commonTags)
  }

  companion object {
    const val DATA_DOG_AGENT_HOST_TAG = "DD_AGENT_HOST"
    const val DATA_DOG_ENVIRONMENT_TAG = "DD_ENV"
    const val DATA_DOG_SERVICE_TAG = "DD_SERVICE"
    const val DATA_DOG_VERSION_TAG = "DD_VERSION"
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
