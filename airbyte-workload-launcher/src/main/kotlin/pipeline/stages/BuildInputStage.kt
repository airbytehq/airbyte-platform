/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import io.airbyte.config.WorkloadType
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.workers.input.ReplicationInputMapper
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workload.launcher.metrics.CustomMetricPublisher
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.pipeline.stages.model.CheckPayload
import io.airbyte.workload.launcher.pipeline.stages.model.DiscoverCatalogPayload
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SpecPayload
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.airbyte.workload.launcher.pipeline.stages.model.WorkloadPayload
import io.micronaut.context.annotation.Value
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono

/**
 * Deserializes input payloads and performs any necessary hydration from other
 * sources. When complete, a fully formed workload input should be attached to
 * the IO.
 */
@Singleton
@Named("build")
open class BuildInputStage(
  private val replicationInputMapper: ReplicationInputMapper,
  private val deserializer: PayloadDeserializer,
  metricPublisher: CustomMetricPublisher,
  @Value("\${airbyte.data-plane-id}") dataplaneId: String,
) : LaunchStage(metricPublisher, dataplaneId) {
  @Trace(operationName = MeterFilterFactory.LAUNCH_PIPELINE_STAGE_OPERATION_NAME, resourceName = "BuildInputStage")
  @Instrument(
    start = "WORKLOAD_STAGE_START",
    end = "WORKLOAD_STAGE_DONE",
    tags = [Tag(key = MeterFilterFactory.STAGE_NAME_TAG, value = "build")],
  )
  override fun apply(input: LaunchStageIO): Mono<LaunchStageIO> = super.apply(input)

  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    val built = buildPayload(input.msg.workloadInput, input.msg.workloadType)

    return input.apply {
      payload = built
    }
  }

  override fun getStageName(): StageName = StageName.BUILD

  private fun buildPayload(
    rawPayload: String,
    type: WorkloadType,
  ): WorkloadPayload =
    when (type) {
      WorkloadType.CHECK -> {
        val parsed: CheckConnectionInput = deserializer.toCheckConnectionInput(rawPayload)
        CheckPayload(parsed)
      }

      WorkloadType.DISCOVER -> {
        val parsed: DiscoverCatalogInput = deserializer.toDiscoverCatalogInput(rawPayload)
        DiscoverCatalogPayload(parsed)
      }

      WorkloadType.SYNC -> {
        val parsed: ReplicationActivityInput = deserializer.toReplicationActivityInput(rawPayload)
        SyncPayload(replicationInputMapper.toReplicationInput(parsed))
      }

      WorkloadType.SPEC -> {
        val parsed: SpecInput = deserializer.toSpecInput(rawPayload)
        SpecPayload(parsed)
      }

      else -> {
        throw NotImplementedError("Unimplemented workload type: $type")
      }
    }
}
