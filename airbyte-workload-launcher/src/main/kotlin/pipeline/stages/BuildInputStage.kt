/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import io.airbyte.config.WorkloadType
import io.airbyte.featureflag.Context
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.annotations.Instrument
import io.airbyte.metrics.annotations.Tag
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.workers.input.InputFeatureFlagContextMapper
import io.airbyte.workers.input.ReplicationInputMapper
import io.airbyte.workers.models.CheckConnectionInput
import io.airbyte.workers.models.DiscoverCatalogInput
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.models.SpecInput
import io.airbyte.workers.serde.PayloadDeserializer
import io.airbyte.workload.launcher.metrics.MeterFilterFactory
import io.airbyte.workload.launcher.pipeline.stages.model.CheckPayload
import io.airbyte.workload.launcher.pipeline.stages.model.DiscoverCatalogPayload
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.pipeline.stages.model.SpecPayload
import io.airbyte.workload.launcher.pipeline.stages.model.SyncPayload
import io.airbyte.workload.launcher.pipeline.stages.model.WorkloadPayload
import jakarta.inject.Named
import jakarta.inject.Singleton
import reactor.core.publisher.Mono

/**
 * Deserializes input payloads, derives a feature flag context from input data and attaches both to the IO.
 * The input is usually not fully hydrated with further hydration eventually performed by the launched pod itself.
 */
@Singleton
@Named("build")
open class BuildInputStage(
  private val replicationInputMapper: ReplicationInputMapper,
  private val deserializer: PayloadDeserializer,
  metricClient: MetricClient,
  private val ffCtxMapper: InputFeatureFlagContextMapper,
) : LaunchStage(metricClient) {
  @Trace(operationName = MeterFilterFactory.LAUNCH_PIPELINE_STAGE_OPERATION_NAME, resourceName = "BuildInputStage")
  @Instrument(
    start = "WORKLOAD_STAGE_START",
    end = "WORKLOAD_STAGE_DONE",
    tags = [Tag(key = MetricTags.STAGE_NAME_TAG, value = "build")],
  )
  override fun apply(input: LaunchStageIO): Mono<LaunchStageIO> = super.apply(input)

  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    val built = buildPayload(input.msg.workloadInput, input.msg.workloadType)
    val ffCtx = buildInputContext(built)

    return input.apply {
      payload = built
      ffContext = ffCtx
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

  private fun buildInputContext(payload: WorkloadPayload): Context =
    when (payload) {
      is CheckPayload -> ffCtxMapper.map(payload.input)
      is DiscoverCatalogPayload -> ffCtxMapper.map(payload.input)
      is SpecPayload -> ffCtxMapper.map(payload.input)
      is SyncPayload -> ffCtxMapper.map(payload.input)
    }
}
