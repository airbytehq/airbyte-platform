package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workers.ReplicationInputHydrator
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_PIPELINE_STAGE_OPERATION_NAME
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStage
import io.airbyte.workload.launcher.pipeline.stages.model.LaunchStageIO
import io.airbyte.workload.launcher.serde.PayloadDeserializer
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
@Named("build")
class BuildInputStage(
  private val replicationInputHydrator: ReplicationInputHydrator,
  private val deserializer: PayloadDeserializer,
) : LaunchStage {
  @Trace(operationName = LAUNCH_PIPELINE_STAGE_OPERATION_NAME)
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    val parsed: ReplicationActivityInput = deserializer.toReplicationActivityInput(input.msg.workloadInput)
    val hydrated: ReplicationInput = replicationInputHydrator.getHydratedReplicationInput(parsed)

    return input.apply {
      replicationInput = hydrated
    }
  }

  override fun getStageName(): StageName {
    return StageName.BUILD
  }
}
