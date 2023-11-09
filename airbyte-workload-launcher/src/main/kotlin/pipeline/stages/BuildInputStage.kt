package io.airbyte.workload.launcher.pipeline.stages

import datadog.trace.api.Trace
import io.airbyte.config.secrets.hydration.SecretsHydrator
import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workload.launcher.metrics.MeterFilterFactory.Companion.LAUNCH_PIPELINE_STAGE_OPERATION_NAME
import io.airbyte.workload.launcher.pipeline.LaunchStage
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.airbyte.workload.launcher.serde.PayloadDeserializer
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class BuildInputStage(
  private val secretsHydrator: SecretsHydrator,
  private val deserializer: PayloadDeserializer,
) : LaunchStage {
  @Trace(operationName = LAUNCH_PIPELINE_STAGE_OPERATION_NAME)
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    logger.info { "Stage: ${javaClass.simpleName}" }

    val parsed: ReplicationInput = deserializer.toReplicationInput(input.msg.workloadInput)

    parsed.apply {
      sourceConfiguration = secretsHydrator.hydrate(parsed.sourceConfiguration)
      destinationConfiguration = secretsHydrator.hydrate(parsed.destinationConfiguration)
    }

    return input.apply {
      replicationInput = parsed
    }
  }

  override fun getStageName(): StageName {
    return StageName.BUILD
  }
}
