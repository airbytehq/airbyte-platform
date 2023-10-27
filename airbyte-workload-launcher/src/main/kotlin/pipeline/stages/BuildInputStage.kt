package io.airbyte.workload.launcher.pipeline.stages

import io.airbyte.persistence.job.models.ReplicationInput
import io.airbyte.workload.launcher.pipeline.LaunchStage
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.airbyte.workload.launcher.serde.PayloadDeserializer
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class BuildInputStage(
//  private val secretsHydrator: SecretsHydrator,
  private val deserializer: PayloadDeserializer,
) : LaunchStage {
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    logger.info { "Stage: ${javaClass.simpleName}" }

    val parsed: ReplicationInput = deserializer.toReplicationInput(input.msg.workloadInput)

//    parsed.apply {
//      sourceConfiguration = secretsHydrator.hydrate(parsed.sourceConfiguration)
//      destinationConfiguration = secretsHydrator.hydrate(parsed.destinationConfiguration)
//    }

    return input.apply {
      replicationInput = parsed
    }
  }

  override fun getStageName(): StageName {
    return StageName.BUILD
  }
}
