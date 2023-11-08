/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.launcher.pipeline.stages

import io.airbyte.workload.api.client2.generated.WorkloadApi
import io.airbyte.workload.api.client2.model.generated.ClaimResponse
import io.airbyte.workload.api.client2.model.generated.WorkloadClaimRequest
import io.airbyte.workload.launcher.pipeline.LaunchStage
import io.airbyte.workload.launcher.pipeline.LaunchStageIO
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Value
import jakarta.inject.Singleton

private val logger = KotlinLogging.logger {}

@Singleton
class ClaimStage(
  private val workloadApiClient: WorkloadApi,
  @Value("\${airbyte.data-plane-id}") private val dataplaneId: String,
) : LaunchStage {
  override fun applyStage(input: LaunchStageIO): LaunchStageIO {
    logger.info { "Stage: ${javaClass.simpleName}" }
    val resp: ClaimResponse =
      workloadApiClient.workloadClaim(
        WorkloadClaimRequest(
          input.msg.workloadId,
          dataplaneId,
        ),
      )

    return input.apply {
      skip = !resp.claimed
    }
  }

  override fun getStageName(): StageName {
    return StageName.CLAIM
  }
}
