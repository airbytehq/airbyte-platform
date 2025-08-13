/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody
import io.airbyte.api.client.model.generated.JobIdRequestBody
import io.airbyte.commons.micronaut.EnvConstants
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.EnableDataObservability
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Workspace
import io.micronaut.context.annotation.Requires
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import jakarta.inject.Singleton
import java.util.UUID

@JsonDeserialize(builder = EvaluateOutlierInput.Builder::class)
data class EvaluateOutlierInput(
  val jobId: Long,
  val connectionId: UUID?,
) {
  class Builder
    @JvmOverloads
    constructor(
      var jobId: Long? = null,
      var connectionId: UUID? = null,
    ) {
      fun jobId(jobId: Long) = apply { this.jobId = jobId }

      fun connectionId(connectionId: UUID) = apply { this.connectionId = connectionId }

      fun build(): EvaluateOutlierInput =
        EvaluateOutlierInput(
          jobId = jobId ?: throw IllegalArgumentException("jobId must be specified."),
          connectionId = connectionId,
        )
    }
}

@JsonDeserialize(builder = FinalizeJobStatsInput.Builder::class)
data class FinalizeJobStatsInput(
  val jobId: Long,
  val connectionId: UUID,
) {
  class Builder
    @JvmOverloads
    constructor(
      var jobId: Long? = null,
      var connectionId: UUID? = null,
    ) {
      fun jobId(jobId: Long) = apply { this.jobId = jobId }

      fun connectionId(connectionId: UUID) = apply { this.connectionId = connectionId }

      fun build(): FinalizeJobStatsInput =
        FinalizeJobStatsInput(
          jobId = jobId ?: throw IllegalArgumentException("jobId must be specified."),
          connectionId = connectionId ?: throw IllegalArgumentException("connectionId must be specified."),
        )
    }
}

@ActivityInterface
interface JobPostProcessingActivity {
  @ActivityMethod
  fun evaluateOutlier(input: EvaluateOutlierInput)

  @ActivityMethod
  fun finalizeJobStats(input: FinalizeJobStatsInput)
}

@Singleton
@Requires(env = [EnvConstants.CONTROL_PLANE])
class JobProcessingActivityImpl(
  private val airbyteApiClient: AirbyteApiClient,
  private val featureFlagClient: FeatureFlagClient,
) : JobPostProcessingActivity {
  override fun evaluateOutlier(input: EvaluateOutlierInput) {
    // TODO input.connectionId shouldn't be nullable, this is just for a smooth transition.
    val dataObservabilityEnabled = input.connectionId?.let { isOutlierDetectionEnabled(input.connectionId) } ?: false
    if (!dataObservabilityEnabled) {
      return
    }

    airbyteApiClient.jobsApi.evaluateOutlier(JobIdRequestBody(input.jobId))
  }

  override fun finalizeJobStats(input: FinalizeJobStatsInput) {
    val dataObservabilityEnabled = isOutlierDetectionEnabled(input.connectionId)
    if (!dataObservabilityEnabled) {
      return
    }

    airbyteApiClient.jobsApi.finalizeJob(JobIdRequestBody(input.jobId))
  }

  private fun isOutlierDetectionEnabled(connectionId: UUID): Boolean {
    val workspace = airbyteApiClient.workspaceApi.getWorkspaceByConnectionId(ConnectionIdRequestBody(connectionId = connectionId))
    val context = Multi(listOf(Connection(connectionId), Workspace(workspace.workspaceId)))
    return featureFlagClient.boolVariation(EnableDataObservability, context)
  }
}
