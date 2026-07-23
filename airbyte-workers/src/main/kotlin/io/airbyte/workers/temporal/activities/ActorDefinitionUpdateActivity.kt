/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.activities

import io.airbyte.api.client.AirbyteApiClient
import io.airbyte.api.client.model.generated.ActorDefinitionFinishRequest
import io.airbyte.api.client.model.generated.ActorDefinitionMetadata
import io.airbyte.api.client.model.generated.ActorUpdateRequest
import io.airbyte.commons.temporal.scheduling.SpecMetadata
import io.airbyte.commons.temporal.scheduling.SpecRequest
import io.airbyte.config.ActorType
import io.airbyte.config.FailureReason
import io.airbyte.config.JobTypeResourceLimit
import io.airbyte.config.ResourceRequirements
import io.airbyte.config.ScopedResourceRequirements
import io.airbyte.workers.commands.FailureConverter
import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import jakarta.inject.Singleton
import java.util.UUID
import io.airbyte.api.client.model.generated.ActorType as ApiActorType
import io.airbyte.api.client.model.generated.FailureReason as ApiFailureReason
import io.airbyte.api.client.model.generated.JobType as ApiJobType
import io.airbyte.api.client.model.generated.JobTypeResourceLimit as ApiJobTypeResourceLimit
import io.airbyte.api.client.model.generated.ResourceRequirements as ApiResourceRequirements
import io.airbyte.api.client.model.generated.ScopedResourceRequirements as ApiScopedResourceRequirements

data class FinishInput(
  val requestId: String,
  val commandId: String?,
  val actorDefinitionId: UUID?,
  val actorType: ActorType,
  val specRequest: SpecRequest,
  val specMetadata: SpecMetadata,
)

data class FinishOutput(
  val requestId: String,
  val actorDefinitionId: UUID?,
  val failureReason: FailureReason?,
)

@ActivityInterface
interface ActorDefinitionUpdateActivity {
  @ActivityMethod
  fun finish(input: FinishInput): FinishOutput
}

@Singleton
class ActorDefinitionUpdateActivityImpl(
  val airbyteApiClient: AirbyteApiClient,
  val failureConverter: FailureConverter,
) : ActorDefinitionUpdateActivity {
  override fun finish(input: FinishInput): FinishOutput {
    val request =
      ActorDefinitionFinishRequest(
        actorUpdateRequest =
          ActorUpdateRequest(
            actorType = input.actorType.toApi(),
            actorDefinitionId = input.actorDefinitionId,
            imageName = input.specRequest.dockerImage,
            imageTag = input.specRequest.dockerImageTag,
          ),
        metadata =
          ActorDefinitionMetadata(
            name = input.specMetadata.name,
            icon = input.specMetadata.icon,
            documentationUrl = input.specMetadata.documentationUrl,
            resourceRequirements = input.specMetadata.resourceRequirements?.toApi(),
          ),
        commandId = input.commandId,
        workspaceId = input.specRequest.workspaceId,
      )
    val response = airbyteApiClient.actorDefinitionApi.finishActorDefinitionUpdate(request)
    return FinishOutput(
      requestId = input.requestId,
      actorDefinitionId = response.actorDefinitionId,
      failureReason = response.failureReason?.toInternal(),
    )
  }

  private fun ApiFailureReason.toInternal(): FailureReason =
    FailureReason()
      .withFailureType(failureConverter.getFailureType(this.failureType))
      .withFailureOrigin(failureConverter.getFailureOrigin(this.failureOrigin))
      .withInternalMessage(this.internalMessage)
      .withExternalMessage(this.externalMessage)
      .withStacktrace(this.stacktrace)
      .withRetryable(this.retryable)
      .withTimestamp(this.timestamp)

  private fun ActorType.toApi(): ApiActorType =
    when (this) {
      ActorType.SOURCE -> ApiActorType.SOURCE
      ActorType.DESTINATION -> ApiActorType.DESTINATION
    }

  private fun ScopedResourceRequirements.toApi(): ApiScopedResourceRequirements =
    ApiScopedResourceRequirements(
      default = default?.toApi(),
      jobSpecific = jobSpecific?.map { it.toApi() },
    )

  private fun ResourceRequirements.toApi(): ApiResourceRequirements =
    ApiResourceRequirements(
      cpuRequest = cpuRequest,
      cpuLimit = cpuLimit,
      memoryRequest = memoryRequest,
      memoryLimit = memoryLimit,
    )

  private fun JobTypeResourceLimit.toApi(): ApiJobTypeResourceLimit =
    ApiJobTypeResourceLimit(
      jobType = ApiJobType.valueOf(jobType.name),
      resourceRequirements = resourceRequirements.toApi(),
    )
}
