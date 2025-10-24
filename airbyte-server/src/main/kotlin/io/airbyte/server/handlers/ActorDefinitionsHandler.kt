/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import io.airbyte.api.model.generated.ActorDefinitionCreateRequest
import io.airbyte.api.model.generated.ActorDefinitionCreateResponse
import io.airbyte.api.model.generated.ActorDefinitionMetadata
import io.airbyte.api.model.generated.ActorDefinitionResultResponse
import io.airbyte.api.model.generated.ActorDefinitionUpdateRequest
import io.airbyte.api.model.generated.ActorDefinitionUpdateResponse
import io.airbyte.api.model.generated.ActorUpdateRequest
import io.airbyte.api.model.generated.ScopedResourceRequirements
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.handlers.DestinationDefinitionsHandler
import io.airbyte.commons.server.handlers.SourceDefinitionsHandler
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper
import io.airbyte.commons.temporal.TemporalClient
import io.airbyte.commons.temporal.scheduling.ActorDefinitionUpdateInput
import io.airbyte.commons.temporal.scheduling.SpecMetadata
import io.airbyte.commons.temporal.scheduling.SpecRequest
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.ScopeType
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.domain.models.ActorDefinitionId
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.server.services.CommandService
import jakarta.inject.Singleton
import java.io.IOException
import java.time.Instant
import java.util.UUID
import io.airbyte.api.model.generated.FailureOrigin as ApiFailureOrigin
import io.airbyte.api.model.generated.FailureReason as ApiFailureReason
import io.airbyte.api.model.generated.FailureType as ApiFailureType

/**
 * Handler for unified actor definition operations (sources and destinations).
 */
@Singleton
class ActorDefinitionsHandler(
  private val sourceDefinitionsHandler: SourceDefinitionsHandler,
  private val destinationDefinitionsHandler: DestinationDefinitionsHandler,
  private val actorDefinitionHandlerHelper: ActorDefinitionHandlerHelper,
  private val actorDefinitionService: ActorDefinitionService,
  private val apiToPojoConverters: ApiPojoConverters,
  private val commandService: CommandService,
  private val temporalClient: TemporalClient,
) {
  /**
   * Creates an actor definition (source or destination) based on the actorType parameter.
   * Returns a request ID that can be used to track the async operation.
   *
   * @param actorDefinitionCreateRequest - the actor definition creation request
   * @return ActorDefinitionCreateResponse with requestId
   * @throws IOException if there is an error creating the definition
   */
  fun createActorDefinition(actorDefinitionCreateRequest: ActorDefinitionCreateRequest): ActorDefinitionCreateResponse {
    val requestId = UUID.randomUUID().toString()

    val input =
      ActorDefinitionUpdateInput(
        requestId = requestId,
        actorType = ActorType.fromValue(actorDefinitionCreateRequest.actorType.toString()),
        actorDefinitionId = null,
        specRequest =
          SpecRequest(
            shouldFetchSpec = true,
            dockerImage = actorDefinitionCreateRequest.actorDefinition.dockerRepository,
            dockerImageTag = actorDefinitionCreateRequest.actorDefinition.dockerImageTag,
            workspaceId = actorDefinitionCreateRequest.workspaceId,
          ),
        specMetadata =
          SpecMetadata(
            name = actorDefinitionCreateRequest.actorDefinition.name,
            icon = actorDefinitionCreateRequest.actorDefinition.icon,
            documentationUrl = actorDefinitionCreateRequest.actorDefinition.documentationUrl.toString(),
            resourceRequirements =
              apiToPojoConverters.scopedResourceReqsToInternal(
                actorDefinitionCreateRequest.actorDefinition.resourceRequirements,
              ),
          ),
      )
    temporalClient.submitActorDefinitionUpdateAsync(input)

    return ActorDefinitionCreateResponse().requestId(requestId)
  }

  /**
   * Updates an actor definition (source or destination) based on the actorType parameter.
   * Returns a request ID that can be used to track the async operation.
   *
   * @param actorDefinitionUpdateRequest - the actor definition update request
   * @return ActorDefinitionUpdateResponse with requestId
   * @throws IOException if there is an error updating the definition
   */
  fun updateActorDefinition(actorDefinitionUpdateRequest: ActorDefinitionUpdateRequest): ActorDefinitionUpdateResponse {
    actorDefinitionHandlerHelper.validateVersionSupport(
      actorDefinitionId = actorDefinitionUpdateRequest.actorDefinitionId,
      connectorVersion = actorDefinitionUpdateRequest.dockerImageTag,
      actorType = ActorType.fromValue(actorDefinitionUpdateRequest.actorType.toString()),
    )

    val requestId = UUID.randomUUID().toString()

    val actorDefinitionVersionId = getActorDefinitionVersionId(actorDefinitionUpdateRequest.actorDefinitionId)
    val currentVersion = actorDefinitionService.getActorDefinitionVersion(actorDefinitionVersionId)
    val actorType = ActorType.fromValue(actorDefinitionUpdateRequest.actorType.toString())

    val input =
      ActorDefinitionUpdateInput(
        requestId = requestId,
        actorType = actorType,
        actorDefinitionId = actorDefinitionUpdateRequest.actorDefinitionId,
        specRequest =
          SpecRequest(
            shouldFetchSpec =
              actorDefinitionHandlerHelper.shouldFetchSpec(
                currentVersion = currentVersion,
                connectorVersion = actorDefinitionUpdateRequest.dockerImageTag,
                actorType = actorType,
              ),
            dockerImage = currentVersion.dockerRepository,
            dockerImageTag = actorDefinitionUpdateRequest.dockerImageTag,
            workspaceId = actorDefinitionUpdateRequest.workspaceId,
          ),
        specMetadata =
          SpecMetadata(
            name = null,
            icon = null,
            documentationUrl = null,
            resourceRequirements = null,
          ),
      )
    temporalClient.submitActorDefinitionUpdateAsync(input)

    return ActorDefinitionUpdateResponse().requestId(requestId)
  }

  private fun getActorDefinitionVersionId(actorDefinitionId: UUID): UUID =
    actorDefinitionService
      .getDefaultVersionForActorDefinitionIdOptional(actorDefinitionId)
      .orElseThrow { IdNotFoundKnownException("Could not find configuration for actorDefinition", actorDefinitionId.toString()) }
      .versionId

  /**
   * Result of finishing an actor definition create or update operation.
   */
  data class ActorDefinitionFinishResult(
    val actorDefinitionId: UUID?,
    val failureReason: ApiFailureReason?,
  )

  /**
   * Result of retrieving spec from command, including potential failure.
   */
  private data class SpecResult(
    val spec: ConnectorSpecification?,
    val failureReason: ApiFailureReason?,
  )

  fun finishActorDefinitionUpdate(
    actorType: ActorType,
    actorUpdateRequest: ActorUpdateRequest,
    actorDefinitionMetadata: ActorDefinitionMetadata,
    commandId: String?,
    workspaceId: UUID,
  ): ActorDefinitionFinishResult {
    val specResult =
      if (commandId !=
        null
      ) {
        getSpecFromCommandSafely(commandId, actorUpdateRequest.imageName, actorUpdateRequest.imageTag)
      } else {
        SpecResult(null, null)
      }

    // If spec retrieval failed, return failure immediately
    if (specResult.failureReason != null) {
      return ActorDefinitionFinishResult(null, specResult.failureReason)
    }

    return try {
      val actorDefinitionId =
        if (actorUpdateRequest.actorDefinitionId == null) {
          finishCreate(
            actorType = actorType,
            spec = specResult.spec ?: throw IllegalStateException("Spec not found"),
            imageName = actorUpdateRequest.imageName,
            imageTag = actorUpdateRequest.imageTag,
            workspaceId = workspaceId,
            actorDefinitionMetadata = actorDefinitionMetadata,
          )
        } else {
          finishUpdate(
            actorType = actorType,
            actorDefinitionId = actorUpdateRequest.actorDefinitionId,
            spec = specResult.spec,
            imageTag = actorUpdateRequest.imageTag,
            actorDefinitionMetadata = actorDefinitionMetadata,
          )
        }
      ActorDefinitionFinishResult(actorDefinitionId, null)
    } catch (e: Exception) {
      ActorDefinitionFinishResult(
        null,
        ApiFailureReason()
          .failureOrigin(ApiFailureOrigin.AIRBYTE_PLATFORM)
          .failureType(ApiFailureType.SYSTEM_ERROR)
          .internalMessage(e.message ?: "Unknown error occurred during actor definition finish")
          .timestamp(Instant.now().toEpochMilli()),
      )
    }
  }

  private fun finishCreate(
    actorType: ActorType,
    spec: ConnectorSpecification,
    actorDefinitionMetadata: ActorDefinitionMetadata,
    imageName: String,
    imageTag: String,
    workspaceId: UUID,
  ): UUID {
    val actorDefinitionId = UUID.randomUUID()
    val adv =
      actorDefinitionHandlerHelper
        .defaultDefinitionVersionFromCreate(
          spec = spec,
          dockerRepository = imageName,
          dockerImageTag = imageTag,
          documentationUrl = actorDefinitionMetadata.documentationUrl,
        ).withActorDefinitionId(actorDefinitionId)

    return when (actorType) {
      ActorType.SOURCE ->
        sourceDefinitionsHandler
          .saveCustomSourceDefinition(
            name = actorDefinitionMetadata.name,
            icon = actorDefinitionMetadata.icon,
            actorDefinitionVersion = adv,
            resourceRequirements = actorDefinitionMetadata.resourceRequirements,
            scopeId = workspaceId,
            scopeType = ScopeType.WORKSPACE,
          ).sourceDefinitionId
      ActorType.DESTINATION ->
        destinationDefinitionsHandler
          .saveCustomDestinationDefinition(
            name = actorDefinitionMetadata.name,
            icon = actorDefinitionMetadata.icon,
            actorDefinitionVersion = adv,
            resourceRequirements = actorDefinitionMetadata.resourceRequirements,
            scopeId = workspaceId,
            scopeType = ScopeType.WORKSPACE,
          ).destinationDefinitionId
    }
  }

  private fun finishUpdate(
    actorType: ActorType,
    actorDefinitionId: UUID,
    spec: ConnectorSpecification?,
    imageTag: String,
    actorDefinitionMetadata: ActorDefinitionMetadata,
  ): UUID {
    val actorDefinitionVersionId = getActorDefinitionVersionId(actorDefinitionId)
    val newVersion =
      actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
        currentVersionId = actorDefinitionVersionId,
        spec = spec,
        actorType = actorType,
        newDockerImageTag = imageTag,
      )
    return when (actorType) {
      ActorType.SOURCE ->
        sourceDefinitionsHandler
          .updateSourceDefinition(
            newVersion = newVersion,
            name = actorDefinitionMetadata.name,
            resourceRequirements = actorDefinitionMetadata.resourceRequirements,
          ).sourceDefinitionId
      ActorType.DESTINATION ->
        destinationDefinitionsHandler
          .updateDestinationDefinition(
            newVersion = newVersion,
            name = actorDefinitionMetadata.name,
            resourceRequirements = actorDefinitionMetadata.resourceRequirements,
          ).destinationDefinitionId
    }
  }

  private fun getSpecFromCommand(
    commandId: String,
    imageName: String,
    imageTag: String,
  ): ConnectorSpecification {
    val commandOutput = commandService.getSpecJobOutput(commandId, withLogs = false)
    if (commandOutput?.spec == null) {
      throw IllegalStateException("Failed to retrieve spec for image: $imageName:$imageTag")
    }
    return commandOutput.spec
  }

  /**
   * Safely retrieve spec from command, returning failure reason instead of throwing.
   * This allows the workflow to complete gracefully with a failure result.
   */
  private fun getSpecFromCommandSafely(
    commandId: String,
    imageName: String,
    imageTag: String,
  ): SpecResult =
    try {
      val commandOutput = commandService.getSpecJobOutput(commandId, withLogs = false)
      if (commandOutput == null) {
        SpecResult(
          null,
          ApiFailureReason()
            .failureOrigin(ApiFailureOrigin.AIRBYTE_PLATFORM)
            .failureType(ApiFailureType.SYSTEM_ERROR)
            .internalMessage("Command output not found for commandId: $commandId")
            .timestamp(Instant.now().toEpochMilli()),
        )
      } else if (commandOutput.spec == null) {
        // If the command has a failure reason, convert it to API FailureReason
        // Otherwise create a new one with a default message
        val apiFailureReason =
          if (commandOutput.failureReason != null) {
            // Convert from config.FailureReason to API FailureReason
            // This already has proper origin/type set from the launcher/workload
            apiToPojoConverters.failureReasonToApi(commandOutput.failureReason)
          } else {
            // Fallback if no failure reason was provided
            ApiFailureReason()
              .failureOrigin(ApiFailureOrigin.AIRBYTE_PLATFORM)
              .failureType(ApiFailureType.SYSTEM_ERROR)
              .internalMessage("Failed to retrieve spec for image: $imageName:$imageTag")
              .timestamp(Instant.now().toEpochMilli())
          }
        SpecResult(null, apiFailureReason)
      } else {
        SpecResult(commandOutput.spec, null)
      }
    } catch (e: Exception) {
      SpecResult(
        null,
        ApiFailureReason()
          .failureOrigin(ApiFailureOrigin.AIRBYTE_PLATFORM)
          .failureType(ApiFailureType.SYSTEM_ERROR)
          .internalMessage(e.message ?: "Unknown error retrieving spec for image: $imageName:$imageTag")
          .timestamp(Instant.now().toEpochMilli()),
      )
    }

  /**
   * Get the result of an actor definition create or update operation.
   *
   * @param requestId - the request ID returned from create or update
   * @return ActorDefinitionResultResponse with status and optional result data
   */
  fun getActorDefinitionResult(requestId: String): ActorDefinitionResultResponse {
    val output =
      temporalClient.tryGetActorDefinitionWorkflowResult(requestId)
        ?: return ActorDefinitionResultResponse().status(ActorDefinitionResultResponse.StatusEnum.RUNNING)
    return ActorDefinitionResultResponse()
      .status(
        if (output.actorDefinitionId !=
          null
        ) {
          ActorDefinitionResultResponse.StatusEnum.SUCCEEDED
        } else {
          ActorDefinitionResultResponse.StatusEnum.FAILED
        },
      ).actorDefinitionId(output.actorDefinitionId)
      .failureReason(apiToPojoConverters.failureReasonToApi(output.failureReason))
  }
}
