/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.server.generated.models.ActorDefinitionVersionBreakingChanges
import io.airbyte.api.server.generated.models.ActorDefinitionVersionRead
import io.airbyte.api.server.generated.models.DestinationIdRequestBody
import io.airbyte.api.server.generated.models.GetActorDefinitionVersionDefaultRequestBody
import io.airbyte.api.server.generated.models.ResolveActorDefinitionVersionRequestBody
import io.airbyte.api.server.generated.models.ResolveActorDefinitionVersionResponse
import io.airbyte.api.server.generated.models.SourceIdRequestBody
import io.airbyte.api.server.generated.models.SupportLevel
import io.airbyte.api.server.generated.models.SupportState
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.enums.convertTo
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.errors.NotFoundException
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus
import io.airbyte.config.persistence.ActorDefinitionVersionResolver
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.opentelemetry.instrumentation.annotations.WithSpan
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.util.UUID

/**
 * ActorDefinitionVersionHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@Singleton
open class ActorDefinitionVersionHandler
  @Inject
  constructor(
    private val sourceService: SourceService,
    private val destinationService: DestinationService,
    private val actorDefinitionService: ActorDefinitionService,
    private val actorDefinitionVersionResolver: ActorDefinitionVersionResolver,
    private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
    private val actorDefinitionHandlerHelper: ActorDefinitionHandlerHelper,
    private val apiPojoConverters: ApiPojoConverters,
  ) {
    @WithSpan
    fun getActorDefinitionVersionForSourceId(sourceIdRequestBody: SourceIdRequestBody): ActorDefinitionVersionRead {
      val sourceConnection = sourceService.getSourceConnection(sourceIdRequestBody.sourceId)
      val sourceDefinition = sourceService.getSourceDefinitionFromSource(sourceConnection.sourceId)
      val versionWithOverrideStatus =
        actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(
          sourceDefinition,
          sourceConnection.workspaceId,
          sourceConnection.sourceId,
        )
      return createActorDefinitionVersionRead(versionWithOverrideStatus)
    }

    private fun getDefaultVersion(
      actorType: ActorType,
      actorDefinitionId: UUID,
    ): ActorDefinitionVersion =
      when (actorType) {
        ActorType.SOURCE ->
          actorDefinitionService
            .getActorDefinitionVersion(sourceService.getStandardSourceDefinition(actorDefinitionId).defaultVersionId)

        ActorType.DESTINATION ->
          actorDefinitionService.getActorDefinitionVersion(
            destinationService.getStandardDestinationDefinition(actorDefinitionId).defaultVersionId,
          )
      }

    fun getDefaultVersion(actorDefinitionVersionDefaultRequestBody: GetActorDefinitionVersionDefaultRequestBody): ActorDefinitionVersionRead {
      val version =
        actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionVersionDefaultRequestBody.actorDefinitionId)
      return createActorDefinitionVersionRead(ActorDefinitionVersionWithOverrideStatus(version.get(), false))
    }

    fun resolveActorDefinitionVersionByTag(resolveVersionReq: ResolveActorDefinitionVersionRequestBody): ResolveActorDefinitionVersionResponse {
      val actorDefinitionId = resolveVersionReq.actorDefinitionId
      val actorType = resolveVersionReq.actorType.convertTo<io.airbyte.config.ActorType>()
      val defaultVersion = getDefaultVersion(actorType, actorDefinitionId)

      val optResolvedVersion =
        actorDefinitionVersionResolver.resolveVersionForTag(
          actorDefinitionId,
          actorType,
          defaultVersion.dockerRepository,
          resolveVersionReq.dockerImageTag,
        )

      if (optResolvedVersion.isEmpty) {
        throw NotFoundException(
          String.format(
            "Could not find actor definition version for actor definition id %s and tag %s",
            actorDefinitionId,
            resolveVersionReq.dockerImageTag,
          ),
        )
      }

      val resolvedVersion = optResolvedVersion.get()

      return ResolveActorDefinitionVersionResponse(
        versionId = resolvedVersion.versionId,
        dockerImageTag = resolvedVersion.dockerImageTag,
        dockerRepository = resolvedVersion.dockerRepository,
        supportRefreshes = resolvedVersion.supportsRefreshes,
        supportFileTransfer = resolvedVersion.supportsFileTransfer,
        supportDataActivation = resolvedVersion.supportsDataActivation,
        connectorIPCOptions = resolvedVersion.connectorIPCOptions,
      )
    }

    @WithSpan
    fun getActorDefinitionVersionForDestinationId(destinationIdRequestBody: DestinationIdRequestBody): ActorDefinitionVersionRead {
      val destinationConnection = destinationService.getDestinationConnection(destinationIdRequestBody.destinationId)
      val destinationDefinition =
        destinationService.getDestinationDefinitionFromDestination(destinationConnection.destinationId)
      val versionWithOverrideStatus =
        actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
          destinationDefinition,
          destinationConnection.workspaceId,
          destinationConnection.destinationId,
        )
      return createActorDefinitionVersionRead(versionWithOverrideStatus)
    }

    @InternalForTesting
    fun createActorDefinitionVersionRead(versionWithOverrideStatus: ActorDefinitionVersionWithOverrideStatus): ActorDefinitionVersionRead {
      val actorDefinitionVersion = versionWithOverrideStatus.actorDefinitionVersion
      // ApiPojoConverters and the helper still build Java models; convert them at this boundary
      val breakingChanges =
        actorDefinitionHandlerHelper
          .getVersionBreakingChanges(actorDefinitionVersion)
          .orElse(null)
          ?.let { Jsons.convertValue(it, ActorDefinitionVersionBreakingChanges::class.java) }

      return ActorDefinitionVersionRead(
        dockerRepository = actorDefinitionVersion.dockerRepository,
        dockerImageTag = actorDefinitionVersion.dockerImageTag,
        supportsRefreshes = actorDefinitionVersion.supportsRefreshes,
        // support_state is a NOT NULL column
        supportState = apiPojoConverters.toApiSupportState(actorDefinitionVersion.supportState)!!.convertTo<SupportState>(),
        supportLevel = apiPojoConverters.toApiSupportLevel(actorDefinitionVersion.supportLevel).convertTo<SupportLevel>(),
        cdkVersion = actorDefinitionVersion.cdkVersion,
        lastPublished = apiPojoConverters.toOffsetDateTime(actorDefinitionVersion.lastPublished),
        isVersionOverrideApplied = versionWithOverrideStatus.isOverrideApplied,
        supportsFileTransfer = actorDefinitionVersion.supportsFileTransfer,
        supportsDataActivation = actorDefinitionVersion.supportsDataActivation,
        connectorIPCOptions = actorDefinitionVersion.connectorIPCOptions,
        breakingChanges = breakingChanges,
      )
    }
  }
