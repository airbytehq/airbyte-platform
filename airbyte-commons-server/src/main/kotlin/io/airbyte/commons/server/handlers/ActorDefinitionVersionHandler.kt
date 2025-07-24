/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import datadog.trace.api.Trace
import io.airbyte.api.model.generated.ActorDefinitionVersionBreakingChanges
import io.airbyte.api.model.generated.ActorDefinitionVersionRead
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.GetActorDefinitionVersionDefaultRequestBody
import io.airbyte.api.model.generated.ResolveActorDefinitionVersionRequestBody
import io.airbyte.api.model.generated.ResolveActorDefinitionVersionResponse
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.errors.NotFoundException
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.persistence.ActorDefinitionVersionHelper.ActorDefinitionVersionWithOverrideStatus
import io.airbyte.config.persistence.ActorDefinitionVersionResolver
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.io.IOException
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
    @Trace
    @Throws(
      JsonValidationException::class,
      IOException::class,
      ConfigNotFoundException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
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

    @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
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

    @Throws(IOException::class)
    fun getDefaultVersion(actorDefinitionVersionDefaultRequestBody: GetActorDefinitionVersionDefaultRequestBody): ActorDefinitionVersionRead {
      val version =
        actorDefinitionService.getDefaultVersionForActorDefinitionIdOptional(actorDefinitionVersionDefaultRequestBody.actorDefinitionId)
      return createActorDefinitionVersionRead(ActorDefinitionVersionWithOverrideStatus(version.get(), false))
    }

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun resolveActorDefinitionVersionByTag(resolveVersionReq: ResolveActorDefinitionVersionRequestBody): ResolveActorDefinitionVersionResponse {
      val actorDefinitionId = resolveVersionReq.actorDefinitionId
      val actorType = apiPojoConverters.toInternalActorType(resolveVersionReq.actorType)
      val defaultVersion = getDefaultVersion(actorType!!, actorDefinitionId)

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

      return ResolveActorDefinitionVersionResponse()
        .versionId(resolvedVersion.versionId)
        .dockerImageTag(resolvedVersion.dockerImageTag)
        .dockerRepository(resolvedVersion.dockerRepository)
        .supportRefreshes(resolvedVersion.supportsRefreshes)
        .supportFileTransfer(resolvedVersion.supportsFileTransfer)
        .supportDataActivation(resolvedVersion.supportsDataActivation)
        .connectorIPCOptions(resolvedVersion.connectorIPCOptions)
    }

    @Trace
    @Throws(
      JsonValidationException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
      IOException::class,
      ConfigNotFoundException::class,
    )
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

    @VisibleForTesting
    @Throws(IOException::class)
    fun createActorDefinitionVersionRead(versionWithOverrideStatus: ActorDefinitionVersionWithOverrideStatus): ActorDefinitionVersionRead {
      val actorDefinitionVersion = versionWithOverrideStatus.actorDefinitionVersion
      val advRead =
        ActorDefinitionVersionRead()
          .dockerRepository(actorDefinitionVersion.dockerRepository)
          .dockerImageTag(actorDefinitionVersion.dockerImageTag)
          .supportsRefreshes(actorDefinitionVersion.supportsRefreshes)
          .supportState(apiPojoConverters.toApiSupportState(actorDefinitionVersion.supportState))
          .supportLevel(apiPojoConverters.toApiSupportLevel(actorDefinitionVersion.supportLevel))
          .cdkVersion(actorDefinitionVersion.cdkVersion)
          .lastPublished(apiPojoConverters.toOffsetDateTime(actorDefinitionVersion.lastPublished))
          .isVersionOverrideApplied(versionWithOverrideStatus.isOverrideApplied)
          .supportsFileTransfer(actorDefinitionVersion.supportsFileTransfer)
          .supportsDataActivation(actorDefinitionVersion.supportsDataActivation)
          .connectorIPCOptions(actorDefinitionVersion.connectorIPCOptions)

      val breakingChanges =
        actorDefinitionHandlerHelper.getVersionBreakingChanges(actorDefinitionVersion)
      breakingChanges.ifPresent { breakingChanges: ActorDefinitionVersionBreakingChanges? ->
        advRead.breakingChanges =
          breakingChanges
      }

      return advRead
    }
  }
