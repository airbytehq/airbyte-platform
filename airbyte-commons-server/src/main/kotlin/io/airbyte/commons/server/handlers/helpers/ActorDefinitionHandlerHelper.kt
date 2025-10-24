/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers

import io.airbyte.api.model.generated.ActorDefinitionVersionBreakingChanges
import io.airbyte.api.model.generated.DeadlineAction
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.commons.server.ServerConstants
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.errors.UnsupportedProtocolVersionException
import io.airbyte.commons.server.scheduler.SynchronousSchedulerClient
import io.airbyte.commons.version.AirbyteProtocolVersion
import io.airbyte.commons.version.AirbyteProtocolVersionRange
import io.airbyte.config.ActorDefinitionBreakingChange
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.ReleaseStage
import io.airbyte.config.SupportLevel
import io.airbyte.config.helpers.ConnectorRegistryConverters.toActorDefinitionBreakingChanges
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator
import io.airbyte.config.persistence.ActorDefinitionVersionResolver
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.protocol.models.v0.ConnectorSpecification
import jakarta.inject.Singleton
import java.io.IOException
import java.net.URI
import java.time.LocalDate
import java.util.Optional
import java.util.UUID

/**
 * A helper class for server code that is the shared for actor definitions (source definitions and
 * destination definitions).
 */
@Singleton
class ActorDefinitionHandlerHelper(
  private val synchronousSchedulerClient: SynchronousSchedulerClient,
  private val protocolVersionRange: AirbyteProtocolVersionRange,
  private val actorDefinitionVersionResolver: ActorDefinitionVersionResolver,
  private val remoteDefinitionsProvider: RemoteDefinitionsProvider,
  private val actorDefinitionService: ActorDefinitionService,
  private val apiPojoConverters: ApiPojoConverters,
  private val airbyteCompatibleConnectorsValidator: AirbyteCompatibleConnectorsValidator,
) {
  /**
   * Validates that a connector version is supported by the current platform version.
   *
   * @param actorDefinitionId - the actor definition id
   * @param connectorVersion - the connector version (docker image tag) to validate
   * @param actorType - the actor type (SOURCE or DESTINATION)
   * @throws BadRequestProblem if the connector version is not supported by the platform
   */
  fun validateVersionSupport(
    actorDefinitionId: UUID,
    connectorVersion: String,
    actorType: ActorType,
  ) {
    val isNewConnectorVersionSupported =
      airbyteCompatibleConnectorsValidator.validate(
        connectorId = actorDefinitionId.toString(),
        connectorVersion = connectorVersion,
      )
    if (!isNewConnectorVersionSupported.isValid) {
      val message =
        if (isNewConnectorVersionSupported.message != null) {
          isNewConnectorVersionSupported.message
        } else {
          "$actorType $actorDefinitionId can't be updated to version $connectorVersion because the version is not supported by current platform version"
        }
      throw BadRequestProblem(message, ProblemMessageData().message(message))
    }
  }

  /**
   * Create a new actor definition version to set as default for a new connector from a create
   * request.
   *
   * @param dockerRepository - the docker repository
   * @param dockerImageTag - the docker image tag
   * @param documentationUrl - the documentation url
   * @param workspaceId - the workspace id
   * @return - the new actor definition version
   * @throws IOException - if there is an error fetching the spec
   */
  fun defaultDefinitionVersionFromCreate(
    dockerRepository: String,
    dockerImageTag: String,
    documentationUrl: URI,
    workspaceId: UUID,
  ): ActorDefinitionVersion {
    val spec =
      getSpecForImage(
        dockerRepository,
        dockerImageTag, // Only custom connectors can be created via handlers.
        true,
        workspaceId,
      )

    return defaultDefinitionVersionFromCreate(spec, dockerRepository, dockerImageTag, documentationUrl.toString())
  }

  fun defaultDefinitionVersionFromCreate(
    spec: ConnectorSpecification,
    dockerRepository: String,
    dockerImageTag: String,
    documentationUrl: String,
  ): ActorDefinitionVersion {
    val protocolVersion = getAndValidateProtocolVersionFromSpec(spec)

    return ActorDefinitionVersion()
      .withDockerImageTag(dockerImageTag)
      .withDockerRepository(dockerRepository)
      .withSpec(spec)
      .withDocumentationUrl(documentationUrl)
      .withProtocolVersion(protocolVersion)
      .withSupportLevel(SupportLevel.NONE)
      .withInternalSupportLevel(100L)
      .withReleaseStage(ReleaseStage.CUSTOM)
  }

  /**
   * Create a new actor definition version to set as default from an existing default version and an
   * update request.
   *
   * @param currentVersion - the current default version
   * @param newDockerImageTag - the new docker image tag
   * @param isCustomConnector - whether the connector is a custom connector
   * @param workspaceId - context in which the job will run
   * @return - a new actor definition version
   * @throws IOException - if there is an error fetching the spec
   */
  fun defaultDefinitionVersionFromUpdate(
    currentVersion: ActorDefinitionVersion,
    actorType: ActorType,
    newDockerImageTag: String,
    isCustomConnector: Boolean,
    workspaceId: UUID,
  ): ActorDefinitionVersion {
    val newVersionFromDbOrRemote =
      actorDefinitionVersionResolver.resolveVersionForTag(
        currentVersion.actorDefinitionId,
        actorType,
        currentVersion.dockerRepository,
        newDockerImageTag,
      )

    val isDev = ServerConstants.DEV_IMAGE_TAG == newDockerImageTag

    // The version already exists in the database or in our registry
    if (newVersionFromDbOrRemote.isPresent) {
      val newVersion = newVersionFromDbOrRemote.get()

      if (isDev) {
        // re-fetch spec for dev images to allow for easier iteration
        val refreshedSpec =
          getSpecForImage(currentVersion.dockerRepository, newDockerImageTag, isCustomConnector, workspaceId)
        newVersion.spec = refreshedSpec
        newVersion.protocolVersion = getAndValidateProtocolVersionFromSpec(refreshedSpec)
      }

      return newVersion
    }

    // We've never seen this version
    val spec = getSpecForImage(currentVersion.dockerRepository, newDockerImageTag, isCustomConnector, workspaceId)
    val protocolVersion = getAndValidateProtocolVersionFromSpec(spec)

    return ActorDefinitionVersion()
      .withActorDefinitionId(currentVersion.actorDefinitionId)
      .withDockerRepository(currentVersion.dockerRepository)
      .withDockerImageTag(newDockerImageTag)
      .withSpec(spec)
      .withDocumentationUrl(currentVersion.documentationUrl)
      .withProtocolVersion(protocolVersion)
      .withReleaseStage(currentVersion.releaseStage)
      .withReleaseDate(currentVersion.releaseDate)
      .withSupportLevel(currentVersion.supportLevel)
      .withInternalSupportLevel(currentVersion.internalSupportLevel)
      .withCdkVersion(currentVersion.cdkVersion)
      .withLastPublished(currentVersion.lastPublished)
      .withAllowedHosts(currentVersion.allowedHosts)
      .withSupportsFileTransfer(currentVersion.supportsFileTransfer)
      .withSupportsRefreshes(currentVersion.supportsRefreshes)
  }

  fun defaultDefinitionVersionFromUpdate(
    currentVersionId: UUID,
    spec: ConnectorSpecification?,
    actorType: ActorType,
    newDockerImageTag: String,
  ): ActorDefinitionVersion {
    val currentVersion = actorDefinitionService.getActorDefinitionVersion(currentVersionId)

    val newVersionFromCache =
      actorDefinitionVersionResolver.resolveVersionForTag(
        actorDefinitionId = currentVersion.actorDefinitionId,
        actorType = actorType,
        dockerRepository = currentVersion.dockerRepository,
        dockerImageTag = newDockerImageTag,
      )

    // The version already exists in the database or in our registry
    if (newVersionFromCache.isPresent) {
      val newVersion = newVersionFromCache.get()
      if (spec != null) {
        newVersion.spec = spec
        newVersion.protocolVersion = getAndValidateProtocolVersionFromSpec(spec)
      }
      return newVersion
    }

    if (spec == null) {
      throw IllegalStateException("Failed to retrieve spec for image: ${currentVersion.dockerRepository}:$newDockerImageTag")
    }

    // We've never seen this version
    return ActorDefinitionVersion()
      .withActorDefinitionId(currentVersion.actorDefinitionId)
      .withDockerRepository(currentVersion.dockerRepository)
      .withDockerImageTag(newDockerImageTag)
      .withSpec(spec)
      .withDocumentationUrl(currentVersion.documentationUrl)
      .withProtocolVersion(getAndValidateProtocolVersionFromSpec(spec))
      .withReleaseStage(currentVersion.releaseStage)
      .withReleaseDate(currentVersion.releaseDate)
      .withSupportLevel(currentVersion.supportLevel)
      .withInternalSupportLevel(currentVersion.internalSupportLevel)
      .withCdkVersion(currentVersion.cdkVersion)
      .withLastPublished(currentVersion.lastPublished)
      .withAllowedHosts(currentVersion.allowedHosts)
      .withSupportsFileTransfer(currentVersion.supportsFileTransfer)
      .withSupportsRefreshes(currentVersion.supportsRefreshes)
  }

  fun shouldFetchSpec(
    currentVersion: ActorDefinitionVersion?,
    connectorVersion: String,
    actorType: ActorType,
  ): Boolean {
    if (currentVersion == null || ServerConstants.DEV_IMAGE_TAG == connectorVersion) {
      return true
    }
    val newVersionFromCache =
      actorDefinitionVersionResolver.resolveVersionForTag(
        actorDefinitionId = currentVersion.actorDefinitionId,
        actorType = actorType,
        dockerRepository = currentVersion.dockerRepository,
        dockerImageTag = connectorVersion,
      )
    return !newVersionFromCache.isPresent
  }

  private fun getSpecForImage(
    dockerRepository: String,
    imageTag: String,
    isCustomConnector: Boolean,
    workspaceId: UUID,
  ): ConnectorSpecification {
    val imageName = "$dockerRepository:$imageTag"
    val response = synchronousSchedulerClient.createGetSpecJob(imageName, isCustomConnector, workspaceId)
    if (!response.isSuccess) {
      throw IllegalStateException("Get Spec Job failed (imageName=$imageName, isCustomConnector=$isCustomConnector, workspaceId=$workspaceId)")
    }

    if (response.output == null) {
      throw NullPointerException(
        "Get Spec job return null spec (imageName=$imageName, isCustomConnector=$isCustomConnector, workspaceId=$workspaceId)",
      )
    }

    return response.output
  }

  fun getAndValidateProtocolVersionFromSpec(spec: ConnectorSpecification): String {
    val airbyteProtocolVersion = AirbyteProtocolVersion.getWithDefault(spec.protocolVersion)
    if (!protocolVersionRange.isSupported(airbyteProtocolVersion)) {
      throw UnsupportedProtocolVersionException(airbyteProtocolVersion, protocolVersionRange.min, protocolVersionRange.max)
    }
    return airbyteProtocolVersion.serialize()
  }

  /**
   * Fetches an optional breaking change list from the registry entry for the actor definition version
   * and persists it to the DB if present. The optional is empty if the registry entry is not found.
   *
   * @param actorDefinitionVersion - the actor definition version
   * @param actorType - the actor type
   * @throws IOException - if there is an error persisting the breaking changes
   */
  fun getBreakingChanges(
    actorDefinitionVersion: ActorDefinitionVersion,
    actorType: ActorType,
  ): List<ActorDefinitionBreakingChange> {
    val connectorRepository = actorDefinitionVersion.dockerRepository
    // We always want the most up-to-date version of the list breaking changes, in case they've been
    // updated retroactively after the version was released.
    val dockerImageTag = "latest"
    val breakingChanges: Optional<List<ActorDefinitionBreakingChange>>
    when (actorType) {
      ActorType.SOURCE -> {
        val registryDef =
          remoteDefinitionsProvider.getSourceDefinitionByVersion(connectorRepository, dockerImageTag)
        breakingChanges =
          registryDef.map { obj: ConnectorRegistrySourceDefinition ->
            toActorDefinitionBreakingChanges(
              obj,
            )
          }
      }

      ActorType.DESTINATION -> {
        val registryDef =
          remoteDefinitionsProvider.getDestinationDefinitionByVersion(connectorRepository, dockerImageTag)
        breakingChanges =
          registryDef.map { obj: ConnectorRegistryDestinationDefinition ->
            toActorDefinitionBreakingChanges(
              obj,
            )
          }
      }

      else -> throw IllegalArgumentException("Actor type not supported: $actorType")
    }

    return breakingChanges.orElse(listOf())
  }

  private fun firstUpcomingBreakingChange(breakingChanges: List<ActorDefinitionBreakingChange>): Optional<ActorDefinitionBreakingChange> =
    breakingChanges
      .stream()
      .min(
        Comparator.comparing { b: ActorDefinitionBreakingChange ->
          LocalDate.parse(
            b.upgradeDeadline,
          )
        },
      )

  fun getVersionBreakingChanges(actorDefinitionVersion: ActorDefinitionVersion): Optional<ActorDefinitionVersionBreakingChanges> {
    val breakingChanges =
      actorDefinitionService.listBreakingChangesForActorDefinitionVersion(actorDefinitionVersion)

    if (!breakingChanges.isEmpty()) {
      val firstBreakingChange = firstUpcomingBreakingChange(breakingChanges)
      val minUpgradeDeadline =
        firstBreakingChange
          .map { it: ActorDefinitionBreakingChange ->
            LocalDate.parse(
              it.upgradeDeadline,
            )
          }.orElse(null)
      val minDeadlineAction =
        firstBreakingChange
          .map { obj: ActorDefinitionBreakingChange -> obj.deadlineAction }
          .orElse(null)
      val apiDeadlineAction =
        if (minDeadlineAction == DeadlineAction.AUTO_UPGRADE.toString()) DeadlineAction.AUTO_UPGRADE else DeadlineAction.DISABLE
      return Optional.of(
        ActorDefinitionVersionBreakingChanges()
          .upcomingBreakingChanges(
            breakingChanges
              .stream()
              .map { breakingChange: ActorDefinitionBreakingChange? -> apiPojoConverters.toApiBreakingChange(breakingChange) }
              .toList(),
          ).minUpgradeDeadline(minUpgradeDeadline)
          .deadlineAction(apiDeadlineAction),
      )
    } else {
      return Optional.empty()
    }
  }
}
