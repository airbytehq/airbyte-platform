/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.AdvancedAuth
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.DestinationDefinitionSpecificationRead
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.SourceDefinitionSpecificationRead
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.converters.OauthModelConverter.getAdvancedAuth
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata.Companion.mock
import io.airbyte.config.JobConfig
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.SourceService
import io.airbyte.domain.models.EntitledConnectorSpec
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.services.entitlements.ConnectorConfigEntitlementService
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID

/**
 * This class is responsible for getting the specification for a given connector. It is used by the
 * [io.airbyte.commons.server.handlers.SchedulerHandler] to get the specification for a
 * connector.
 */
@Singleton
open class ConnectorDefinitionSpecificationHandler(
  private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
  private val jobConverter: JobConverter,
  private val sourceService: SourceService,
  private val destinationService: DestinationService,
  private val workspaceHelper: WorkspaceHelper,
  private val oAuthService: OAuthService,
  private val connectorConfigEntitlementService: ConnectorConfigEntitlementService,
) {
  /**
   * Get the specification for a given source.
   *
   * @param sourceIdRequestBody - the id of the source to get the specification for.
   * @return the specification for the source.
   * @throws JsonValidationException - if the specification is invalid.
   * @throws ConfigNotFoundException - if the source does not exist.
   * @throws IOException - if there is an error reading the specification.
   */
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun getSpecificationForSourceId(sourceIdRequestBody: SourceIdRequestBody): SourceDefinitionSpecificationRead {
    val source = sourceService.getSourceConnection(sourceIdRequestBody.sourceId)
    val sourceDefinition = sourceService.getStandardSourceDefinition(source.sourceDefinitionId)
    val sourceVersion =
      actorDefinitionVersionHelper.getSourceVersion(sourceDefinition, source.workspaceId, sourceIdRequestBody.sourceId)
    // todo (cgardens) - passing workspace id here seems like a bug.
    val entitledConnectorSpec =
      connectorConfigEntitlementService.getEntitledConnectorSpec(OrganizationId(source.workspaceId), sourceVersion)
    return getSourceSpecificationRead(sourceDefinition, entitledConnectorSpec, source.workspaceId)
  }

  /**
   * Get the definition specification for a given source.
   *
   * @param sourceDefinitionIdWithWorkspaceId - the id of the source to get the specification for.
   * @return the specification for the source.
   * @throws JsonValidationException - if the specification is invalid.
   * @throws ConfigNotFoundException - if the source does not exist.
   * @throws IOException - if there is an error reading the specification.
   */
  @Throws(
    ConfigNotFoundException::class,
    IOException::class,
    JsonValidationException::class,
    io.airbyte.config.persistence.ConfigNotFoundException::class,
  )
  fun getSourceDefinitionSpecification(sourceDefinitionIdWithWorkspaceId: SourceDefinitionIdWithWorkspaceId): SourceDefinitionSpecificationRead {
    val sourceDefinitionId = sourceDefinitionIdWithWorkspaceId.sourceDefinitionId
    val source = sourceService.getStandardSourceDefinition(sourceDefinitionId)
    val sourceVersion =
      actorDefinitionVersionHelper.getSourceVersion(source, sourceDefinitionIdWithWorkspaceId.workspaceId)
    val organizationId = workspaceHelper.getOrganizationForWorkspace(sourceDefinitionIdWithWorkspaceId.workspaceId)
    val entitledConnectorSpec =
      connectorConfigEntitlementService.getEntitledConnectorSpec(OrganizationId(organizationId), sourceVersion)
    return getSourceSpecificationRead(source, entitledConnectorSpec, sourceDefinitionIdWithWorkspaceId.workspaceId)
  }

  /**
   * Get the specification for a given destination.
   *
   * @param destinationIdRequestBody - the id of the destination to get the specification for.
   * @return the specification for the destination.
   * @throws JsonValidationException - if the specification is invalid.
   * @throws ConfigNotFoundException - if the destination does not exist.
   * @throws IOException - if there is an error reading the specification.
   */
  @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
  fun getSpecificationForDestinationId(destinationIdRequestBody: DestinationIdRequestBody): DestinationDefinitionSpecificationRead {
    val destination = destinationService.getDestinationConnection(destinationIdRequestBody.destinationId)
    val destinationDefinition =
      destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId)
    val destinationVersion =
      actorDefinitionVersionHelper.getDestinationVersion(
        destinationDefinition,
        destination.workspaceId,
        destinationIdRequestBody.destinationId,
      )
    val organizationId = workspaceHelper.getOrganizationForWorkspace(destination.workspaceId)
    val entitledConnectorSpec =
      connectorConfigEntitlementService.getEntitledConnectorSpec(OrganizationId(organizationId), destinationVersion)
    return getDestinationSpecificationRead(
      destinationDefinition,
      entitledConnectorSpec,
      destinationVersion.supportsRefreshes,
      destination.workspaceId,
    )
  }

  /**
   * Get the definition specification for a given destination.
   *
   * @param destinationDefinitionIdWithWorkspaceId - the id of the destination to get the
   * specification for.
   * @return the specification for the destination.
   * @throws JsonValidationException - if the specification is invalid.
   * @throws ConfigNotFoundException - if the destination does not exist.
   * @throws IOException - if there is an error reading the specification.
   */
  @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
  fun getDestinationSpecification(
    destinationDefinitionIdWithWorkspaceId: DestinationDefinitionIdWithWorkspaceId,
  ): DestinationDefinitionSpecificationRead {
    val destinationDefinitionId = destinationDefinitionIdWithWorkspaceId.destinationDefinitionId
    val destination = destinationService.getStandardDestinationDefinition(destinationDefinitionId)
    val destinationVersion =
      actorDefinitionVersionHelper.getDestinationVersion(destination, destinationDefinitionIdWithWorkspaceId.workspaceId)
    val organizationId = workspaceHelper.getOrganizationForWorkspace(destinationDefinitionIdWithWorkspaceId.workspaceId)
    val entitledConnectorSpec =
      connectorConfigEntitlementService.getEntitledConnectorSpec(OrganizationId(organizationId), destinationVersion)
    return getDestinationSpecificationRead(
      destination,
      entitledConnectorSpec,
      destinationVersion.supportsRefreshes,
      destinationDefinitionIdWithWorkspaceId.workspaceId,
    )
  }

  @VisibleForTesting
  @Throws(IOException::class)
  fun getSourceSpecificationRead(
    sourceDefinition: StandardSourceDefinition,
    entitledConnectorSpec: EntitledConnectorSpec,
    workspaceId: UUID,
  ): SourceDefinitionSpecificationRead {
    val spec = entitledConnectorSpec.spec
    val specRead =
      SourceDefinitionSpecificationRead()
        .jobInfo(jobConverter.getSynchronousJobRead(mock(JobConfig.ConfigType.GET_SPEC)))
        .connectionSpecification(spec.connectionSpecification)
        .sourceDefinitionId(sourceDefinition.sourceDefinitionId)

    if (spec.documentationUrl != null) {
      specRead.documentationUrl(spec.documentationUrl.toString())
    }

    val advancedAuth = getAdvancedAuth(spec)
    advancedAuth.ifPresent { advancedAuth: AdvancedAuth? ->
      specRead.advancedAuth =
        advancedAuth
    }
    if (advancedAuth.isPresent) {
      val sourceOAuthParameter =
        oAuthService.getSourceOAuthParameterOptional(workspaceId, sourceDefinition.sourceDefinitionId)
      specRead.advancedAuthGlobalCredentialsAvailable = sourceOAuthParameter.isPresent
    }

    return specRead
  }

  @VisibleForTesting
  @Throws(IOException::class)
  fun getDestinationSpecificationRead(
    destinationDefinition: StandardDestinationDefinition,
    entitledConnectorSpec: EntitledConnectorSpec,
    supportsRefreshes: Boolean,
    workspaceId: UUID,
  ): DestinationDefinitionSpecificationRead {
    val spec = entitledConnectorSpec.spec
    val specRead =
      DestinationDefinitionSpecificationRead()
        .jobInfo(jobConverter.getSynchronousJobRead(mock(JobConfig.ConfigType.GET_SPEC)))
        .supportedDestinationSyncModes(getFinalDestinationSyncModes(spec.supportedDestinationSyncModes, supportsRefreshes))
        .connectionSpecification(spec.connectionSpecification)
        .documentationUrl(spec.documentationUrl.toString())
        .destinationDefinitionId(destinationDefinition.destinationDefinitionId)

    val advancedAuth = getAdvancedAuth(spec)
    advancedAuth.ifPresent { advancedAuth: AdvancedAuth? ->
      specRead.advancedAuth =
        advancedAuth
    }
    if (advancedAuth.isPresent) {
      val destinationOAuthParameter =
        oAuthService.getDestinationOAuthParameterOptional(workspaceId, destinationDefinition.destinationDefinitionId)
      specRead.advancedAuthGlobalCredentialsAvailable = destinationOAuthParameter.isPresent
    }

    return specRead
  }

  private fun getFinalDestinationSyncModes(
    syncModes: List<DestinationSyncMode>,
    supportsRefreshes: Boolean,
  ): List<io.airbyte.api.model.generated.DestinationSyncMode> {
    val finalSyncModes: MutableList<io.airbyte.api.model.generated.DestinationSyncMode> = ArrayList()
    var hasDedup = false
    var hasOverwrite = false
    for (syncMode in syncModes) {
      when (syncMode) {
        DestinationSyncMode.APPEND -> finalSyncModes.add(io.airbyte.api.model.generated.DestinationSyncMode.APPEND)
        DestinationSyncMode.APPEND_DEDUP -> {
          finalSyncModes.add(io.airbyte.api.model.generated.DestinationSyncMode.APPEND_DEDUP)
          hasDedup = true
        }

        DestinationSyncMode.OVERWRITE -> {
          finalSyncModes.add(io.airbyte.api.model.generated.DestinationSyncMode.OVERWRITE)
          hasOverwrite = true
        }

        DestinationSyncMode.UPDATE -> finalSyncModes.add(io.airbyte.api.model.generated.DestinationSyncMode.UPDATE)
        DestinationSyncMode.SOFT_DELETE -> finalSyncModes.add(io.airbyte.api.model.generated.DestinationSyncMode.SOFT_DELETE)
        else -> throw IllegalStateException("Unexpected value: $syncMode")
      }
    }
    if (supportsRefreshes && hasDedup && hasOverwrite) {
      finalSyncModes.add(io.airbyte.api.model.generated.DestinationSyncMode.OVERWRITE_DEDUP)
    }
    return finalSyncModes
  }
}
