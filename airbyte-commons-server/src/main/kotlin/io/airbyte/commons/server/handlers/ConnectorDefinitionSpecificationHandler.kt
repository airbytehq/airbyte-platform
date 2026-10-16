/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import io.airbyte.api.server.generated.models.AdvancedAuth
import io.airbyte.api.server.generated.models.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.server.generated.models.DestinationDefinitionSpecificationRead
import io.airbyte.api.server.generated.models.DestinationIdRequestBody
import io.airbyte.api.server.generated.models.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.server.generated.models.SourceDefinitionSpecificationRead
import io.airbyte.api.server.generated.models.SourceIdRequestBody
import io.airbyte.api.server.generated.models.SynchronousJobRead
import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.converters.JobConverter
import io.airbyte.commons.server.converters.OauthModelConverter.getAdvancedAuth
import io.airbyte.commons.server.scheduler.SynchronousJobMetadata.Companion.mock
import io.airbyte.config.JobConfig
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.WorkspaceHelper
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OAuthService
import io.airbyte.data.services.SourceService
import io.airbyte.domain.models.EntitledConnectorSpec
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.services.entitlements.ConnectorConfigEntitlementService
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID
import io.airbyte.api.server.generated.models.DestinationSyncMode as KotlinDestinationSyncMode

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

  @InternalForTesting
  fun getSourceSpecificationRead(
    sourceDefinition: StandardSourceDefinition,
    entitledConnectorSpec: EntitledConnectorSpec,
    workspaceId: UUID,
  ): SourceDefinitionSpecificationRead {
    val spec = entitledConnectorSpec.spec
    val advancedAuth = getKotlinAdvancedAuth(spec)
    val globalCredentialsAvailable =
      advancedAuth?.let { oAuthService.getSourceOAuthParameterOptional(workspaceId, sourceDefinition.sourceDefinitionId).isPresent }

    return SourceDefinitionSpecificationRead(
      jobInfo = getSpecJobInfo(),
      connectionSpecification = spec.connectionSpecification,
      sourceDefinitionId = sourceDefinition.sourceDefinitionId,
      documentationUrl = spec.documentationUrl?.toString(),
      advancedAuth = advancedAuth,
      advancedAuthGlobalCredentialsAvailable = globalCredentialsAvailable,
    )
  }

  @InternalForTesting
  fun getDestinationSpecificationRead(
    destinationDefinition: StandardDestinationDefinition,
    entitledConnectorSpec: EntitledConnectorSpec,
    supportsRefreshes: Boolean,
    workspaceId: UUID,
  ): DestinationDefinitionSpecificationRead {
    val spec = entitledConnectorSpec.spec
    val advancedAuth = getKotlinAdvancedAuth(spec)
    val globalCredentialsAvailable =
      advancedAuth?.let { oAuthService.getDestinationOAuthParameterOptional(workspaceId, destinationDefinition.destinationDefinitionId).isPresent }

    return DestinationDefinitionSpecificationRead(
      jobInfo = getSpecJobInfo(),
      supportedDestinationSyncModes = getFinalDestinationSyncModes(spec.supportedDestinationSyncModes, supportsRefreshes),
      connectionSpecification = spec.connectionSpecification,
      documentationUrl = spec.documentationUrl.toString(),
      destinationDefinitionId = destinationDefinition.destinationDefinitionId,
      advancedAuth = advancedAuth,
      advancedAuthGlobalCredentialsAvailable = globalCredentialsAvailable,
    )
  }

  // JobConverter and OauthModelConverter still build the Java models. Convert through JSON, not by enum name:
  // the logs field is untyped in Kotlin, and AdvancedAuth's flow-type constants are named differently.
  private fun getSpecJobInfo(): SynchronousJobRead =
    Jsons.convertValue(jobConverter.getSynchronousJobRead(mock(JobConfig.ConfigType.GET_SPEC)), SynchronousJobRead::class.java)

  private fun getKotlinAdvancedAuth(spec: ConnectorSpecification): AdvancedAuth? =
    getAdvancedAuth(spec).orElse(null)?.let { Jsons.convertValue(it, AdvancedAuth::class.java) }

  private fun getFinalDestinationSyncModes(
    syncModes: List<DestinationSyncMode>,
    supportsRefreshes: Boolean,
  ): List<KotlinDestinationSyncMode> {
    val finalSyncModes: MutableList<KotlinDestinationSyncMode> = ArrayList()
    var hasDedup = false
    var hasOverwrite = false
    for (syncMode in syncModes) {
      when (syncMode) {
        DestinationSyncMode.APPEND -> finalSyncModes.add(KotlinDestinationSyncMode.APPEND)
        DestinationSyncMode.APPEND_DEDUP -> {
          finalSyncModes.add(KotlinDestinationSyncMode.APPEND_DEDUP)
          hasDedup = true
        }

        DestinationSyncMode.OVERWRITE -> {
          finalSyncModes.add(KotlinDestinationSyncMode.OVERWRITE)
          hasOverwrite = true
        }

        DestinationSyncMode.UPDATE -> finalSyncModes.add(KotlinDestinationSyncMode.UPDATE)
        DestinationSyncMode.SOFT_DELETE -> finalSyncModes.add(KotlinDestinationSyncMode.SOFT_DELETE)
        else -> throw IllegalStateException("Unexpected value: $syncMode")
      }
    }
    if (supportsRefreshes && hasDedup && hasOverwrite) {
      finalSyncModes.add(KotlinDestinationSyncMode.OVERWRITE_DEDUP)
    }
    return finalSyncModes
  }
}
