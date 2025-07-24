/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.ActorDefinitionIdWithScope
import io.airbyte.api.model.generated.CustomDestinationDefinitionCreate
import io.airbyte.api.model.generated.DestinationDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.DestinationDefinitionRead
import io.airbyte.api.model.generated.DestinationDefinitionReadList
import io.airbyte.api.model.generated.DestinationDefinitionUpdate
import io.airbyte.api.model.generated.PrivateDestinationDefinitionRead
import io.airbyte.api.model.generated.PrivateDestinationDefinitionReadList
import io.airbyte.api.model.generated.WorkspaceIdActorDefinitionRequestBody
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.api.problems.model.generated.ProblemMessageData
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.UnprocessableEntityProblem
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.lang.Exceptions
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.errors.IdNotFoundKnownException
import io.airbyte.commons.server.errors.InternalServerKnownException
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectorRegistryDestinationDefinition
import io.airbyte.config.ScopeType
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.helpers.ConnectorRegistryConverters.toActorDefinitionVersion
import io.airbyte.config.helpers.ConnectorRegistryConverters.toStandardDestinationDefinition
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator
import io.airbyte.config.init.SupportStateUpdater
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.DestinationDefinition
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.HideActorDefinitionFromList
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.Workspace
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.IOException
import java.net.URI
import java.net.URISyntaxException
import java.util.UUID
import java.util.function.Function
import java.util.function.Supplier
import java.util.stream.Collectors
import java.util.stream.Stream

/**
 * DestinationDefinitionsHandler. Javadocs suppressed because api docs should be used as source of
 * truth.
 */
@Singleton
open class DestinationDefinitionsHandler
  @VisibleForTesting
  constructor(
    private val actorDefinitionService: ActorDefinitionService,
    @param:Named("uuidGenerator") private val uuidSupplier: Supplier<UUID>,
    private val actorDefinitionHandlerHelper: ActorDefinitionHandlerHelper,
    private val remoteDefinitionsProvider: RemoteDefinitionsProvider,
    private val destinationHandler: DestinationHandler,
    private val supportStateUpdater: SupportStateUpdater,
    private val featureFlagClient: FeatureFlagClient,
    private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
    private val airbyteCompatibleConnectorsValidator: AirbyteCompatibleConnectorsValidator,
    private val destinationService: DestinationService,
    private val workspaceService: WorkspaceService,
    private val licenseEntitlementChecker: LicenseEntitlementChecker,
    private val apiPojoConverters: ApiPojoConverters,
  ) {
    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun buildDestinationDefinitionRead(
      destinationDefinitionId: UUID,
      includeTombstone: Boolean,
    ): DestinationDefinitionRead {
      val destinationDefinition =
        destinationService.getStandardDestinationDefinition(destinationDefinitionId, includeTombstone)
      val destinationVersion = actorDefinitionService.getActorDefinitionVersion(destinationDefinition.defaultVersionId)
      return buildDestinationDefinitionRead(destinationDefinition, destinationVersion)
    }

    @VisibleForTesting
    fun buildDestinationDefinitionRead(
      standardDestinationDefinition: StandardDestinationDefinition,
      destinationVersion: ActorDefinitionVersion,
    ): DestinationDefinitionRead {
      try {
        return DestinationDefinitionRead()
          .destinationDefinitionId(standardDestinationDefinition.destinationDefinitionId)
          .name(standardDestinationDefinition.name)
          .dockerRepository(destinationVersion.dockerRepository)
          .dockerImageTag(destinationVersion.dockerImageTag)
          .documentationUrl(URI(destinationVersion.documentationUrl))
          .icon(standardDestinationDefinition.iconUrl)
          .protocolVersion(destinationVersion.protocolVersion)
          .supportLevel(apiPojoConverters.toApiSupportLevel(destinationVersion.supportLevel))
          .releaseStage(apiPojoConverters.toApiReleaseStage(destinationVersion.releaseStage))
          .releaseDate(apiPojoConverters.toLocalDate(destinationVersion.releaseDate))
          .lastPublished(apiPojoConverters.toOffsetDateTime(destinationVersion.lastPublished))
          .cdkVersion(destinationVersion.cdkVersion)
          .metrics(standardDestinationDefinition.metrics)
          .custom(standardDestinationDefinition.custom)
          .enterprise(standardDestinationDefinition.enterprise)
          .resourceRequirements(apiPojoConverters.scopedResourceReqsToApi(standardDestinationDefinition.resourceRequirements))
          .language(destinationVersion.language)
          .supportsDataActivation(destinationVersion.supportsDataActivation)
      } catch (e: URISyntaxException) {
        throw InternalServerKnownException("Unable to process retrieved latest destination definitions list", e)
      } catch (e: NullPointerException) {
        throw InternalServerKnownException("Unable to process retrieved latest destination definitions list", e)
      }
    }

    @Throws(IOException::class)
    fun listDestinationDefinitions(): DestinationDefinitionReadList {
      val standardDestinationDefinitions = destinationService.listStandardDestinationDefinitions(false)
      val destinationDefinitionVersionMap = getVersionsForDestinationDefinitions(standardDestinationDefinitions)
      return toDestinationDefinitionReadList(standardDestinationDefinitions, destinationDefinitionVersionMap)
    }

    private fun toDestinationDefinitionReadList(
      defs: List<StandardDestinationDefinition>,
      defIdToVersionMap: Map<UUID, ActorDefinitionVersion?>,
    ): DestinationDefinitionReadList {
      val reads =
        defs
          .stream()
          .map { d: StandardDestinationDefinition ->
            buildDestinationDefinitionRead(
              d,
              defIdToVersionMap[d.destinationDefinitionId]!!,
            )
          }.collect(Collectors.toList())
      return DestinationDefinitionReadList().destinationDefinitions(reads)
    }

    @Throws(IOException::class)
    private fun getVersionsForDestinationDefinitions(
      destinationDefinitions: List<StandardDestinationDefinition>,
    ): Map<UUID, ActorDefinitionVersion?> =
      actorDefinitionService
        .getActorDefinitionVersions(
          destinationDefinitions
            .stream()
            .map { obj: StandardDestinationDefinition -> obj.defaultVersionId }
            .collect(Collectors.toList()),
        ).stream()
        .collect(
          Collectors.toMap(
            Function { obj: ActorDefinitionVersion -> obj.actorDefinitionId },
            Function { v: ActorDefinitionVersion? -> v },
          ),
        )

    fun listLatestDestinationDefinitions(): DestinationDefinitionReadList {
      // Swallow exceptions when fetching registry, so we don't hard-fail for airgapped deployments.
      val latestDestinations =
        Exceptions.swallowWithDefault({ remoteDefinitionsProvider.getDestinationDefinitions() }, emptyList())
      val destinationDefs =
        latestDestinations
          .stream()
          .map<StandardDestinationDefinition> { obj: ConnectorRegistryDestinationDefinition -> toStandardDestinationDefinition(obj) }
          .toList()

      val destinationDefVersions =
        latestDestinations.stream().collect(
          Collectors.toMap(
            Function { obj: ConnectorRegistryDestinationDefinition -> obj.destinationDefinitionId },
            Function { destination: ConnectorRegistryDestinationDefinition? ->
              Exceptions.swallowWithDefault(
                {
                  toActorDefinitionVersion(
                    destination!!,
                  )
                },
                null,
              )
            },
          ),
        )

      // filter out any destination definitions with no corresponding version
      val validDestinationDefs =
        destinationDefs
          .stream()
          .filter { d: StandardDestinationDefinition -> destinationDefVersions[d.destinationDefinitionId] != null }
          .toList()

      return toDestinationDefinitionReadList(validDestinationDefs, destinationDefVersions)
    }

    @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
    fun listDestinationDefinitionsForWorkspace(
      workspaceIdActorDefinitionRequestBody: WorkspaceIdActorDefinitionRequestBody,
    ): DestinationDefinitionReadList =
      if (workspaceIdActorDefinitionRequestBody.filterByUsed != null && workspaceIdActorDefinitionRequestBody.filterByUsed) {
        listDestinationDefinitionsUsedByWorkspace(workspaceIdActorDefinitionRequestBody.workspaceId)
      } else {
        listAllowedDestinationDefinitions(workspaceIdActorDefinitionRequestBody.workspaceId)
      }

    @Throws(IOException::class)
    fun listDestinationDefinitionsUsedByWorkspace(workspaceId: UUID): DestinationDefinitionReadList {
      val destinationDefs = destinationService.listDestinationDefinitionsForWorkspace(workspaceId, false)

      val destinationDefVersionMap =
        actorDefinitionVersionHelper.getDestinationVersions(destinationDefs, workspaceId)

      return toDestinationDefinitionReadList(destinationDefs, destinationDefVersionMap)
    }

    @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
    fun listAllowedDestinationDefinitions(workspaceId: UUID): DestinationDefinitionReadList {
      val publicDestinationDefs = destinationService.listPublicDestinationDefinitions(false)

      val workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)
      val publicDestinationEntitlements =
        licenseEntitlementChecker.checkEntitlements(
          workspace.organizationId,
          Entitlement.DESTINATION_CONNECTOR,
          publicDestinationDefs.stream().map { obj: StandardDestinationDefinition -> obj.destinationDefinitionId }.toList(),
        )

      val entitledPublicDestinationDefs =
        publicDestinationDefs
          .stream()
          .filter { d: StandardDestinationDefinition -> publicDestinationEntitlements[d.destinationDefinitionId]!! }

      val destinationDefs =
        Stream
          .concat(
            entitledPublicDestinationDefs,
            destinationService.listGrantedDestinationDefinitions(workspaceId, false).stream(),
          ).toList()

      // Hide destination definitions from the list via feature flag
      val shownDestinationDefs =
        destinationDefs
          .stream()
          .filter { destinationDefinition: StandardDestinationDefinition ->
            !featureFlagClient.boolVariation(
              HideActorDefinitionFromList,
              Multi(
                java.util.List.of(
                  DestinationDefinition(destinationDefinition.destinationDefinitionId),
                  Workspace(workspaceId),
                ),
              ),
            )
          }.toList()

      val sourceDefVersionMap =
        actorDefinitionVersionHelper.getDestinationVersions(shownDestinationDefs, workspaceId)
      return toDestinationDefinitionReadList(shownDestinationDefs, sourceDefVersionMap)
    }

    @Throws(IOException::class)
    fun listPrivateDestinationDefinitions(workspaceIdRequestBody: WorkspaceIdRequestBody): PrivateDestinationDefinitionReadList {
      val standardDestinationDefinitionBooleanMap =
        destinationService.listGrantableDestinationDefinitions(workspaceIdRequestBody.workspaceId, false)
      val destinationDefinitionVersionMap =
        getVersionsForDestinationDefinitions(
          standardDestinationDefinitionBooleanMap.stream().map { obj: Map.Entry<StandardDestinationDefinition, Boolean> -> obj.key }.toList(),
        )
      return toPrivateDestinationDefinitionReadList(standardDestinationDefinitionBooleanMap, destinationDefinitionVersionMap)
    }

    @Throws(IOException::class)
    fun listPublicDestinationDefinitions(): DestinationDefinitionReadList {
      val standardDestinationDefinitions = destinationService.listPublicDestinationDefinitions(false)
      val destinationDefinitionVersionMap = getVersionsForDestinationDefinitions(standardDestinationDefinitions)
      return toDestinationDefinitionReadList(standardDestinationDefinitions, destinationDefinitionVersionMap)
    }

    private fun toPrivateDestinationDefinitionReadList(
      defs: List<Map.Entry<StandardDestinationDefinition, Boolean>>,
      defIdToVersionMap: Map<UUID, ActorDefinitionVersion?>,
    ): PrivateDestinationDefinitionReadList {
      val reads =
        defs
          .stream()
          .map { entry: Map.Entry<StandardDestinationDefinition, Boolean> ->
            PrivateDestinationDefinitionRead()
              .destinationDefinition(buildDestinationDefinitionRead(entry.key, defIdToVersionMap[entry.key.destinationDefinitionId]!!))
              .granted(entry.value)
          }.collect(Collectors.toList())
      return PrivateDestinationDefinitionReadList().destinationDefinitions(reads)
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun getDestinationDefinition(
      destinationDefinitionId: UUID,
      includeTombstone: Boolean,
    ): DestinationDefinitionRead = buildDestinationDefinitionRead(destinationDefinitionId, includeTombstone)

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun getDestinationDefinitionForWorkspace(
      destinationDefinitionIdWithWorkspaceId: DestinationDefinitionIdWithWorkspaceId,
    ): DestinationDefinitionRead {
      val definitionId = destinationDefinitionIdWithWorkspaceId.destinationDefinitionId
      val workspaceId = destinationDefinitionIdWithWorkspaceId.workspaceId
      if (!workspaceService.workspaceCanUseDefinition(definitionId, workspaceId)) {
        throw IdNotFoundKnownException("Cannot find the requested definition with given id for this workspace", definitionId.toString())
      }
      return getDestinationDefinition(definitionId, true)
    }

    @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
    fun getDestinationDefinitionForScope(actorDefinitionIdWithScope: ActorDefinitionIdWithScope): DestinationDefinitionRead {
      val definitionId = actorDefinitionIdWithScope.actorDefinitionId
      val scopeId = actorDefinitionIdWithScope.scopeId
      val scopeType = ScopeType.fromValue(actorDefinitionIdWithScope.scopeType.toString())
      if (!actorDefinitionService.scopeCanUseDefinition(definitionId, scopeId, scopeType.value())) {
        val message = String.format("Cannot find the requested definition with given id for this %s", scopeType)
        throw IdNotFoundKnownException(message, definitionId.toString())
      }
      return getDestinationDefinition(definitionId, true)
    }

    @Throws(IOException::class)
    fun createCustomDestinationDefinition(customDestinationDefinitionCreate: CustomDestinationDefinitionCreate): DestinationDefinitionRead {
      val id = uuidSupplier.get()
      val destinationDefCreate = customDestinationDefinitionCreate.destinationDefinition
      val workspaceId = resolveWorkspaceId(customDestinationDefinitionCreate)
      val actorDefinitionVersion =
        actorDefinitionHandlerHelper
          .defaultDefinitionVersionFromCreate(
            destinationDefCreate.dockerRepository,
            destinationDefCreate.dockerImageTag,
            destinationDefCreate.documentationUrl,
            workspaceId,
          ).withActorDefinitionId(id)

      val destinationDefinition =
        StandardDestinationDefinition()
          .withDestinationDefinitionId(id)
          .withName(destinationDefCreate.name)
          .withIcon(destinationDefCreate.icon)
          .withTombstone(false)
          .withPublic(false)
          .withCustom(true)
          .withResourceRequirements(apiPojoConverters.scopedResourceReqsToInternal(destinationDefCreate.resourceRequirements))

      // legacy call; todo: remove once we drop workspace_id column
      if (customDestinationDefinitionCreate.workspaceId != null) {
        destinationService.writeCustomConnectorMetadata(
          destinationDefinition,
          actorDefinitionVersion,
          customDestinationDefinitionCreate.workspaceId,
          ScopeType.WORKSPACE,
        )
      } else {
        destinationService.writeCustomConnectorMetadata(
          destinationDefinition,
          actorDefinitionVersion,
          customDestinationDefinitionCreate.scopeId,
          ScopeType.fromValue(customDestinationDefinitionCreate.scopeType.toString()),
        )
      }

      return buildDestinationDefinitionRead(destinationDefinition, actorDefinitionVersion)
    }

    private fun resolveWorkspaceId(customDestinationDefinitionCreate: CustomDestinationDefinitionCreate): UUID {
      if (customDestinationDefinitionCreate.workspaceId != null) {
        return customDestinationDefinitionCreate.workspaceId
      }
      if (ScopeType.fromValue(customDestinationDefinitionCreate.scopeType.toString()) == ScopeType.WORKSPACE) {
        return customDestinationDefinitionCreate.scopeId
      }
      throw UnprocessableEntityProblem(
        ProblemMessageData()
          .message(
            String.format(
              "Cannot determine workspace ID for custom destination definition creation: %s",
              customDestinationDefinitionCreate,
            ),
          ),
      )
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
    fun updateDestinationDefinition(destinationDefinitionUpdate: DestinationDefinitionUpdate): DestinationDefinitionRead {
      val isNewConnectorVersionSupported =
        airbyteCompatibleConnectorsValidator.validate(
          destinationDefinitionUpdate.destinationDefinitionId.toString(),
          destinationDefinitionUpdate.dockerImageTag,
        )
      if (!isNewConnectorVersionSupported.isValid) {
        val message =
          if (isNewConnectorVersionSupported.message != null) {
            isNewConnectorVersionSupported.message
          } else {
            String.format(
              "Destination %s can't be updated to version %s cause the version " +
                "is not supported by current platform version",
              destinationDefinitionUpdate.destinationDefinitionId.toString(),
              destinationDefinitionUpdate.dockerImageTag,
            )
          }
        throw BadRequestProblem(message, ProblemMessageData().message(message))
      }

      val currentDestination =
        destinationService
          .getStandardDestinationDefinition(destinationDefinitionUpdate.destinationDefinitionId)
      val currentVersion = actorDefinitionService.getActorDefinitionVersion(currentDestination.defaultVersionId)

      val newDestination = buildDestinationDefinitionUpdate(currentDestination, destinationDefinitionUpdate)

      val newVersion =
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
          currentVersion,
          ActorType.DESTINATION,
          destinationDefinitionUpdate.dockerImageTag,
          currentDestination.custom,
          destinationDefinitionUpdate.workspaceId,
        )

      val breakingChangesForDef =
        actorDefinitionHandlerHelper.getBreakingChanges(newVersion, ActorType.DESTINATION)
      destinationService.writeConnectorMetadata(newDestination, newVersion, breakingChangesForDef)

      val updatedDestinationDefinition =
        destinationService
          .getStandardDestinationDefinition(destinationDefinitionUpdate.destinationDefinitionId)
      supportStateUpdater.updateSupportStatesForDestinationDefinition(updatedDestinationDefinition)
      return buildDestinationDefinitionRead(newDestination, newVersion)
    }

    @VisibleForTesting
    fun buildDestinationDefinitionUpdate(
      currentDestination: StandardDestinationDefinition,
      destinationDefinitionUpdate: DestinationDefinitionUpdate,
    ): StandardDestinationDefinition {
      val updatedResourceReqs =
        if (destinationDefinitionUpdate.resourceRequirements != null) {
          apiPojoConverters.scopedResourceReqsToInternal(destinationDefinitionUpdate.resourceRequirements)
        } else {
          currentDestination.resourceRequirements
        }

      val newDestination =
        StandardDestinationDefinition()
          .withDestinationDefinitionId(currentDestination.destinationDefinitionId)
          .withName(currentDestination.name)
          .withIcon(currentDestination.icon)
          .withIconUrl(currentDestination.iconUrl)
          .withTombstone(currentDestination.tombstone)
          .withPublic(currentDestination.public)
          .withCustom(currentDestination.custom)
          .withMetrics(currentDestination.metrics)
          .withResourceRequirements(updatedResourceReqs)

      if (destinationDefinitionUpdate.name != null && currentDestination.custom) {
        newDestination.withName(destinationDefinitionUpdate.name)
      }

      return newDestination
    }

    @Throws(
      JsonValidationException::class,
      ConfigNotFoundException::class,
      IOException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun deleteDestinationDefinition(destinationDefinitionId: UUID) {
      // "delete" all destinations associated with the destination definition as well. This will cascade
      // to connections that depend on any deleted
      // destinations. Delete destinations first in case a failure occurs mid-operation.

      val persistedDestinationDefinition =
        destinationService.getStandardDestinationDefinition(destinationDefinitionId)

      for (destinationRead in destinationHandler
        .listDestinationsForDestinationDefinition(destinationDefinitionId)
        .destinations) {
        destinationHandler.deleteDestination(destinationRead)
      }

      persistedDestinationDefinition.withTombstone(true)
      destinationService.updateStandardDestinationDefinition(persistedDestinationDefinition)
    }

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun grantDestinationDefinitionToWorkspaceOrOrganization(
      actorDefinitionIdWithScope: ActorDefinitionIdWithScope,
    ): PrivateDestinationDefinitionRead {
      val standardDestinationDefinition =
        destinationService.getStandardDestinationDefinition(actorDefinitionIdWithScope.actorDefinitionId)
      val actorDefinitionVersion =
        actorDefinitionService.getActorDefinitionVersion(standardDestinationDefinition.defaultVersionId)
      actorDefinitionService.writeActorDefinitionWorkspaceGrant(
        actorDefinitionIdWithScope.actorDefinitionId,
        actorDefinitionIdWithScope.scopeId,
        ScopeType.fromValue(actorDefinitionIdWithScope.scopeType.toString()),
      )
      return PrivateDestinationDefinitionRead()
        .destinationDefinition(buildDestinationDefinitionRead(standardDestinationDefinition, actorDefinitionVersion))
        .granted(true)
    }

    @Throws(IOException::class)
    fun revokeDestinationDefinition(actorDefinitionIdWithScope: ActorDefinitionIdWithScope) {
      actorDefinitionService.deleteActorDefinitionWorkspaceGrant(
        actorDefinitionIdWithScope.actorDefinitionId,
        actorDefinitionIdWithScope.scopeId,
        ScopeType.fromValue(actorDefinitionIdWithScope.scopeType.toString()),
      )
    }
  }
