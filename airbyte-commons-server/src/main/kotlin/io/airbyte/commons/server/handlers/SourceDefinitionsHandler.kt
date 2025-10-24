/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.ActorDefinitionIdWithScope
import io.airbyte.api.model.generated.CustomSourceDefinitionCreate
import io.airbyte.api.model.generated.PrivateSourceDefinitionRead
import io.airbyte.api.model.generated.PrivateSourceDefinitionReadList
import io.airbyte.api.model.generated.ScopedResourceRequirements
import io.airbyte.api.model.generated.SourceDefinitionIdWithWorkspaceId
import io.airbyte.api.model.generated.SourceDefinitionRead
import io.airbyte.api.model.generated.SourceDefinitionRead.SourceTypeEnum
import io.airbyte.api.model.generated.SourceDefinitionReadList
import io.airbyte.api.model.generated.SourceDefinitionUpdate
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
import io.airbyte.config.ConnectorRegistrySourceDefinition
import io.airbyte.config.ScopeType
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.helpers.ConnectorRegistryConverters.toActorDefinitionVersion
import io.airbyte.config.helpers.ConnectorRegistryConverters.toStandardSourceDefinition
import io.airbyte.config.init.AirbyteCompatibleConnectorsValidator
import io.airbyte.config.init.SupportStateUpdater
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.specs.RemoteDefinitionsProvider
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.HideActorDefinitionFromList
import io.airbyte.featureflag.Multi
import io.airbyte.featureflag.SourceDefinition
import io.airbyte.featureflag.Workspace
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Inject
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
 * OperationsHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
open class SourceDefinitionsHandler
  @Inject
  constructor(
    private val actorDefinitionService: ActorDefinitionService,
    @param:Named("uuidGenerator") private val uuidSupplier: Supplier<UUID>,
    private val actorDefinitionHandlerHelper: ActorDefinitionHandlerHelper,
    private val remoteDefinitionsProvider: RemoteDefinitionsProvider,
    private val sourceHandler: SourceHandler,
    private val supportStateUpdater: SupportStateUpdater,
    private val featureFlagClient: FeatureFlagClient,
    private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
    private val airbyteCompatibleConnectorsValidator: AirbyteCompatibleConnectorsValidator,
    private val sourceService: SourceService,
    private val workspaceService: WorkspaceService,
    private val licenseEntitlementChecker: LicenseEntitlementChecker,
    private val apiPojoConverters: ApiPojoConverters,
  ) {
    fun buildSourceDefinitionRead(
      sourceDefinitionId: UUID,
      includeTombstone: Boolean,
    ): SourceDefinitionRead {
      val sourceDefinition = sourceService.getStandardSourceDefinition(sourceDefinitionId, includeTombstone)
      val sourceVersion = actorDefinitionService.getActorDefinitionVersion(sourceDefinition.defaultVersionId)
      return buildSourceDefinitionRead(sourceDefinition, sourceVersion)
    }

    @VisibleForTesting
    fun buildSourceDefinitionRead(
      standardSourceDefinition: StandardSourceDefinition,
      sourceVersion: ActorDefinitionVersion,
    ): SourceDefinitionRead {
      try {
        return SourceDefinitionRead()
          .sourceDefinitionId(standardSourceDefinition.sourceDefinitionId)
          .name(standardSourceDefinition.name)
          .sourceType(getSourceType(standardSourceDefinition))
          .dockerRepository(sourceVersion.dockerRepository)
          .dockerImageTag(sourceVersion.dockerImageTag)
          .documentationUrl(URI(sourceVersion.documentationUrl))
          .icon(standardSourceDefinition.iconUrl)
          .protocolVersion(sourceVersion.protocolVersion)
          .supportLevel(apiPojoConverters.toApiSupportLevel(sourceVersion.supportLevel))
          .releaseStage(apiPojoConverters.toApiReleaseStage(sourceVersion.releaseStage))
          .releaseDate(apiPojoConverters.toLocalDate(sourceVersion.releaseDate))
          .lastPublished(apiPojoConverters.toOffsetDateTime(sourceVersion.lastPublished))
          .cdkVersion(sourceVersion.cdkVersion)
          .metrics(standardSourceDefinition.metrics)
          .custom(standardSourceDefinition.custom)
          .enterprise(standardSourceDefinition.enterprise)
          .resourceRequirements(apiPojoConverters.scopedResourceReqsToApi(standardSourceDefinition.resourceRequirements))
          .maxSecondsBetweenMessages(standardSourceDefinition.maxSecondsBetweenMessages)
          .language(sourceVersion.language)
      } catch (e: URISyntaxException) {
        throw InternalServerKnownException("Unable to process retrieved latest source definitions list", e)
      } catch (e: NullPointerException) {
        throw InternalServerKnownException("Unable to process retrieved latest source definitions list", e)
      }
    }

    fun listSourceDefinitions(): SourceDefinitionReadList {
      val standardSourceDefinitions = sourceService.listStandardSourceDefinitions(false)
      val sourceDefinitionVersionMap = getVersionsForSourceDefinitions(standardSourceDefinitions)
      return toSourceDefinitionReadList(standardSourceDefinitions, sourceDefinitionVersionMap)
    }

    private fun getVersionsForSourceDefinitions(sourceDefinitions: List<StandardSourceDefinition>): Map<UUID, ActorDefinitionVersion?> =
      actorDefinitionService
        .getActorDefinitionVersions(
          sourceDefinitions
            .stream()
            .map { obj: StandardSourceDefinition -> obj.defaultVersionId }
            .collect(Collectors.toList()),
        ).stream()
        .collect(
          Collectors.toMap(
            Function { obj: ActorDefinitionVersion -> obj.actorDefinitionId },
            Function { v: ActorDefinitionVersion? -> v },
          ),
        )

    private fun toSourceDefinitionReadList(
      defs: List<StandardSourceDefinition>,
      defIdToVersionMap: Map<UUID, ActorDefinitionVersion?>,
    ): SourceDefinitionReadList {
      val reads =
        defs
          .stream()
          .map { d: StandardSourceDefinition ->
            buildSourceDefinitionRead(
              d,
              defIdToVersionMap[d.sourceDefinitionId]!!,
            )
          }.collect(Collectors.toList())
      return SourceDefinitionReadList().sourceDefinitions(reads)
    }

    fun listLatestSourceDefinitions(): SourceDefinitionReadList {
      // Swallow exceptions when fetching registry, so we don't hard-fail for airgapped deployments.
      val latestSources =
        Exceptions.swallowWithDefault({ remoteDefinitionsProvider.getSourceDefinitions() }, emptyList())
      val sourceDefs =
        latestSources
          .stream()
          .map<StandardSourceDefinition> { obj: ConnectorRegistrySourceDefinition -> toStandardSourceDefinition(obj) }
          .toList()

      val sourceDefVersionMap =
        latestSources.stream().collect(
          Collectors.toMap(
            Function { obj: ConnectorRegistrySourceDefinition -> obj.sourceDefinitionId },
            Function { destination: ConnectorRegistrySourceDefinition? ->
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
      val validSourceDefs =
        sourceDefs
          .stream()
          .filter { s: StandardSourceDefinition -> sourceDefVersionMap[s.sourceDefinitionId] != null }
          .toList()

      return toSourceDefinitionReadList(validSourceDefs, sourceDefVersionMap)
    }

    fun listSourceDefinitionsForWorkspace(workspaceIdActorDefinitionRequestBody: WorkspaceIdActorDefinitionRequestBody): SourceDefinitionReadList =
      if (workspaceIdActorDefinitionRequestBody.filterByUsed != null && workspaceIdActorDefinitionRequestBody.filterByUsed) {
        listSourceDefinitionsUsedByWorkspace(workspaceIdActorDefinitionRequestBody.workspaceId)
      } else {
        listAllowedSourceDefinitions(workspaceIdActorDefinitionRequestBody.workspaceId)
      }

    fun listSourceDefinitionsUsedByWorkspace(workspaceId: UUID): SourceDefinitionReadList {
      val sourceDefs = sourceService.listSourceDefinitionsForWorkspace(workspaceId, false)

      val sourceDefVersionMap =
        actorDefinitionVersionHelper.getSourceVersions(sourceDefs, workspaceId)

      return toSourceDefinitionReadList(sourceDefs, sourceDefVersionMap)
    }

    fun listAllowedSourceDefinitions(workspaceId: UUID): SourceDefinitionReadList {
      val publicSourceDefs = sourceService.listPublicSourceDefinitions(false)

      val workspace = workspaceService.getStandardWorkspaceNoSecrets(workspaceId, true)
      val publicSourceEntitlements =
        licenseEntitlementChecker.checkEntitlements(
          workspace.organizationId,
          Entitlement.SOURCE_CONNECTOR,
          publicSourceDefs.stream().map { obj: StandardSourceDefinition -> obj.sourceDefinitionId }.toList(),
        )

      val entitledPublicSourceDefs =
        publicSourceDefs
          .stream()
          .filter { s: StandardSourceDefinition -> publicSourceEntitlements[s.sourceDefinitionId]!! }

      val sourceDefs =
        Stream
          .concat(
            entitledPublicSourceDefs,
            sourceService.listGrantedSourceDefinitions(workspaceId, false).stream(),
          ).toList()

      // Hide source definitions from the list via feature flag
      val shownSourceDefs =
        sourceDefs
          .stream()
          .filter { sourceDefinition: StandardSourceDefinition ->
            !featureFlagClient.boolVariation(
              HideActorDefinitionFromList,
              Multi(java.util.List.of(SourceDefinition(sourceDefinition.sourceDefinitionId), Workspace(workspaceId))),
            )
          }.toList()

      val sourceDefVersionMap =
        actorDefinitionVersionHelper.getSourceVersions(shownSourceDefs, workspaceId)
      return toSourceDefinitionReadList(shownSourceDefs, sourceDefVersionMap)
    }

    fun listPrivateSourceDefinitions(workspaceIdRequestBody: WorkspaceIdRequestBody): PrivateSourceDefinitionReadList {
      val standardSourceDefinitionBooleanMap =
        sourceService.listGrantableSourceDefinitions(workspaceIdRequestBody.workspaceId, false)
      val sourceDefinitionVersionMap =
        getVersionsForSourceDefinitions(
          standardSourceDefinitionBooleanMap.stream().map { obj: Map.Entry<StandardSourceDefinition, Boolean> -> obj.key }.toList(),
        )
      return toPrivateSourceDefinitionReadList(standardSourceDefinitionBooleanMap, sourceDefinitionVersionMap)
    }

    fun listPublicSourceDefinitions(): SourceDefinitionReadList {
      val standardSourceDefinitions = sourceService.listPublicSourceDefinitions(false)
      val sourceDefinitionVersionMap = getVersionsForSourceDefinitions(standardSourceDefinitions)
      return toSourceDefinitionReadList(standardSourceDefinitions, sourceDefinitionVersionMap)
    }

    private fun toPrivateSourceDefinitionReadList(
      defs: List<Map.Entry<StandardSourceDefinition, Boolean>>,
      defIdToVersionMap: Map<UUID, ActorDefinitionVersion?>,
    ): PrivateSourceDefinitionReadList {
      val reads =
        defs
          .stream()
          .map { entry: Map.Entry<StandardSourceDefinition, Boolean> ->
            PrivateSourceDefinitionRead()
              .sourceDefinition(buildSourceDefinitionRead(entry.key, defIdToVersionMap[entry.key.sourceDefinitionId]!!))
              .granted(entry.value)
          }.collect(Collectors.toList())
      return PrivateSourceDefinitionReadList().sourceDefinitions(reads)
    }

    fun getSourceDefinition(
      sourceDefinitionId: UUID,
      includeTombstone: Boolean,
    ): SourceDefinitionRead = buildSourceDefinitionRead(sourceDefinitionId, includeTombstone)

    fun getSourceDefinitionForScope(actorDefinitionIdWithScope: ActorDefinitionIdWithScope): SourceDefinitionRead {
      val definitionId = actorDefinitionIdWithScope.actorDefinitionId
      val scopeId = actorDefinitionIdWithScope.scopeId
      val scopeType = ScopeType.fromValue(actorDefinitionIdWithScope.scopeType.toString())
      if (!actorDefinitionService.scopeCanUseDefinition(definitionId, scopeId, scopeType.value())) {
        val message = String.format("Cannot find the requested definition with given id for this %s", scopeType)
        throw IdNotFoundKnownException(message, definitionId.toString())
      }
      return getSourceDefinition(definitionId, true)
    }

    fun getSourceDefinitionForWorkspace(sourceDefinitionIdWithWorkspaceId: SourceDefinitionIdWithWorkspaceId): SourceDefinitionRead {
      val definitionId = sourceDefinitionIdWithWorkspaceId.sourceDefinitionId
      val workspaceId = sourceDefinitionIdWithWorkspaceId.workspaceId
      if (!workspaceService.workspaceCanUseDefinition(definitionId, workspaceId)) {
        throw IdNotFoundKnownException("Cannot find the requested definition with given id for this workspace", definitionId.toString())
      }
      return getSourceDefinition(definitionId, true)
    }

    fun createCustomSourceDefinition(customSourceDefinitionCreate: CustomSourceDefinitionCreate): SourceDefinitionRead {
      val id = uuidSupplier.get()
      val sourceDefinitionCreate = customSourceDefinitionCreate.sourceDefinition
      val workspaceId = resolveWorkspaceId(customSourceDefinitionCreate)
      val actorDefinitionVersion =
        actorDefinitionHandlerHelper
          .defaultDefinitionVersionFromCreate(
            sourceDefinitionCreate.dockerRepository,
            sourceDefinitionCreate.dockerImageTag,
            sourceDefinitionCreate.documentationUrl,
            workspaceId,
          ).withActorDefinitionId(id)

      val scopeId = customSourceDefinitionCreate.workspaceId ?: customSourceDefinitionCreate.scopeId
      val scopeType =
        if (customSourceDefinitionCreate.workspaceId !=
          null
        ) {
          ScopeType.WORKSPACE
        } else {
          ScopeType.fromValue(customSourceDefinitionCreate.scopeType.toString())
        }

      val sourceDefinition =
        saveCustomSourceDefinition(
          name = sourceDefinitionCreate.name,
          icon = sourceDefinitionCreate.icon,
          actorDefinitionVersion = actorDefinitionVersion,
          resourceRequirements = sourceDefinitionCreate.resourceRequirements,
          scopeId = scopeId,
          scopeType = scopeType,
        )

      return buildSourceDefinitionRead(sourceDefinition, actorDefinitionVersion)
    }

    fun saveCustomSourceDefinition(
      name: String,
      icon: String?,
      actorDefinitionVersion: ActorDefinitionVersion,
      resourceRequirements: ScopedResourceRequirements?,
      scopeId: UUID,
      scopeType: ScopeType,
    ): StandardSourceDefinition {
      val sourceDefinition =
        StandardSourceDefinition()
          .withSourceDefinitionId(actorDefinitionVersion.actorDefinitionId)
          .withName(name)
          .withIcon(icon)
          .withTombstone(false)
          .withPublic(false)
          .withCustom(true)
          .withResourceRequirements(apiPojoConverters.scopedResourceReqsToInternal(resourceRequirements))

      sourceService.writeCustomConnectorMetadata(
        sourceDefinition = sourceDefinition,
        defaultVersion = actorDefinitionVersion,
        scopeId = scopeId,
        scopeType = scopeType,
      )

      return sourceDefinition
    }

    private fun resolveWorkspaceId(customSourceDefinitionCreate: CustomSourceDefinitionCreate): UUID {
      if (customSourceDefinitionCreate.workspaceId != null) {
        return customSourceDefinitionCreate.workspaceId
      }
      if (ScopeType.fromValue(customSourceDefinitionCreate.scopeType.toString()) == ScopeType.WORKSPACE) {
        return customSourceDefinitionCreate.scopeId
      }
      throw UnprocessableEntityProblem(
        ProblemMessageData()
          .message(String.format("Cannot determine workspace ID for custom source definition creation: %s", customSourceDefinitionCreate)),
      )
    }

    fun updateSourceDefinition(sourceDefinitionUpdate: SourceDefinitionUpdate): SourceDefinitionRead {
      actorDefinitionHandlerHelper.validateVersionSupport(
        actorDefinitionId = sourceDefinitionUpdate.sourceDefinitionId,
        connectorVersion = sourceDefinitionUpdate.dockerImageTag,
        actorType = ActorType.SOURCE,
      )

      val currentSource = sourceService.getStandardSourceDefinition(sourceDefinitionUpdate.sourceDefinitionId)
      val currentVersion = actorDefinitionService.getActorDefinitionVersion(currentSource.defaultVersionId)

      val newVersion =
        actorDefinitionHandlerHelper.defaultDefinitionVersionFromUpdate(
          currentVersion,
          ActorType.SOURCE,
          sourceDefinitionUpdate.dockerImageTag,
          currentSource.custom,
          sourceDefinitionUpdate.workspaceId,
        )
      val newSource = updateSourceDefinition(newVersion, sourceDefinitionUpdate.name, sourceDefinitionUpdate.resourceRequirements)

      return buildSourceDefinitionRead(newSource, newVersion)
    }

    fun updateSourceDefinition(
      newVersion: ActorDefinitionVersion,
      name: String?,
      resourceRequirements: ScopedResourceRequirements?,
    ): StandardSourceDefinition {
      val currentSource = sourceService.getStandardSourceDefinition(newVersion.actorDefinitionId)
      val newSource = buildSourceDefinitionUpdate(currentSource, name, resourceRequirements)
      val breakingChangesForDef = actorDefinitionHandlerHelper.getBreakingChanges(newVersion, ActorType.SOURCE)
      sourceService.writeConnectorMetadata(newSource, newVersion, breakingChangesForDef)

      val updatedSourceDefinition = sourceService.getStandardSourceDefinition(newSource.sourceDefinitionId)
      supportStateUpdater.updateSupportStatesForSourceDefinition(updatedSourceDefinition)

      return updatedSourceDefinition
    }

    @VisibleForTesting
    fun buildSourceDefinitionUpdate(
      currentSourceDefinition: StandardSourceDefinition,
      name: String?,
      resourceRequirements: ScopedResourceRequirements?,
    ): StandardSourceDefinition {
      val updatedResourceReqs =
        if (resourceRequirements != null) {
          apiPojoConverters.scopedResourceReqsToInternal(resourceRequirements)
        } else {
          currentSourceDefinition.resourceRequirements
        }

      val newSource =
        StandardSourceDefinition()
          .withSourceDefinitionId(currentSourceDefinition.sourceDefinitionId)
          .withName(currentSourceDefinition.name)
          .withIcon(currentSourceDefinition.icon)
          .withIconUrl(currentSourceDefinition.iconUrl)
          .withTombstone(currentSourceDefinition.tombstone)
          .withPublic(currentSourceDefinition.public)
          .withCustom(currentSourceDefinition.custom)
          .withMetrics(currentSourceDefinition.metrics)
          .withMaxSecondsBetweenMessages(currentSourceDefinition.maxSecondsBetweenMessages)
          .withResourceRequirements(updatedResourceReqs)

      if (name != null && currentSourceDefinition.custom) {
        newSource.withName(name)
      }

      return newSource
    }

    fun deleteSourceDefinition(sourceDefinitionId: UUID) {
      // "delete" all sources associated with the source definition as well. This will cascade to
      // connections that depend on any deleted sources.
      // Delete sources first in case a failure occurs mid-operation.

      val persistedSourceDefinition =
        sourceService.getStandardSourceDefinition(sourceDefinitionId)

      for (sourceRead in sourceHandler.listSourcesForSourceDefinition(sourceDefinitionId).sources) {
        sourceHandler.deleteSource(sourceRead)
      }

      persistedSourceDefinition.withTombstone(true)
      sourceService.updateStandardSourceDefinition(persistedSourceDefinition)
    }

    fun grantSourceDefinitionToWorkspaceOrOrganization(actorDefinitionIdWithScope: ActorDefinitionIdWithScope): PrivateSourceDefinitionRead {
      val standardSourceDefinition =
        sourceService.getStandardSourceDefinition(actorDefinitionIdWithScope.actorDefinitionId)
      val actorDefinitionVersion =
        actorDefinitionService.getActorDefinitionVersion(standardSourceDefinition.defaultVersionId)
      actorDefinitionService.writeActorDefinitionWorkspaceGrant(
        actorDefinitionIdWithScope.actorDefinitionId,
        actorDefinitionIdWithScope.scopeId,
        ScopeType.fromValue(actorDefinitionIdWithScope.scopeType.toString()),
      )
      return PrivateSourceDefinitionRead()
        .sourceDefinition(buildSourceDefinitionRead(standardSourceDefinition, actorDefinitionVersion))
        .granted(true)
    }

    fun revokeSourceDefinition(actorDefinitionIdWithScope: ActorDefinitionIdWithScope) {
      actorDefinitionService.deleteActorDefinitionWorkspaceGrant(
        actorDefinitionIdWithScope.actorDefinitionId,
        actorDefinitionIdWithScope.scopeId,
        ScopeType.fromValue(actorDefinitionIdWithScope.scopeType.toString()),
      )
    }

    companion object {
      private fun getSourceType(standardSourceDefinition: StandardSourceDefinition): SourceTypeEnum? {
        if (standardSourceDefinition.sourceType == null) {
          return null
        }
        return SourceTypeEnum.fromValue(standardSourceDefinition.sourceType.value())
      }
    }
  }
