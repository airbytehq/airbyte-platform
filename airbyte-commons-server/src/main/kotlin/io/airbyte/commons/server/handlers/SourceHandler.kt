/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.Lists
import datadog.trace.api.Trace
import io.airbyte.api.model.generated.ActorCatalogWithUpdatedAt
import io.airbyte.api.model.generated.ActorDefinitionVersionBreakingChanges
import io.airbyte.api.model.generated.ActorListCursorPaginatedRequestBody
import io.airbyte.api.model.generated.ActorStatus
import io.airbyte.api.model.generated.ActorType
import io.airbyte.api.model.generated.CompleteOAuthResponse
import io.airbyte.api.model.generated.ConnectionRead
import io.airbyte.api.model.generated.DiscoverCatalogResult
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.model.generated.PartialSourceUpdate
import io.airbyte.api.model.generated.ScopedResourceRequirements
import io.airbyte.api.model.generated.SourceCreate
import io.airbyte.api.model.generated.SourceDiscoverSchemaWriteRequestBody
import io.airbyte.api.model.generated.SourceIdRequestBody
import io.airbyte.api.model.generated.SourceRead
import io.airbyte.api.model.generated.SourceReadList
import io.airbyte.api.model.generated.SourceSearch
import io.airbyte.api.model.generated.SourceSnippetRead
import io.airbyte.api.model.generated.SourceUpdate
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.converters.ConfigurationUpdate
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper.setSecretsInConnectionConfiguration
import io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper.validateNoSecretsInConfiguration
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.JobStatus
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.config.secrets.SecretCoordinate.Companion.fromFullCoordinate
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers.configWithTextualSecretPlaceholders
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers.processConfigSecrets
import io.airbyte.config.secrets.SecretsRepositoryReader
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.config.secrets.toInlined
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.CatalogService
import io.airbyte.data.services.PartialUserConfigService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.shared.DEFAULT_PAGE_SIZE
import io.airbyte.data.services.shared.ResourcesQueryPaginated
import io.airbyte.data.services.shared.buildFilters
import io.airbyte.data.services.shared.parseSortKey
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.OrganizationId
import io.airbyte.domain.models.SecretStorageId
import io.airbyte.domain.models.UserId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.domain.services.entitlements.ConnectorConfigEntitlementService
import io.airbyte.domain.services.secrets.SecretPersistenceService
import io.airbyte.domain.services.secrets.SecretReferenceService
import io.airbyte.domain.services.secrets.SecretStorageService
import io.airbyte.metrics.lib.ApmTraceUtils.addTagsToTrace
import io.airbyte.metrics.lib.MetricTags.SOURCE_ID
import io.airbyte.metrics.lib.MetricTags.WORKSPACE_ID
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.IOException
import java.util.Map
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier

/**
 * SourceHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
class SourceHandler
  @VisibleForTesting
  constructor(
    private val catalogService: CatalogService,
    private val secretsRepositoryReader: SecretsRepositoryReader,
    private val validator: JsonSchemaValidator,
    private val connectionsHandler: ConnectionsHandler,
    @param:Named("uuidGenerator") private val uuidGenerator: Supplier<UUID>,
    @param:Named("jsonSecretsProcessorWithCopy") private val secretsProcessor: JsonSecretsProcessor,
    private val configurationUpdate: ConfigurationUpdate,
    private val oAuthConfigSupplier: OAuthConfigSupplier,
    private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
    private val sourceService: SourceService,
    private val workspaceHelper: WorkspaceHelper,
    private val secretPersistenceService: SecretPersistenceService,
    private val actorDefinitionHandlerHelper: ActorDefinitionHandlerHelper,
    private val actorDefinitionVersionUpdater: ActorDefinitionVersionUpdater,
    private val licenseEntitlementChecker: LicenseEntitlementChecker,
    private val connectorConfigEntitlementService: ConnectorConfigEntitlementService,
    private val catalogConverter: CatalogConverter,
    private val apiPojoConverters: ApiPojoConverters,
    private val airbyteEdition: AirbyteEdition,
    private val secretsRepositoryWriter: SecretsRepositoryWriter,
    private val secretStorageService: SecretStorageService,
    private val secretReferenceService: SecretReferenceService,
    private val currentUserService: CurrentUserService,
    private val partialUserConfigService: PartialUserConfigService,
  ) {
    @Throws(
      JsonValidationException::class,
      ConfigNotFoundException::class,
      IOException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun createSourceWithOptionalSecret(sourceCreate: SourceCreate): SourceRead {
      if (sourceCreate.secretId != null && !sourceCreate.secretId.isBlank()) {
        val hydratedSecret = hydrateOAuthResponseSecret(sourceCreate.secretId, sourceCreate.workspaceId)
        val spec =
          getSourceVersionForWorkspaceId(sourceCreate.sourceDefinitionId, sourceCreate.workspaceId).spec
        // add OAuth Response data to connection configuration
        sourceCreate.connectionConfiguration =
          setSecretsInConnectionConfiguration(
            spec,
            hydratedSecret,
            sourceCreate.connectionConfiguration,
          )
      }
      return createSource(sourceCreate)
    }

    @Trace
    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun updateSourceWithOptionalSecret(partialSourceUpdate: PartialSourceUpdate): SourceRead {
      val spec = getSourceVersionForSourceId(partialSourceUpdate.sourceId).spec
      addTagsToTrace(
        Map.of<String?, String?>(
          SOURCE_ID,
          partialSourceUpdate.sourceId.toString(),
        ),
      )
      if (partialSourceUpdate.secretId != null && !partialSourceUpdate.secretId.isBlank()) {
        val sourceConnection: SourceConnection
        try {
          sourceConnection = sourceService.getSourceConnection(partialSourceUpdate.sourceId)
        } catch (e: ConfigNotFoundException) {
          throw ConfigNotFoundException(e.type, e.configId)
        }
        val hydratedSecret = hydrateOAuthResponseSecret(partialSourceUpdate.secretId, sourceConnection.workspaceId)
        // add OAuth Response data to connection configuration
        partialSourceUpdate.connectionConfiguration =
          setSecretsInConnectionConfiguration(
            spec,
            hydratedSecret,
            Optional
              .ofNullable(partialSourceUpdate.connectionConfiguration)
              .orElse(Jsons.emptyObject()),
          )
        addTagsToTrace(
          Map.of(
            "oauth_secret",
            true,
          ),
        )
      } else {
        // We aren't using a secret to update the source so no server provided credentials should have been
        // passed in.
        validateNoSecretsInConfiguration(spec, partialSourceUpdate.connectionConfiguration)
      }
      return partialUpdateSource(partialSourceUpdate)
    }

    @VisibleForTesting
    @Throws(
      ConfigNotFoundException::class,
      IOException::class,
      JsonValidationException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun createSource(sourceCreate: SourceCreate): SourceRead {
      if (sourceCreate.resourceAllocation != null && airbyteEdition == AirbyteEdition.CLOUD) {
        throw BadRequestException(String.format("Setting resource allocation is not permitted on %s", airbyteEdition))
      }

      // validate configuration
      val sourceVersion =
        getSourceVersionForWorkspaceId(
          sourceCreate.sourceDefinitionId,
          sourceCreate.workspaceId,
        )
      val spec = sourceVersion.spec
      validateSource(spec, sourceCreate.connectionConfiguration)

      // persist
      val sourceId = uuidGenerator.get()
      persistSourceConnection(
        if (sourceCreate.name != null) sourceCreate.name else "default",
        sourceCreate.sourceDefinitionId,
        sourceCreate.workspaceId,
        sourceId,
        false,
        sourceCreate.connectionConfiguration,
        sourceVersion,
        sourceCreate.resourceAllocation,
      )

      // read configuration from db
      return buildSourceRead(sourceService.getSourceConnection(sourceId), spec)
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun partialUpdateSource(partialSourceUpdate: PartialSourceUpdate): SourceRead {
      if (partialSourceUpdate.resourceAllocation != null && airbyteEdition == AirbyteEdition.CLOUD) {
        throw BadRequestException(String.format("Setting resource allocation is not permitted on %s", airbyteEdition))
      }

      val sourceId = partialSourceUpdate.sourceId

      val updatedSource =
        configurationUpdate
          .partialSource(
            sourceId,
            partialSourceUpdate.name,
            partialSourceUpdate.connectionConfiguration,
          )
      val sourceVersion = getSourceVersionForSourceId(sourceId)
      val spec = sourceVersion.spec

      validateSourceUpdate(partialSourceUpdate.connectionConfiguration, updatedSource, spec)

      addTagsToTrace(Map.of<String?, String?>(WORKSPACE_ID, updatedSource.workspaceId.toString()))

      // persist
      persistSourceConnection(
        updatedSource.name,
        updatedSource.sourceDefinitionId,
        updatedSource.workspaceId,
        updatedSource.sourceId,
        updatedSource.tombstone,
        updatedSource.configuration,
        sourceVersion,
        partialSourceUpdate.resourceAllocation,
      )

      // read configuration from db
      return buildSourceRead(sourceService.getSourceConnection(sourceId), spec)
    }

    @Trace
    @Throws(
      ConfigNotFoundException::class,
      IOException::class,
      JsonValidationException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun updateSource(sourceUpdate: SourceUpdate): SourceRead {
      if (sourceUpdate.resourceAllocation != null && airbyteEdition == AirbyteEdition.CLOUD) {
        throw BadRequestException(String.format("Setting resource allocation is not permitted on %s", airbyteEdition))
      }

      val sourceId = sourceUpdate.sourceId
      val updatedSource =
        configurationUpdate
          .source(sourceId, sourceUpdate.name, sourceUpdate.connectionConfiguration)
      val sourceVersion = getSourceVersionForSourceId(sourceId)
      val spec = sourceVersion.spec

      validateSourceUpdate(sourceUpdate.connectionConfiguration, updatedSource, spec)

      addTagsToTrace(
        Map.of<String?, String?>(
          WORKSPACE_ID,
          updatedSource.workspaceId.toString(),
          SOURCE_ID,
          sourceId.toString(),
        ),
      )

      // persist
      persistSourceConnection(
        updatedSource.name,
        updatedSource.sourceDefinitionId,
        updatedSource.workspaceId,
        updatedSource.sourceId,
        updatedSource.tombstone,
        updatedSource.configuration,
        sourceVersion,
        sourceUpdate.resourceAllocation,
      )

      // read configuration from db
      return buildSourceRead(sourceService.getSourceConnection(sourceId), spec)
    }

    /**
     * Upgrades the source to the source definition's default version.
     *
     * @param sourceIdRequestBody - ID of the source to upgrade
     */
    @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
    fun upgradeSourceVersion(sourceIdRequestBody: SourceIdRequestBody) {
      val sourceConnection = sourceService.getSourceConnection(sourceIdRequestBody.sourceId)
      val sourceDefinition = sourceService.getStandardSourceDefinition(sourceConnection.sourceDefinitionId)
      actorDefinitionVersionUpdater.upgradeActorVersion(sourceConnection, sourceDefinition)
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun getSource(sourceIdRequestBody: SourceIdRequestBody): SourceRead = getSource(sourceIdRequestBody, false)

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun getSource(
      sourceIdRequestBody: SourceIdRequestBody,
      includeSecretCoordinates: Boolean,
    ): SourceRead = buildSourceRead(sourceIdRequestBody.sourceId, includeSecretCoordinates)

    @Throws(IOException::class)
    fun getMostRecentSourceActorCatalogWithUpdatedAt(sourceIdRequestBody: SourceIdRequestBody): ActorCatalogWithUpdatedAt {
      val actorCatalog =
        catalogService.getMostRecentSourceActorCatalog(sourceIdRequestBody.sourceId)
      return if (actorCatalog.isEmpty) {
        ActorCatalogWithUpdatedAt()
      } else {
        ActorCatalogWithUpdatedAt().updatedAt(actorCatalog.get().updatedAt).catalog(actorCatalog.get().catalog)
      }
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun listSourcesForWorkspace(actorListCursorPaginatedRequestBody: ActorListCursorPaginatedRequestBody): SourceReadList {
      val filters = actorListCursorPaginatedRequestBody.filters
      val pageSize =
        if (actorListCursorPaginatedRequestBody.pageSize != null) {
          actorListCursorPaginatedRequestBody.pageSize
        } else {
          DEFAULT_PAGE_SIZE
        }

      // Parse sort key to extract field and direction
      val sortKeyInfo =
        parseSortKey(actorListCursorPaginatedRequestBody.sortKey, ActorType.SOURCE)
      val internalSortKey = sortKeyInfo.sortKey
      val ascending = sortKeyInfo.ascending
      val actorFilters = buildFilters(filters)

      val cursorPagination =
        sourceService.buildCursorPagination(
          actorListCursorPaginatedRequestBody.cursor,
          internalSortKey,
          actorFilters,
          ascending,
          pageSize,
        )!!

      val numSources =
        sourceService.countWorkspaceSourcesFiltered(
          actorListCursorPaginatedRequestBody.workspaceId,
          cursorPagination,
        )

      val sourceConnectionsWithCount =
        sourceService.listWorkspaceSourceConnectionsWithCounts(actorListCursorPaginatedRequestBody.workspaceId, cursorPagination)

      val sourceReads: MutableList<SourceRead> = Lists.newArrayList()

      for ((source, _, connectionCount, lastSync, connectionJobStatuses, isActive) in sourceConnectionsWithCount) {
        val sourceRead = buildSourceRead(source)
        if (lastSync != null) {
          sourceRead.lastSync(lastSync.toEpochSecond())
        }
        sourceRead.numConnections(connectionCount)
        sourceRead.status =
          if (isActive) ActorStatus.ACTIVE else ActorStatus.INACTIVE

        // Convert Map<JobStatus, Integer> to Map<String, Integer> for API
        val statusCountsMap: MutableMap<String, Int> = HashMap()
        for ((key, value) in connectionJobStatuses) {
          val statusKey =
            when (key) {
              JobStatus.SUCCEEDED -> "succeeded"
              JobStatus.FAILED -> "failed"
              JobStatus.RUNNING -> "running"
              JobStatus.PENDING -> "pending"
              JobStatus.INCOMPLETE -> "incomplete"
              JobStatus.CANCELLED -> "cancelled"
              else -> continue // Skip unknown statuses
            }
          statusCountsMap[statusKey] = value
        }
        sourceRead.connectionJobStatuses(statusCountsMap)

        sourceReads.add(sourceRead)
      }
      return SourceReadList().sources(sourceReads).numConnections(numSources).pageSize(pageSize)
    }

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun listSourcesForWorkspaces(listResourcesForWorkspacesRequestBody: ListResourcesForWorkspacesRequestBody): SourceReadList {
      val sourceConnections =
        sourceService.listWorkspacesSourceConnections(
          ResourcesQueryPaginated(
            listResourcesForWorkspacesRequestBody.workspaceIds,
            listResourcesForWorkspacesRequestBody.includeDeleted,
            listResourcesForWorkspacesRequestBody.pagination.pageSize,
            listResourcesForWorkspacesRequestBody.pagination.rowOffset,
            null,
          ),
        )

      val reads: MutableList<SourceRead> = Lists.newArrayList()
      for (sc in sourceConnections) {
        reads.add(buildSourceReadWithStatus(sc))
      }

      return SourceReadList().sources(reads)
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun listSourcesForSourceDefinition(sourceDefinitionId: UUID): SourceReadList {
      val reads: MutableList<SourceRead> = Lists.newArrayList()
      for (sourceConnection in sourceService.listSourcesForDefinition(sourceDefinitionId)) {
        reads.add(buildSourceRead(sourceConnection))
      }

      return SourceReadList().sources(reads)
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun searchSources(sourceSearch: SourceSearch?): SourceReadList {
      val reads: MutableList<SourceRead> = Lists.newArrayList()

      for (sci in sourceService.listSourceConnection()) {
        if (!sci.tombstone) {
          val sourceRead = buildSourceRead(sci)
          if (MatchSearchHandler.matchSearch(sourceSearch, sourceRead)) {
            reads.add(sourceRead)
          }
        }
      }

      return SourceReadList().sources(reads)
    }

    @Throws(
      JsonValidationException::class,
      IOException::class,
      ConfigNotFoundException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun deleteSource(sourceIdRequestBody: SourceIdRequestBody) {
      // get existing source
      val source = buildSourceRead(sourceIdRequestBody.sourceId)
      deleteSource(source)
    }

    @Throws(
      JsonValidationException::class,
      IOException::class,
      ConfigNotFoundException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun deleteSource(source: SourceRead) {
      // "delete" all connections associated with source as well.
      // Delete connections first in case it fails in the middle, source will still be visible
      val workspaceIdRequestBody =
        WorkspaceIdRequestBody()
          .workspaceId(source.workspaceId)

      val uuidsToDelete =
        connectionsHandler
          .listConnectionsForWorkspace(workspaceIdRequestBody)
          .connections
          .stream()
          .filter { con: ConnectionRead -> con.sourceId == source.sourceId }
          .map { obj: ConnectionRead -> obj.connectionId }
          .toList()

      for (uuidToDelete in uuidsToDelete) {
        connectionsHandler.deleteConnection(uuidToDelete)
      }

      val secretPersistence = secretPersistenceService.getPersistenceFromWorkspaceId(WorkspaceId(workspaceIdRequestBody.workspaceId))

      val configWithSecretReferences =
        secretReferenceService.getConfigWithSecretReferences(
          ActorId(source.sourceId),
          source.connectionConfiguration,
          WorkspaceId(source.workspaceId),
        )

      // Delete airbyte-managed secrets for this source
      secretsRepositoryWriter.deleteFromConfig(configWithSecretReferences, secretPersistence)

      // Delete secret references for this source
      secretReferenceService.deleteActorSecretReferences(ActorId(source.sourceId))

      // Delete partial user config(s) for this source, if any
      partialUserConfigService.deletePartialUserConfigForSource(source.sourceId)

      // Mark source as tombstoned and clear config
      try {
        sourceService.tombstoneSource(
          source.name,
          source.workspaceId,
          source.sourceId,
        )
      } catch (e: ConfigNotFoundException) {
        throw ConfigNotFoundException(e.type, e.configId)
      }
    }

    @Throws(JsonValidationException::class, IOException::class)
    fun writeDiscoverCatalogResult(request: SourceDiscoverSchemaWriteRequestBody): DiscoverCatalogResult {
      val persistenceCatalog = catalogConverter.toProtocol(request.catalog)
      val catalogId = writeActorCatalog(persistenceCatalog, request)

      return DiscoverCatalogResult().catalogId(catalogId)
    }

    @Throws(IOException::class)
    private fun writeActorCatalog(
      persistenceCatalog: AirbyteCatalog,
      request: SourceDiscoverSchemaWriteRequestBody,
    ): UUID =
      catalogService.writeActorCatalogWithFetchEvent(
        persistenceCatalog,
        request.sourceId,
        request.connectorVersion,
        request.configurationHash,
      )

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class, ConfigNotFoundException::class)
    private fun buildSourceReadWithStatus(sourceConnection: SourceConnection): SourceRead {
      val sourceRead = buildSourceRead(sourceConnection)
      // add source status into sourceRead
      if (sourceService.isSourceActive(sourceConnection.sourceId)) {
        sourceRead.status(ActorStatus.ACTIVE)
      } else {
        sourceRead.status(ActorStatus.INACTIVE)
      }
      return sourceRead
    }

    @JvmOverloads
    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun buildSourceRead(
      sourceId: UUID,
      includeSecretCoordinates: Boolean = false,
    ): SourceRead {
      // read configuration from db
      val sourceConnection = sourceService.getSourceConnection(sourceId)
      return buildSourceRead(sourceConnection, includeSecretCoordinates)
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    private fun buildSourceRead(
      sourceConnection: SourceConnection,
      includeSecretCoordinates: Boolean = false,
    ): SourceRead {
      val sourceDef = sourceService.getSourceDefinitionFromSource(sourceConnection.sourceId)
      val sourceVersion =
        actorDefinitionVersionHelper.getSourceVersion(sourceDef, sourceConnection.workspaceId, sourceConnection.sourceId)
      val spec = sourceVersion.spec
      return buildSourceRead(sourceConnection, spec, includeSecretCoordinates)
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    private fun buildSourceRead(
      sourceConnection: SourceConnection,
      spec: ConnectorSpecification,
      includeSecretCoordinates: Boolean = false,
    ): SourceRead {
      // read configuration from db
      val standardSourceDefinition =
        sourceService
          .getStandardSourceDefinition(sourceConnection.sourceDefinitionId)
      val configWithRefs =
        secretReferenceService.getConfigWithSecretReferences(
          ActorId(sourceConnection.sourceId),
          sourceConnection.configuration,
          WorkspaceId(sourceConnection.workspaceId),
        )
      val inlinedConfigWithRefs: JsonNode = configWithRefs.toInlined().value
      val sanitizedConfig =
        if (includeSecretCoordinates) {
          secretsProcessor.simplifySecretsForOutput(
            configWithRefs,
            spec.connectionSpecification,
            airbyteEdition != AirbyteEdition.CLOUD,
          )
        } else {
          secretsProcessor.prepareSecretsForOutput(inlinedConfigWithRefs, spec.connectionSpecification)
        }
      sourceConnection.configuration = sanitizedConfig
      return toSourceRead(sourceConnection, standardSourceDefinition)
    }

    @Throws(JsonValidationException::class)
    private fun validateSource(
      spec: ConnectorSpecification,
      implementationJson: JsonNode,
    ) {
      validator.ensure(spec.connectionSpecification, implementationJson)
    }

    /**
     * Validates the provided update JSON against the spec by merging it into the full updated source
     * config.
     *
     *
     * Note: The existing source config may have been persisted with secret object nodes instead of raw
     * values, which must be replaced with placeholder text nodes in order to pass validation.
     */
    @Throws(JsonValidationException::class)
    private fun validateSourceUpdate(
      providedUpdateJson: JsonNode?,
      updatedSource: SourceConnection,
      spec: ConnectorSpecification,
    ) {
      // Replace any secret object nodes with placeholder text nodes that will pass validation.
      val updatedSourceConfigWithSecretPlaceholders =
        configWithTextualSecretPlaceholders(
          updatedSource.configuration,
          spec.connectionSpecification,
        )
      // Merge the provided update JSON into the updated source config with secret placeholders.
      // The final result should pass validation as long as the provided update JSON is valid.
      val mergedConfig =
        Optional
          .ofNullable(providedUpdateJson)
          .map { update: JsonNode ->
            Jsons.mergeNodes(
              updatedSourceConfigWithSecretPlaceholders,
              update,
            )
          }.orElse(updatedSourceConfigWithSecretPlaceholders)

      validateSource(spec, mergedConfig)
    }

    @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
    private fun getSourceVersionForSourceId(sourceId: UUID): ActorDefinitionVersion {
      val source = sourceService.getSourceConnection(sourceId)
      val sourceDef = sourceService.getStandardSourceDefinition(source.sourceDefinitionId)
      return actorDefinitionVersionHelper.getSourceVersion(sourceDef, source.workspaceId, sourceId)
    }

    @Throws(
      IOException::class,
      JsonValidationException::class,
      ConfigNotFoundException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun getSourceVersionForWorkspaceId(
      sourceDefId: UUID,
      workspaceId: UUID,
    ): ActorDefinitionVersion {
      val sourceDef = sourceService.getStandardSourceDefinition(sourceDefId)
      return actorDefinitionVersionHelper.getSourceVersion(sourceDef, workspaceId)
    }

    /**
     * Persists a source to the database, with secret masking and handling applied for OAuth and other
     * secret values in the provided configuration json. Raw secret values and prefixed secret
     * coordinates are split out from the provided config and replaced with coordinates or secret
     * reference IDs, depending on whether the workspace has a secret storage configured.
     */
    @Throws(JsonValidationException::class, IOException::class)
    private fun persistSourceConnection(
      name: String,
      sourceDefinitionId: UUID,
      workspaceId: UUID,
      sourceId: UUID,
      tombstone: Boolean,
      configurationJson: JsonNode,
      sourceVersion: ActorDefinitionVersion,
      resourceRequirements: ScopedResourceRequirements?,
    ) {
      val organizationId = workspaceHelper.getOrganizationForWorkspace(workspaceId)
      licenseEntitlementChecker.ensureEntitled(organizationId, Entitlement.SOURCE_CONNECTOR, sourceDefinitionId)
      connectorConfigEntitlementService.ensureEntitledConfig(OrganizationId(organizationId), sourceVersion, configurationJson)

      val spec = sourceVersion.spec
      val maskedConfig = oAuthConfigSupplier.maskSourceOAuthParameters(sourceDefinitionId, workspaceId, configurationJson, spec)
      val secretStorageId = Optional.ofNullable(secretStorageService.getByWorkspaceId(WorkspaceId(workspaceId))).map { obj -> obj.id.value }

      val newSourceConnection =
        SourceConnection()
          .withName(name)
          .withSourceDefinitionId(sourceDefinitionId)
          .withWorkspaceId(workspaceId)
          .withSourceId(sourceId)
          .withTombstone(tombstone)
          .withResourceRequirements(apiPojoConverters.scopedResourceReqsToInternal(resourceRequirements))

      var updatedConfig = persistConfigRawSecretValues(maskedConfig, secretStorageId, workspaceId, spec, sourceId)

      if (secretStorageId.isPresent) {
        val reprocessedConfig =
          processConfigSecrets(
            updatedConfig,
            spec.connectionSpecification,
            secretStorageId.map { obj -> SecretStorageId(obj) }.orElse(null),
          )
        updatedConfig =
          secretReferenceService
            .createAndInsertSecretReferencesWithStorageId(
              reprocessedConfig,
              ActorId(sourceId),
              WorkspaceId(workspaceId),
              SecretStorageId(secretStorageId.get()),
              currentUserService.getCurrentUserIdIfExists().map { UserId(it) }.orElse(null),
            ).value
      }

      newSourceConnection.configuration = updatedConfig
      sourceService.writeSourceConnectionNoSecrets(newSourceConnection)
    }

    /**
     * Persists raw secret values for the given config. Creates or updates depending on whether a prior
     * config exists.
     *
     * @return new config with secret values replaced with secret coordinate nodes.
     */
    @Throws(JsonValidationException::class)
    fun persistConfigRawSecretValues(
      config: JsonNode,
      secretStorageId: Optional<UUID>,
      workspaceId: UUID,
      spec: ConnectorSpecification,
      sourceId: UUID,
    ): JsonNode {
      val secretPersistence = secretPersistenceService.getPersistenceFromWorkspaceId(WorkspaceId(workspaceId))
      val processedConfig =
        processConfigSecrets(
          config,
          spec.connectionSpecification,
          secretStorageId.map { SecretStorageId(it) }.orElse(null),
        )
      val previousConfig =
        sourceService
          .getSourceConnectionIfExists(sourceId)
          .map { obj: SourceConnection -> obj.configuration }
      if (previousConfig.isPresent) {
        val priorConfigWithSecretReferences =
          secretReferenceService.getConfigWithSecretReferences(
            ActorId(sourceId),
            previousConfig.get(),
            WorkspaceId(workspaceId),
          )
        return secretsRepositoryWriter.updateFromConfig(
          workspaceId,
          priorConfigWithSecretReferences,
          processedConfig,
          spec.connectionSpecification,
          secretPersistence,
        )
      } else {
        return secretsRepositoryWriter.createFromConfig(
          workspaceId,
          processedConfig,
          secretPersistence,
        )
      }
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun toSourceRead(
      sourceConnection: SourceConnection,
      standardSourceDefinition: StandardSourceDefinition,
    ): SourceRead {
      val sourceVersionWithOverrideStatus =
        actorDefinitionVersionHelper.getSourceVersionWithOverrideStatus(
          standardSourceDefinition,
          sourceConnection.workspaceId,
          sourceConnection.sourceId,
        )

      val breakingChanges: Optional<ActorDefinitionVersionBreakingChanges> =
        actorDefinitionHandlerHelper.getVersionBreakingChanges(sourceVersionWithOverrideStatus.actorDefinitionVersion)

      val organizationId = workspaceHelper.getOrganizationForWorkspace(sourceConnection.workspaceId)
      val isEntitled =
        licenseEntitlementChecker.checkEntitlement(organizationId, Entitlement.SOURCE_CONNECTOR, standardSourceDefinition.sourceDefinitionId)

      return SourceRead()
        .sourceDefinitionId(standardSourceDefinition.sourceDefinitionId)
        .sourceName(standardSourceDefinition.name)
        .sourceId(sourceConnection.sourceId)
        .workspaceId(sourceConnection.workspaceId)
        .sourceDefinitionId(sourceConnection.sourceDefinitionId)
        .connectionConfiguration(sourceConnection.configuration)
        .name(sourceConnection.name)
        .icon(standardSourceDefinition.iconUrl)
        .isVersionOverrideApplied(sourceVersionWithOverrideStatus.isOverrideApplied)
        .isEntitled(isEntitled)
        .breakingChanges(breakingChanges.orElse(null))
        .supportState(apiPojoConverters.toApiSupportState(sourceVersionWithOverrideStatus.actorDefinitionVersion.supportState))
        .createdAt(sourceConnection.createdAt)
        .resourceAllocation(apiPojoConverters.scopedResourceReqsToApi(sourceConnection.resourceRequirements))
    }

    fun toSourceSnippetRead(
      source: SourceConnection,
      sourceDefinition: StandardSourceDefinition,
    ): SourceSnippetRead =
      SourceSnippetRead()
        .sourceId(source.sourceId)
        .name(source.name)
        .sourceDefinitionId(sourceDefinition.sourceDefinitionId)
        .sourceName(sourceDefinition.name)
        .icon(sourceDefinition.iconUrl)

    @VisibleForTesting
    fun hydrateOAuthResponseSecret(
      secretId: String,
      workspaceId: UUID,
    ): JsonNode {
      val secretCoordinate = fromFullCoordinate(secretId)
      val secretPersistence = secretPersistenceService.getPersistenceFromWorkspaceId(WorkspaceId(workspaceId))
      val secret = secretsRepositoryReader.fetchJsonSecretFromSecretPersistence(secretCoordinate, secretPersistence)
      val completeOAuthResponse =
        Jsons.`object`(
          secret,
          CompleteOAuthResponse::class.java,
        )
      return Jsons.jsonNode(completeOAuthResponse.authPayload)
    }
  }
