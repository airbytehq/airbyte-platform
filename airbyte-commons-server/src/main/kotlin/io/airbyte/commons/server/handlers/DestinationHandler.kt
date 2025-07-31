/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.fasterxml.jackson.databind.JsonNode
import com.google.common.annotations.VisibleForTesting
import com.google.common.collect.Lists
import io.airbyte.api.model.generated.ActorDefinitionVersionBreakingChanges
import io.airbyte.api.model.generated.ActorListCursorPaginatedRequestBody
import io.airbyte.api.model.generated.ActorStatus
import io.airbyte.api.model.generated.ActorType
import io.airbyte.api.model.generated.DestinationCreate
import io.airbyte.api.model.generated.DestinationIdRequestBody
import io.airbyte.api.model.generated.DestinationRead
import io.airbyte.api.model.generated.DestinationReadList
import io.airbyte.api.model.generated.DestinationSearch
import io.airbyte.api.model.generated.DestinationSnippetRead
import io.airbyte.api.model.generated.DestinationUpdate
import io.airbyte.api.model.generated.ListResourcesForWorkspacesRequestBody
import io.airbyte.api.model.generated.PartialDestinationUpdate
import io.airbyte.api.model.generated.ScopedResourceRequirements
import io.airbyte.api.model.generated.WorkspaceIdRequestBody
import io.airbyte.commons.entitlements.Entitlement
import io.airbyte.commons.entitlements.LicenseEntitlementChecker
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.converters.ConfigurationUpdate
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.handlers.helpers.ActorDefinitionHandlerHelper
import io.airbyte.commons.server.handlers.helpers.OAuthSecretHelper.validateNoSecretsInConfiguration
import io.airbyte.commons.server.support.CurrentUserService
import io.airbyte.config.ActorDefinitionVersion
import io.airbyte.config.Configs.AirbyteEdition
import io.airbyte.config.DestinationConnection
import io.airbyte.config.JobStatus
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.persistence.ActorDefinitionVersionHelper
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers.configWithTextualSecretPlaceholders
import io.airbyte.config.secrets.SecretsHelpers.SecretReferenceHelpers.processConfigSecrets
import io.airbyte.config.secrets.SecretsRepositoryWriter
import io.airbyte.config.secrets.toInlined
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.helpers.ActorDefinitionVersionUpdater
import io.airbyte.data.services.DestinationService
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
import io.airbyte.persistence.job.WorkspaceHelper
import io.airbyte.persistence.job.factory.OAuthConfigSupplier
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.validation.json.JsonSchemaValidator
import io.airbyte.validation.json.JsonValidationException
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.IOException
import java.util.Optional
import java.util.UUID
import java.util.function.Supplier

/**
 * DestinationHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
class DestinationHandler
  @VisibleForTesting
  constructor(
    private val validator: JsonSchemaValidator,
    private val connectionsHandler: ConnectionsHandler,
    @param:Named("uuidGenerator") private val uuidGenerator: Supplier<UUID>,
    @param:Named("jsonSecretsProcessorWithCopy") private val secretsProcessor: JsonSecretsProcessor,
    private val configurationUpdate: ConfigurationUpdate,
    private val oAuthConfigSupplier: OAuthConfigSupplier,
    private val actorDefinitionVersionHelper: ActorDefinitionVersionHelper,
    private val destinationService: DestinationService,
    private val actorDefinitionHandlerHelper: ActorDefinitionHandlerHelper,
    private val actorDefinitionVersionUpdater: ActorDefinitionVersionUpdater,
    private val apiPojoConverters: ApiPojoConverters,
    private val workspaceHelper: WorkspaceHelper,
    private val licenseEntitlementChecker: LicenseEntitlementChecker,
    private val airbyteEdition: AirbyteEdition,
    private val secretsRepositoryWriter: SecretsRepositoryWriter,
    private val secretPersistenceService: SecretPersistenceService,
    private val secretStorageService: SecretStorageService,
    private val secretReferenceService: SecretReferenceService,
    private val currentUserService: CurrentUserService,
    private val connectorConfigEntitlementService: ConnectorConfigEntitlementService,
  ) {
    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun createDestination(destinationCreate: DestinationCreate): DestinationRead {
      if (destinationCreate.resourceAllocation != null && airbyteEdition == AirbyteEdition.CLOUD) {
        throw BadRequestException(String.format("Setting resource allocation is not permitted on %s", airbyteEdition))
      }

      // validate configuration
      val destinationVersion =
        getDestinationVersionForWorkspaceId(destinationCreate.destinationDefinitionId, destinationCreate.workspaceId)
      val spec = destinationVersion.spec
      validateDestination(spec, destinationCreate.connectionConfiguration)

      // persist
      val destinationId = uuidGenerator.get()
      persistDestinationConnection(
        if (destinationCreate.name != null) destinationCreate.name else "default",
        destinationCreate.destinationDefinitionId,
        destinationCreate.workspaceId,
        destinationId,
        destinationCreate.connectionConfiguration,
        false,
        destinationVersion,
        destinationCreate.resourceAllocation,
      )

      // read configuration from db
      return buildDestinationRead(destinationService.getDestinationConnection(destinationId), spec)
    }

    @Throws(
      JsonValidationException::class,
      IOException::class,
      ConfigNotFoundException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun deleteDestination(destinationIdRequestBody: DestinationIdRequestBody) {
      // get existing implementation
      val destination = buildDestinationRead(destinationIdRequestBody.destinationId)

      deleteDestination(destination)
    }

    @Throws(
      JsonValidationException::class,
      IOException::class,
      ConfigNotFoundException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun deleteDestination(destination: DestinationRead) {
      // disable all connections associated with this destination
      // Delete connections first in case it fails in the middle, destination will still be visible
      val workspaceIdRequestBody = WorkspaceIdRequestBody().workspaceId(destination.workspaceId)
      for (connectionRead in connectionsHandler.listConnectionsForWorkspace(workspaceIdRequestBody).connections) {
        if (connectionRead.destinationId != destination.destinationId) {
          continue
        }

        connectionsHandler.deleteConnection(connectionRead.connectionId)
      }
      val secretPersistence = secretPersistenceService.getPersistenceFromWorkspaceId(WorkspaceId(destination.workspaceId))
      val configWithSecretReferences =
        secretReferenceService.getConfigWithSecretReferences(
          ActorId(destination.destinationId),
          destination.connectionConfiguration,
          WorkspaceId(destination.workspaceId),
        )

      // Delete airbyte-managed secrets for this destination
      secretsRepositoryWriter.deleteFromConfig(configWithSecretReferences, secretPersistence)

      // Delete secret references for this destination
      secretReferenceService.deleteActorSecretReferences(ActorId(destination.destinationId))

      // Mark destination as tombstoned and clear config
      try {
        destinationService.tombstoneDestination(
          destination.name,
          destination.workspaceId,
          destination.destinationId,
        )
      } catch (e: ConfigNotFoundException) {
        throw ConfigNotFoundException(e.type, e.configId)
      }
    }

    @Throws(
      ConfigNotFoundException::class,
      IOException::class,
      JsonValidationException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun updateDestination(destinationUpdate: DestinationUpdate): DestinationRead {
      if (destinationUpdate.resourceAllocation != null && airbyteEdition == AirbyteEdition.CLOUD) {
        throw BadRequestException(String.format("Setting resource allocation is not permitted on %s", airbyteEdition))
      }

      // get existing implementation
      val updatedDestination =
        configurationUpdate.destination(
          destinationUpdate.destinationId,
          destinationUpdate.name,
          destinationUpdate.connectionConfiguration,
        )

      val destinationVersion =
        getDestinationVersionForDestinationId(
          updatedDestination.destinationDefinitionId,
          updatedDestination.workspaceId,
          updatedDestination.destinationId,
        )
      val spec = destinationVersion.spec

      validateDestinationUpdate(destinationUpdate.connectionConfiguration, updatedDestination, spec)

      // persist
      persistDestinationConnection(
        updatedDestination.name,
        updatedDestination.destinationDefinitionId,
        updatedDestination.workspaceId,
        updatedDestination.destinationId,
        updatedDestination.configuration,
        updatedDestination.tombstone,
        destinationVersion,
        destinationUpdate.resourceAllocation,
      )

      // read configuration from db
      return buildDestinationRead(
        destinationService.getDestinationConnection(destinationUpdate.destinationId),
        spec,
      )
    }

    @Throws(
      ConfigNotFoundException::class,
      IOException::class,
      JsonValidationException::class,
      io.airbyte.config.persistence.ConfigNotFoundException::class,
    )
    fun partialDestinationUpdate(partialDestinationUpdate: PartialDestinationUpdate): DestinationRead {
      if (partialDestinationUpdate.resourceAllocation != null && airbyteEdition == AirbyteEdition.CLOUD) {
        throw BadRequestException(String.format("Setting resource allocation is not permitted on %s", airbyteEdition))
      }

      // get existing implementation
      val updatedDestination =
        configurationUpdate
          .partialDestination(
            partialDestinationUpdate.destinationId,
            partialDestinationUpdate.name,
            partialDestinationUpdate.connectionConfiguration,
          )

      val destinationVersion =
        getDestinationVersionForDestinationId(
          updatedDestination.destinationDefinitionId,
          updatedDestination.workspaceId,
          updatedDestination.destinationId,
        )
      val spec = destinationVersion.spec

      validateNoSecretsInConfiguration(spec, partialDestinationUpdate.connectionConfiguration)

      validateDestinationUpdate(partialDestinationUpdate.connectionConfiguration, updatedDestination, spec)

      // persist
      persistDestinationConnection(
        updatedDestination.name,
        updatedDestination.destinationDefinitionId,
        updatedDestination.workspaceId,
        updatedDestination.destinationId,
        updatedDestination.configuration,
        updatedDestination.tombstone,
        destinationVersion,
        partialDestinationUpdate.resourceAllocation,
      )

      // read configuration from db
      return buildDestinationRead(
        destinationService.getDestinationConnection(partialDestinationUpdate.destinationId),
        spec,
      )
    }

    /**
     * Upgrades the destination to the destination definition's default version.
     *
     * @param destinationIdRequestBody - ID of the destination to upgrade
     */
    @Throws(IOException::class, JsonValidationException::class, ConfigNotFoundException::class)
    fun upgradeDestinationVersion(destinationIdRequestBody: DestinationIdRequestBody) {
      val destinationConnection = destinationService.getDestinationConnection(destinationIdRequestBody.destinationId)
      val destinationDefinition =
        destinationService.getStandardDestinationDefinition(destinationConnection.destinationDefinitionId)
      actorDefinitionVersionUpdater.upgradeActorVersion(destinationConnection, destinationDefinition)
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun getDestination(destinationIdRequestBody: DestinationIdRequestBody): DestinationRead = getDestination(destinationIdRequestBody, false)

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun getDestination(
      destinationIdRequestBody: DestinationIdRequestBody,
      includeSecretCoordinates: Boolean,
    ): DestinationRead = buildDestinationRead(destinationIdRequestBody.destinationId, includeSecretCoordinates)

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun listDestinationsForWorkspace(actorListCursorPaginatedRequestBody: ActorListCursorPaginatedRequestBody): DestinationReadList {
      val filters = actorListCursorPaginatedRequestBody.filters
      val pageSize =
        if (actorListCursorPaginatedRequestBody.pageSize != null) {
          actorListCursorPaginatedRequestBody.pageSize
        } else {
          DEFAULT_PAGE_SIZE
        }

      // Parse sort key to extract field and direction
      val sortKeyInfo =
        parseSortKey(actorListCursorPaginatedRequestBody.sortKey, ActorType.DESTINATION)
      val internalSortKey = sortKeyInfo.sortKey
      val ascending = sortKeyInfo.ascending
      val actorFilters = buildFilters(filters)

      val cursorPagination =
        destinationService.buildCursorPagination(
          actorListCursorPaginatedRequestBody.cursor,
          internalSortKey,
          actorFilters,
          ascending,
          pageSize,
        )!!

      val numDestinations =
        destinationService.countWorkspaceDestinationsFiltered(
          actorListCursorPaginatedRequestBody.workspaceId,
          cursorPagination,
        )

      val destinationConnectionsWithCount =
        destinationService.listWorkspaceDestinationConnectionsWithCounts(actorListCursorPaginatedRequestBody.workspaceId, cursorPagination)

      val destinationReads: MutableList<DestinationRead> = Lists.newArrayList()

      for ((destination, _, connectionCount, lastSync, connectionJobStatuses, isActive) in destinationConnectionsWithCount) {
        val destinationRead = buildDestinationRead(destination)
        if (lastSync != null) {
          destinationRead.lastSync(lastSync.toEpochSecond())
        }
        destinationRead.numConnections(connectionCount)
        destinationRead.status =
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
        destinationRead.connectionJobStatuses(statusCountsMap)

        destinationReads.add(destinationRead)
      }
      return DestinationReadList().destinations(destinationReads).numConnections(numDestinations).pageSize(pageSize)
    }

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    private fun buildDestinationReadWithStatus(destinationConnection: DestinationConnection): DestinationRead {
      val destinationRead = buildDestinationRead(destinationConnection)
      // add destination status into destinationRead
      if (destinationService.isDestinationActive(destinationConnection.destinationId)) {
        destinationRead.status(ActorStatus.ACTIVE)
      } else {
        destinationRead.status(ActorStatus.INACTIVE)
      }
      return destinationRead
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun listDestinationsForWorkspaces(listResourcesForWorkspacesRequestBody: ListResourcesForWorkspacesRequestBody): DestinationReadList {
      val reads: MutableList<DestinationRead> = Lists.newArrayList()
      val destinationConnections =
        destinationService.listWorkspacesDestinationConnections(
          ResourcesQueryPaginated(
            listResourcesForWorkspacesRequestBody.workspaceIds,
            listResourcesForWorkspacesRequestBody.includeDeleted,
            listResourcesForWorkspacesRequestBody.pagination.pageSize,
            listResourcesForWorkspacesRequestBody.pagination.rowOffset,
            null,
          ),
        )
      for (destinationConnection in destinationConnections) {
        reads.add(buildDestinationReadWithStatus(destinationConnection))
      }
      return DestinationReadList().destinations(reads)
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun listDestinationsForDestinationDefinition(destinationDefinitionId: UUID): DestinationReadList {
      val reads: MutableList<DestinationRead> = Lists.newArrayList()

      for (destinationConnection in destinationService
        .listDestinationsForDefinition(destinationDefinitionId)) {
        reads.add(buildDestinationRead(destinationConnection))
      }

      return DestinationReadList().destinations(reads)
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    fun searchDestinations(destinationSearch: DestinationSearch?): DestinationReadList {
      val reads: MutableList<DestinationRead> = Lists.newArrayList()

      for (dci in destinationService.listDestinationConnection()) {
        if (!dci.tombstone) {
          val destinationRead = buildDestinationRead(dci)
          if (MatchSearchHandler.matchSearch(destinationSearch, destinationRead)) {
            reads.add(destinationRead)
          }
        }
      }

      return DestinationReadList().destinations(reads)
    }

    @Throws(JsonValidationException::class)
    private fun validateDestination(
      spec: ConnectorSpecification,
      configuration: JsonNode,
    ) {
      validator.ensure(spec.connectionSpecification, configuration)
    }

    /**
     * Validates the provided update JSON against the spec by merging it into the full updated
     * destination config.
     *
     *
     * Note: The existing destination config may have been persisted with secret object nodes instead of
     * raw values, which must be replaced with placeholder text nodes in order to pass validation.
     */
    @Throws(JsonValidationException::class)
    private fun validateDestinationUpdate(
      providedUpdateJson: JsonNode?,
      updatedDestination: DestinationConnection,
      spec: ConnectorSpecification,
    ) {
      // Replace any secret object nodes with placeholder text nodes that will pass validation.
      val updatedDestinationConfigWithSecretPlaceholders =
        configWithTextualSecretPlaceholders(
          updatedDestination.configuration,
          spec.connectionSpecification,
        )
      // Merge the provided update JSON into the updated destination config with secret placeholders.
      // The final result should pass validation as long as the provided update JSON is valid.
      val mergedConfig =
        Optional
          .ofNullable(providedUpdateJson)
          .map { update: JsonNode ->
            Jsons.mergeNodes(
              updatedDestinationConfigWithSecretPlaceholders,
              update,
            )
          }.orElse(updatedDestinationConfigWithSecretPlaceholders)

      validateDestination(spec, mergedConfig)
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun getDestinationVersionForDestinationId(
      destinationDefinitionId: UUID,
      workspaceId: UUID,
      destinationId: UUID?,
    ): ActorDefinitionVersion {
      val destinationDefinition = destinationService.getStandardDestinationDefinition(destinationDefinitionId)
      return actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId, destinationId)
    }

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun getDestinationVersionForWorkspaceId(
      destinationDefinitionId: UUID,
      workspaceId: UUID,
    ): ActorDefinitionVersion {
      val destinationDefinition = destinationService.getStandardDestinationDefinition(destinationDefinitionId)
      return actorDefinitionVersionHelper.getDestinationVersion(destinationDefinition, workspaceId)
    }

    /**
     * Persists a destination to the database, with secret masking and handling applied for OAuth and
     * other secret values in the provided configuration json. Raw secret values and prefixed secret
     * coordinates are split out from the provided config and replaced with coordinates or secret
     * reference IDs, depending on whether the workspace has a secret storage configured.
     */
    @Throws(JsonValidationException::class, IOException::class)
    private fun persistDestinationConnection(
      name: String,
      destinationDefinitionId: UUID,
      workspaceId: UUID,
      destinationId: UUID,
      configurationJson: JsonNode,
      tombstone: Boolean,
      destinationVersion: ActorDefinitionVersion,
      resourceRequirements: ScopedResourceRequirements?,
    ) {
      val organizationId = workspaceHelper.getOrganizationForWorkspace(workspaceId)
      licenseEntitlementChecker.ensureEntitled(organizationId, Entitlement.DESTINATION_CONNECTOR, destinationDefinitionId)
      connectorConfigEntitlementService.ensureEntitledConfig(OrganizationId(organizationId), destinationVersion, configurationJson)

      val spec = destinationVersion.spec
      val maskedConfig = oAuthConfigSupplier.maskDestinationOAuthParameters(destinationDefinitionId, workspaceId, configurationJson, spec)
      val secretStorageId = Optional.ofNullable(secretStorageService.getByWorkspaceId(WorkspaceId(workspaceId))).map { obj -> obj.id.value }

      val destinationConnection =
        DestinationConnection()
          .withName(name)
          .withDestinationDefinitionId(destinationDefinitionId)
          .withWorkspaceId(workspaceId)
          .withDestinationId(destinationId)
          .withTombstone(tombstone)
          .withResourceRequirements(apiPojoConverters.scopedResourceReqsToInternal(resourceRequirements))

      var updatedConfig: JsonNode = persistConfigRawSecretValues(maskedConfig, secretStorageId, workspaceId, spec, destinationId)

      if (secretStorageId.isPresent) {
        val reprocessedConfig =
          processConfigSecrets(
            updatedConfig,
            spec.connectionSpecification,
            SecretStorageId(secretStorageId.get()),
          )
        updatedConfig =
          secretReferenceService
            .createAndInsertSecretReferencesWithStorageId(
              reprocessedConfig,
              ActorId(destinationId),
              WorkspaceId(workspaceId),
              SecretStorageId(secretStorageId.get()),
              currentUserService.getCurrentUserIdIfExists().map { obj -> UserId(obj) }.orElse(null),
            ).value
      }

      destinationConnection.configuration = updatedConfig
      destinationService.writeDestinationConnectionNoSecrets(destinationConnection)
    }

    /**
     * Persists raw secret values for the given config. Creates or updates depending on whether a prior
     * config exists.
     *
     * @return new config with secret values replaced with secret coordinate nodes.
     */
    @Throws(JsonValidationException::class)
    private fun persistConfigRawSecretValues(
      config: JsonNode,
      secretStorageId: Optional<UUID>,
      workspaceId: UUID,
      spec: ConnectorSpecification,
      destinationId: UUID,
    ): JsonNode {
      val secretPersistence = secretPersistenceService.getPersistenceFromWorkspaceId(WorkspaceId(workspaceId))
      val processedConfig =
        processConfigSecrets(
          config,
          spec.connectionSpecification,
          secretStorageId.map { obj -> SecretStorageId(obj) }.orElse(null),
        )
      val previousConfig =
        destinationService
          .getDestinationConnectionIfExists(destinationId)
          .map { obj: DestinationConnection -> obj.configuration }
      if (previousConfig.isPresent) {
        val priorConfigWithSecretReferences =
          secretReferenceService.getConfigWithSecretReferences(
            ActorId(destinationId),
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

    @JvmOverloads
    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    fun buildDestinationRead(
      destinationId: UUID,
      includeSecretCoordinates: Boolean = false,
    ): DestinationRead = buildDestinationRead(destinationService.getDestinationConnection(destinationId), includeSecretCoordinates)

    @Throws(JsonValidationException::class, IOException::class, ConfigNotFoundException::class)
    private fun buildDestinationRead(
      destinationConnection: DestinationConnection,
      includeSecretCoordinates: Boolean = false,
    ): DestinationRead {
      val destinationDef =
        destinationService.getDestinationDefinitionFromDestination(destinationConnection.destinationId)
      val destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(
          destinationDef,
          destinationConnection.workspaceId,
          destinationConnection.destinationId,
        )
      val spec = destinationVersion.spec
      return buildDestinationRead(destinationConnection, spec, includeSecretCoordinates)
    }

    @Throws(ConfigNotFoundException::class, IOException::class, JsonValidationException::class)
    private fun buildDestinationRead(
      destinationConnection: DestinationConnection,
      spec: ConnectorSpecification,
      includeSecretCoordinates: Boolean = false,
    ): DestinationRead {
      // remove secrets from config before returning the read

      val dci = Jsons.clone(destinationConnection)
      val configWithRefs =
        secretReferenceService.getConfigWithSecretReferences(
          ActorId(dci.destinationId),
          dci.configuration,
          WorkspaceId(destinationConnection.workspaceId),
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
      dci.configuration = sanitizedConfig

      val standardDestinationDefinition =
        destinationService.getStandardDestinationDefinition(dci.destinationDefinitionId)
      return toDestinationRead(dci, standardDestinationDefinition)
    }

    @Throws(JsonValidationException::class, ConfigNotFoundException::class, IOException::class)
    fun toDestinationRead(
      destinationConnection: DestinationConnection,
      standardDestinationDefinition: StandardDestinationDefinition,
    ): DestinationRead {
      val destinationVersionWithOverrideStatus =
        actorDefinitionVersionHelper.getDestinationVersionWithOverrideStatus(
          standardDestinationDefinition,
          destinationConnection.workspaceId,
          destinationConnection.destinationId,
        )

      val breakingChanges: Optional<ActorDefinitionVersionBreakingChanges> =
        actorDefinitionHandlerHelper.getVersionBreakingChanges(destinationVersionWithOverrideStatus.actorDefinitionVersion)

      val organizationId = workspaceHelper.getOrganizationForWorkspace(destinationConnection.workspaceId)
      val isEntitled =
        licenseEntitlementChecker.checkEntitlement(
          organizationId,
          Entitlement.DESTINATION_CONNECTOR,
          standardDestinationDefinition.destinationDefinitionId,
        )

      return DestinationRead()
        .destinationDefinitionId(standardDestinationDefinition.destinationDefinitionId)
        .destinationId(destinationConnection.destinationId)
        .workspaceId(destinationConnection.workspaceId)
        .destinationDefinitionId(destinationConnection.destinationDefinitionId)
        .connectionConfiguration(destinationConnection.configuration)
        .name(destinationConnection.name)
        .destinationName(standardDestinationDefinition.name)
        .icon(standardDestinationDefinition.iconUrl)
        .isVersionOverrideApplied(destinationVersionWithOverrideStatus.isOverrideApplied)
        .isEntitled(isEntitled)
        .breakingChanges(breakingChanges.orElse(null))
        .supportState(apiPojoConverters.toApiSupportState(destinationVersionWithOverrideStatus.actorDefinitionVersion.supportState))
        .createdAt(destinationConnection.createdAt)
        .resourceAllocation(apiPojoConverters.scopedResourceReqsToApi(destinationConnection.resourceRequirements))
    }

    fun toDestinationSnippetRead(
      destinationConnection: DestinationConnection,
      standardDestinationDefinition: StandardDestinationDefinition,
    ): DestinationSnippetRead =
      DestinationSnippetRead()
        .destinationId(destinationConnection.destinationId)
        .name(destinationConnection.name)
        .destinationDefinitionId(standardDestinationDefinition.destinationDefinitionId)
        .destinationName(standardDestinationDefinition.name)
        .icon(standardDestinationDefinition.iconUrl)

    companion object {
      private val log = KotlinLogging.logger {}
    }
  }
