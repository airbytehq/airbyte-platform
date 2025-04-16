/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.ScopedConfigurationContextRequestBody
import io.airbyte.api.model.generated.ScopedConfigurationContextResponse
import io.airbyte.api.model.generated.ScopedConfigurationCreateRequestBody
import io.airbyte.api.model.generated.ScopedConfigurationRead
import io.airbyte.commons.server.errors.BadRequestException
import io.airbyte.commons.server.handlers.helpers.ScopedConfigurationRelationshipResolver
import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.config.ScopedConfiguration
import io.airbyte.config.persistence.UserPersistence
import io.airbyte.data.exceptions.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.OrganizationService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.SourceService
import io.airbyte.data.services.WorkspaceService
import io.airbyte.data.services.shared.ConnectorVersionKey
import io.airbyte.data.services.shared.ScopedConfigurationKeys
import io.micronaut.cache.annotation.Cacheable
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.time.Instant
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import java.util.function.Supplier
import java.util.stream.Collectors

fun unixTimestampToOffsetDateTime(unixTimestamp: Long): OffsetDateTime {
  val instantRepresentation = Instant.ofEpochMilli(unixTimestamp)
  return OffsetDateTime.ofInstant(instantRepresentation, ZoneOffset.UTC)
}

/**
 * OperationsHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
open class ScopedConfigurationHandler
  @Inject
  constructor(
    private val scopedConfigurationService: ScopedConfigurationService,
    private val actorDefinitionService: ActorDefinitionService,
    private val sourceService: SourceService,
    private val destinationService: DestinationService,
    private val organizationService: OrganizationService,
    private val workspaceService: WorkspaceService,
    private val userPersistence: UserPersistence,
    private val uuidGenerator: Supplier<UUID>,
    private val scopeRelationshipResolver: ScopedConfigurationRelationshipResolver,
  ) {
    @Cacheable("config-value-name")
    open fun getValueName(
      configKey: String,
      value: String,
    ): String? =
      when (configKey) {
        ConnectorVersionKey.key -> actorDefinitionService.getActorDefinitionVersion(UUID.fromString(value)).dockerImageTag
        else -> null
      }

    private fun resolveActorDefinitionName(actorDefinitionId: UUID): String {
      // Since we don't have a central "Actor Definition" service, we need to check first for source, then destination.
      return try {
        sourceService.getStandardSourceDefinition(actorDefinitionId).name
      } catch (e: ConfigNotFoundException) {
        destinationService.getStandardDestinationDefinition(actorDefinitionId).name
      }
    }

    private fun resolveActorName(actorId: UUID): String {
      // Since we don't have a central "Actor" service, we need to check first for source, then destination.
      return try {
        sourceService.getSourceConnection(actorId).name
      } catch (e: ConfigNotFoundException) {
        destinationService.getDestinationConnection(actorId).name
      }
    }

    @Cacheable("config-resource-name")
    open fun getResourceName(
      resourceType: ConfigResourceType,
      resourceId: UUID,
    ): String =
      when (resourceType) {
        ConfigResourceType.ACTOR_DEFINITION -> resolveActorDefinitionName(resourceId)
        else -> resourceType.name
      }

    @Cacheable("config-origin-name")
    open fun getOriginName(
      originType: ConfigOriginType,
      origin: String,
    ): String? =
      when (originType) {
        ConfigOriginType.USER -> userPersistence.getUser(UUID.fromString(origin)).get().email
        else -> null
      }

    @Cacheable("config-scope-name")
    open fun getScopeName(
      scopeType: ConfigScopeType,
      scopeId: UUID,
    ): String =
      when (scopeType) {
        ConfigScopeType.ORGANIZATION -> organizationService.getOrganization(scopeId).get().name
        ConfigScopeType.WORKSPACE -> workspaceService.getStandardWorkspaceNoSecrets(scopeId, true).name
        ConfigScopeType.ACTOR -> resolveActorName(scopeId)
      }

    @VisibleForTesting
    fun assertCreateRelatedRecordsExist(scopedConfigurationCreate: ScopedConfigurationCreateRequestBody) {
      try {
        getResourceName(ConfigResourceType.fromValue(scopedConfigurationCreate.resourceType), UUID.fromString(scopedConfigurationCreate.resourceId))
        getScopeName(ConfigScopeType.fromValue(scopedConfigurationCreate.scopeType), UUID.fromString(scopedConfigurationCreate.scopeId))
        getOriginName(ConfigOriginType.fromValue(scopedConfigurationCreate.originType), scopedConfigurationCreate.origin)
        getValueName(scopedConfigurationCreate.configKey, scopedConfigurationCreate.value)
      } catch (e: Exception) {
        when (e) {
          is ConfigNotFoundException,
          is io.airbyte.config.persistence.ConfigNotFoundException,
          is NoSuchElementException,
          -> throw BadRequestException(e.message)

          else -> throw e
        }
      }
    }

    @VisibleForTesting
    fun buildScopedConfigurationRead(scopedConfiguration: ScopedConfiguration): ScopedConfigurationRead =
      ScopedConfigurationRead()
        .id(scopedConfiguration.id.toString())
        .configKey(scopedConfiguration.key)
        .value(scopedConfiguration.value)
        .valueName(getValueName(scopedConfiguration.key, scopedConfiguration.value))
        .description(scopedConfiguration.description)
        .referenceUrl(scopedConfiguration.referenceUrl)
        .resourceType(scopedConfiguration.resourceType.toString())
        .resourceId(scopedConfiguration.resourceId.toString())
        .resourceName(getResourceName(scopedConfiguration.resourceType, scopedConfiguration.resourceId))
        .scopeType(scopedConfiguration.scopeType.toString())
        .scopeId(scopedConfiguration.scopeId.toString())
        .scopeName(getScopeName(scopedConfiguration.scopeType, scopedConfiguration.scopeId))
        .originType(scopedConfiguration.originType.toString())
        .origin(scopedConfiguration.origin)
        .originName(getOriginName(scopedConfiguration.originType, scopedConfiguration.origin))
        .updatedAt(scopedConfiguration.updatedAt?.let { unixTimestampToOffsetDateTime(it) })
        .createdAt(scopedConfiguration.createdAt?.let { unixTimestampToOffsetDateTime(it) })
        .expiresAt(scopedConfiguration.expiresAt?.let { LocalDate.parse(it) })

    @VisibleForTesting
    fun buildScopedConfiguration(scopedConfigurationCreate: ScopedConfigurationCreateRequestBody): ScopedConfiguration =
      ScopedConfiguration()
        .withId(uuidGenerator.get())
        .withValue(scopedConfigurationCreate.value)
        .withKey(scopedConfigurationCreate.configKey)
        .withDescription(scopedConfigurationCreate.description)
        .withReferenceUrl(scopedConfigurationCreate.referenceUrl)
        .withResourceId(UUID.fromString(scopedConfigurationCreate.resourceId))
        .withResourceType(ConfigResourceType.fromValue(scopedConfigurationCreate.resourceType))
        .withScopeId(UUID.fromString(scopedConfigurationCreate.scopeId))
        .withScopeType(ConfigScopeType.fromValue(scopedConfigurationCreate.scopeType))
        .withOrigin(scopedConfigurationCreate.origin)
        .withOriginType(ConfigOriginType.fromValue(scopedConfigurationCreate.originType))
        .withExpiresAt(scopedConfigurationCreate.expiresAt?.toString())

    fun listScopedConfigurations(configKey: String): List<ScopedConfigurationRead> {
      val scopedConfigurations: List<ScopedConfiguration> = scopedConfigurationService.listScopedConfigurations(configKey)
      return scopedConfigurations
        .stream()
        .map { scopedConfiguration: ScopedConfiguration ->
          buildScopedConfigurationRead(scopedConfiguration)
        }.collect(Collectors.toList())
    }

    fun listScopedConfigurations(originType: ConfigOriginType): List<ScopedConfigurationRead> {
      val scopedConfigurations: List<ScopedConfiguration> = scopedConfigurationService.listScopedConfigurations(originType)
      return scopedConfigurations
        .stream()
        .map { scopedConfiguration: ScopedConfiguration ->
          buildScopedConfigurationRead(scopedConfiguration)
        }.collect(Collectors.toList())
    }

    fun insertScopedConfiguration(scopedConfigurationCreate: ScopedConfigurationCreateRequestBody): ScopedConfigurationRead {
      assertCreateRelatedRecordsExist(scopedConfigurationCreate)

      val scopedConfiguration = buildScopedConfiguration(scopedConfigurationCreate)
      val insertedScopedConfiguration = scopedConfigurationService.writeScopedConfiguration(scopedConfiguration)

      return buildScopedConfigurationRead(insertedScopedConfiguration)
    }

    fun getScopedConfiguration(id: UUID): ScopedConfigurationRead {
      val scopedConfiguration = scopedConfigurationService.getScopedConfiguration(id)
      return buildScopedConfigurationRead(scopedConfiguration)
    }

    fun updateScopedConfiguration(
      id: UUID,
      scopedConfigurationCreate: ScopedConfigurationCreateRequestBody,
    ): ScopedConfigurationRead {
      assertCreateRelatedRecordsExist(scopedConfigurationCreate)
      val scopedConfiguration = buildScopedConfiguration(scopedConfigurationCreate).withId(id)
      val updatedScopedConfiguration = scopedConfigurationService.writeScopedConfiguration(scopedConfiguration)
      return buildScopedConfigurationRead(updatedScopedConfiguration)
    }

    fun deleteScopedConfiguration(id: UUID) {
      scopedConfigurationService.deleteScopedConfiguration(id)
    }

    fun getScopedConfigurationContext(contextRequestBody: ScopedConfigurationContextRequestBody): ScopedConfigurationContextResponse {
      val key = contextRequestBody.configKey
      val resourceType = ConfigResourceType.fromValue(contextRequestBody.resourceType)
      val resourceId = contextRequestBody.resourceId
      val scopeType = ConfigScopeType.fromValue(contextRequestBody.scopeType)
      val scopeId = contextRequestBody.scopeId

      val configKey =
        ScopedConfigurationKeys[key]
          ?: throw BadRequestException("Config key $key is not supported")

      val ancestorScopes = scopeRelationshipResolver.getAllAncestorScopes(configKey.supportedScopes, scopeType, scopeId)
      val descendantScopes = scopeRelationshipResolver.getAllDescendantScopes(configKey.supportedScopes, scopeType, scopeId)

      val currentScopeMap = mapOf(scopeType to scopeId) + ancestorScopes
      val currentActiveConfig = scopedConfigurationService.getScopedConfiguration(configKey, resourceType, resourceId, currentScopeMap)

      val ancestorConfigs =
        ancestorScopes.flatMap {
          scopedConfigurationService.listScopedConfigurationsWithScopes(key, resourceType, resourceId, it.key, listOf(it.value))
        }
      val descendantConfigs =
        descendantScopes.flatMap {
          scopedConfigurationService.listScopedConfigurationsWithScopes(key, resourceType, resourceId, it.key, it.value)
        }

      return ScopedConfigurationContextResponse()
        .activeConfiguration(currentActiveConfig.map { buildScopedConfigurationRead(it) }.orElse(null))
        .ancestorConfigurations(ancestorConfigs.map { buildScopedConfigurationRead(it) })
        .descendantConfigurations(descendantConfigs.map { buildScopedConfigurationRead(it) })
    }
  }
