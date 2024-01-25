/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.server.handlers

import com.google.common.annotations.VisibleForTesting
import io.airbyte.api.model.generated.ScopedConfigurationCreateRequestBody
import io.airbyte.api.model.generated.ScopedConfigurationRead
import io.airbyte.commons.server.errors.BadRequestException
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
class ScopedConfigurationHandler
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
  ) {
    private fun getValueName(
      configKey: String,
      value: String,
    ): String? {
      return when (configKey) {
        ConnectorVersionKey.key -> actorDefinitionService.getActorDefinitionVersion(UUID.fromString(value)).dockerImageTag
        else -> null
      }
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

    private fun getResourceName(
      resourceType: ConfigResourceType,
      resourceId: UUID,
    ): String {
      return when (resourceType) {
        ConfigResourceType.ACTOR_DEFINITION -> resolveActorDefinitionName(resourceId)
      }
    }

    private fun getOriginName(
      originType: ConfigOriginType,
      origin: String,
    ): String? {
      return when (originType) {
        ConfigOriginType.USER -> userPersistence.getUser(UUID.fromString(origin)).get().email
        else -> null
      }
    }

    private fun getScopeName(
      scopeType: ConfigScopeType,
      scopeId: UUID,
    ): String {
      return when (scopeType) {
        ConfigScopeType.ORGANIZATION -> organizationService.getOrganization(scopeId).get().name
        ConfigScopeType.WORKSPACE -> workspaceService.getStandardWorkspaceNoSecrets(scopeId, true).name
        ConfigScopeType.ACTOR -> resolveActorName(scopeId)
      }
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
    fun buildScopedConfigurationRead(scopedConfiguration: ScopedConfiguration): ScopedConfigurationRead {
      return ScopedConfigurationRead()
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
    }

    @VisibleForTesting
    fun buildScopedConfiguration(scopedConfigurationCreate: ScopedConfigurationCreateRequestBody): ScopedConfiguration {
      return ScopedConfiguration()
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
    }

    fun listScopedConfigurations(): List<ScopedConfigurationRead> {
      val scopedConfigurations: List<ScopedConfiguration> = scopedConfigurationService.listScopedConfigurations()
      return scopedConfigurations.stream().map {
          scopedConfiguration: ScopedConfiguration ->
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
  }
