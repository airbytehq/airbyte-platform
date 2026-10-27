/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.domain.services.auth

import io.airbyte.config.ConfigOriginType
import io.airbyte.config.ConfigResourceType
import io.airbyte.config.ConfigScopeType
import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.services.ActorDefinitionService
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.data.services.shared.ConnectorVersionKey
import io.airbyte.data.services.shared.ScopedConfigurationKeys
import io.airbyte.data.services.shared.ScopedConfigurationMetadata
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID

/** Semantic references only. Arbitrary config values and external origins are not resource IDs. */
data class ScopedConfigurationReferences(
  val scopeType: ConfigScopeType,
  val scopeId: UUID,
  val definition: Optional<UUID>,
  val user: Optional<UUID>,
  val version: Optional<UUID>,
)

@Singleton
class ScopedConfigurationAuthorizationService(
  private val data: AuthorizationDataService,
  private val configs: ScopedConfigurationService,
  private val definitions: ActorDefinitionService,
) {
  fun isConnectorVersionKey(key: String): Boolean = key == ConnectorVersionKey.key

  fun isSupportedKey(key: String): Boolean = ScopedConfigurationKeys.containsKey(key)

  fun existing(id: UUID): Optional<ScopedConfigurationReferences> {
    val config =
      try {
        configs.getScopedConfigurationMetadata(id)
      } catch (_: ConfigNotFoundException) {
        return Optional.empty()
      }
    requireSavedId(config, id)
    return Optional.of(scopeOnly(config))
  }

  fun read(id: UUID): ScopedConfigurationReferences {
    val config = saved(id)
    val metadata = references(config)
    validate(metadata)
    if (config.key != ConnectorVersionKey.key) return metadata
    requireActorVersionParent(metadata)
    return metadata.copy(version = Optional.of(versionSelector(id)))
  }

  fun deletion(id: UUID): ScopedConfigurationReferences {
    val config = saved(id)
    val scope = scopeOnly(config)
    validate(scope)
    if (config.key != ConnectorVersionKey.key || config.resourceType.filter { it == ConfigResourceType.ACTOR_DEFINITION }.isEmpty) return scope
    val definition = config.resourceId.orElseThrow { AuthorizationResourceNotFoundException() }
    // Without a default or breaking changes, unpinning only removes the saved row.
    val default = definitions.getDefaultVersionForActorDefinitionIdOptional(definition)
    if (default.isEmpty) return scope
    if (default.orElseThrow().actorDefinitionId != definition) throw AuthorizationResourceNotFoundException()
    if (definitions.listBreakingChangesForActorDefinition(definition).isEmpty()) return scope
    val parent = ScopedConfigurationReferences(config.scopeType, config.scopeId, Optional.of(definition), Optional.empty(), Optional.empty())
    validate(parent)
    requireActorVersionParent(parent)
    val version =
      if (config.originType == ConfigOriginType.BREAKING_CHANGE) {
        definitions.getActorDefinitionVersion(definition, config.origin).map { it.versionId }
      } else {
        Optional.of(versionSelector(id))
      }
    return ScopedConfigurationReferences(config.scopeType, config.scopeId, Optional.of(definition), Optional.empty(), version)
  }

  fun validate(
    references: ScopedConfigurationReferences,
    actorRequired: Boolean = false,
  ) {
    when (references.scopeType) {
      ConfigScopeType.ORGANIZATION -> data.organization(references.scopeId)
      ConfigScopeType.WORKSPACE -> data.savedWorkspaceAndOrganization(references.scopeId, true)
      ConfigScopeType.ACTOR -> {
        val owner = data.existingSavedActorWorkspaceAndOrganization(references.scopeId)
        if (actorRequired && owner.isEmpty) throw AuthorizationResourceNotFoundException()
        if (owner.isPresent && references.definition.isPresent && references.version.isPresent &&
          data.savedActorDefinitionId(references.scopeId, owner.orElseThrow().workspaceId) != references.definition.orElseThrow()
        ) {
          throw AuthorizationResourceNotFoundException()
        }
      }
    }
    references.definition.ifPresent(data::requireSavedActorDefinition)
    references.user.ifPresent(data::requireUser)
    references.version.ifPresent { version ->
      val definition = data.savedDefinitionVersionParent(version)
      if (references.definition.isPresent && definition != references.definition.orElseThrow()) throw AuthorizationResourceNotFoundException()
    }
  }

  fun requireActorContext(actorId: UUID) {
    if (data.existingSavedActorWorkspaceAndOrganization(actorId).isEmpty) throw AuthorizationResourceNotFoundException()
  }

  private fun saved(id: UUID): ScopedConfigurationMetadata {
    val config =
      try {
        configs.getScopedConfigurationMetadata(id)
      } catch (_: ConfigNotFoundException) {
        throw AuthorizationResourceNotFoundException()
      }
    requireSavedId(config, id)
    return config
  }

  private fun requireSavedId(
    config: ScopedConfigurationMetadata,
    id: UUID,
  ) {
    if (config.id != id) throw AuthorizationResourceNotFoundException()
  }

  private fun scopeOnly(config: ScopedConfigurationMetadata) =
    ScopedConfigurationReferences(config.scopeType, config.scopeId, Optional.empty(), Optional.empty(), Optional.empty())

  private fun references(config: ScopedConfigurationMetadata) =
    ScopedConfigurationReferences(
      config.scopeType,
      config.scopeId,
      if (config.resourceType
          .filter {
            it == ConfigResourceType.ACTOR_DEFINITION
          }.isPresent
      ) {
        config.resourceId
      } else {
        Optional.empty()
      },
      if (config.originType == ConfigOriginType.USER) Optional.of(UUID.fromString(config.origin)) else Optional.empty(),
      Optional.empty(),
    )

  private fun versionSelector(id: UUID): UUID =
    try {
      UUID.fromString(configs.getConnectorVersionSelector(id))
    } catch (_: ConfigNotFoundException) {
      throw AuthorizationResourceNotFoundException()
    }

  private fun requireActorVersionParent(references: ScopedConfigurationReferences) {
    if (references.scopeType != ConfigScopeType.ACTOR || references.definition.isEmpty) return
    data.existingSavedActorWorkspaceAndOrganization(references.scopeId).ifPresent { owner ->
      if (data.savedActorDefinitionId(references.scopeId, owner.workspaceId) !=
        references.definition.orElseThrow()
      ) {
        throw AuthorizationResourceNotFoundException()
      }
    }
  }
}
