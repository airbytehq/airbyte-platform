package io.airbyte.data.services.impls.data.mappers

import io.airbyte.data.repositories.entities.ScopedConfiguration
import java.sql.Date

typealias EntityConfigScopeType = io.airbyte.db.instance.configs.jooq.generated.enums.ConfigScopeType
typealias ModelConfigScopeType = io.airbyte.config.ConfigScopeType
typealias EntityConfigResourceType = io.airbyte.db.instance.configs.jooq.generated.enums.ConfigResourceType
typealias ModelConfigResourceType = io.airbyte.config.ConfigResourceType
typealias EntityConfigOriginType = io.airbyte.db.instance.configs.jooq.generated.enums.ConfigOriginType
typealias ModelConfigOriginType = io.airbyte.config.ConfigOriginType
typealias EntityScopedConfiguration = ScopedConfiguration
typealias ModelScopedConfiguration = io.airbyte.config.ScopedConfiguration

fun EntityConfigScopeType.toConfigModel(): ModelConfigScopeType {
  return when (this) {
    EntityConfigScopeType.organization -> ModelConfigScopeType.ORGANIZATION
    EntityConfigScopeType.workspace -> ModelConfigScopeType.WORKSPACE
    EntityConfigScopeType.actor -> ModelConfigScopeType.ACTOR
  }
}

fun ModelConfigScopeType.toEntity(): EntityConfigScopeType {
  return when (this) {
    ModelConfigScopeType.ORGANIZATION -> EntityConfigScopeType.organization
    ModelConfigScopeType.WORKSPACE -> EntityConfigScopeType.workspace
    ModelConfigScopeType.ACTOR -> EntityConfigScopeType.actor
  }
}

fun EntityConfigResourceType.toConfigModel(): ModelConfigResourceType {
  return when (this) {
    EntityConfigResourceType.actor_definition -> ModelConfigResourceType.ACTOR_DEFINITION
  }
}

fun ModelConfigResourceType.toEntity(): EntityConfigResourceType {
  return when (this) {
    ModelConfigResourceType.ACTOR_DEFINITION -> EntityConfigResourceType.actor_definition
  }
}

fun EntityConfigOriginType.toConfigModel(): ModelConfigOriginType {
  return when (this) {
    EntityConfigOriginType.user -> ModelConfigOriginType.USER
  }
}

fun ModelConfigOriginType.toEntity(): EntityConfigOriginType {
  return when (this) {
    ModelConfigOriginType.USER -> EntityConfigOriginType.user
  }
}

fun EntityScopedConfiguration.toConfigModel(): ModelScopedConfiguration {
  return ModelScopedConfiguration()
    .withId(this.id)
    .withKey(this.key)
    .withValue(this.value)
    .withScopeType(this.scopeType.toConfigModel())
    .withScopeId(this.scopeId)
    .withResourceType(this.resourceType.toConfigModel())
    .withResourceId(this.resourceId)
    .withOriginType(this.originType.toConfigModel())
    .withOrigin(this.origin)
    .withDescription(this.description)
    .withReferenceUrl(this.referenceUrl)
    .withExpiresAt(this.expiresAt?.toString())
}

fun ModelScopedConfiguration.toEntity(): EntityScopedConfiguration {
  return EntityScopedConfiguration(
    id = this.id,
    key = this.key,
    value = this.value,
    scopeType = this.scopeType.toEntity(),
    scopeId = this.scopeId,
    resourceType = this.resourceType.toEntity(),
    resourceId = this.resourceId,
    originType = this.originType.toEntity(),
    origin = this.origin,
    description = this.description,
    referenceUrl = this.referenceUrl,
    expiresAt = this.expiresAt?.let { Date.valueOf(it) },
  )
}
