/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ScopedConfiguration
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigOriginType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigResourceType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigScopeType
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface ScopedConfigurationRepository : PageableRepository<ScopedConfiguration, UUID> {
  fun getByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeId(
    key: String,
    resourceType: ConfigResourceType?,
    resourceId: UUID?,
    scopeType: ConfigScopeType,
    scopeId: UUID,
  ): ScopedConfiguration?

  fun findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndScopeIdInList(
    key: String,
    resourceType: ConfigResourceType?,
    resourceId: UUID?,
    scopeType: ConfigScopeType,
    scopeId: List<UUID>,
  ): List<ScopedConfiguration>

  fun findByKeyAndResourceTypeAndResourceIdAndOriginTypeAndOriginInList(
    key: String,
    resourceType: ConfigResourceType?,
    resourceId: UUID?,
    originType: ConfigOriginType,
    origins: List<String>,
  ): List<ScopedConfiguration>

  fun findByKeyAndResourceTypeAndResourceIdAndScopeTypeAndOriginTypeAndValueInList(
    key: String,
    resourceType: ConfigResourceType?,
    resourceId: UUID?,
    scopeType: ConfigScopeType,
    originType: ConfigOriginType,
    values: List<String>,
  ): List<ScopedConfiguration>

  fun findByKeyAndResourceTypeAndScopeTypeAndScopeId(
    key: String,
    configResourceType: ConfigResourceType?,
    configScopeType: ConfigScopeType?,
    scopeId: UUID,
  ): List<ScopedConfiguration>

  fun findByKeyAndScopeTypeAndScopeId(
    key: String,
    configScopeType: ConfigScopeType,
    scopeId: UUID,
  ): List<ScopedConfiguration>

  fun findByKey(key: String): List<ScopedConfiguration>

  fun findByOriginType(originType: ConfigOriginType): List<ScopedConfiguration>

  fun deleteByIdInList(ids: List<UUID>)

  @Query(
    """
    INSERT INTO scoped_configuration (id, key, value, scope_type, scope_id, resource_type, resource_id, origin_type, origin, description, reference_url, expires_at, created_at, updated_at)
    VALUES (:id, :key, :value, :scopeType, :scopeId, :resourceType, :resourceId, :originType, :origin, :description, :referenceUrl, :expiresAt, now(), now())
    ON CONFLICT (key, resource_type, resource_id, scope_type, scope_id)
    DO UPDATE SET value = :value, origin_type = :originType, origin = :origin, description = :description, reference_url = :referenceUrl, expires_at = :expiresAt, updated_at = now()
    """,
  )
  fun upsertByNaturalKey(
    id: UUID,
    key: String,
    value: String,
    scopeType: ConfigScopeType,
    scopeId: UUID,
    resourceType: ConfigResourceType?,
    resourceId: UUID?,
    originType: ConfigOriginType,
    origin: String,
    description: String?,
    referenceUrl: String?,
    expiresAt: java.util.Date?,
  )

  fun updateByKeyAndResourceTypeAndResourceIdAndOriginTypeAndOriginIn(
    key: String,
    resourceType: ConfigResourceType,
    resourceId: UUID,
    originType: ConfigOriginType,
    origins: List<String>,
    origin: String,
    value: String,
  )
}
