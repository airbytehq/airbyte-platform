/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ScopedConfiguration
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigOriginType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigResourceType
import io.airbyte.db.instance.configs.jooq.generated.enums.ConfigScopeType
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
