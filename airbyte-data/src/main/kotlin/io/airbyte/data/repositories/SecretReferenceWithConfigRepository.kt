/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.SecretReferenceWithConfig
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretReferenceScopeType
import io.micronaut.data.annotation.Join
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface SecretReferenceWithConfigRepository : PageableRepository<SecretReferenceWithConfig, UUID> {
  @Join(value = "secretConfig", type = Join.Type.FETCH)
  fun listByScopeTypeAndScopeId(
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
  ): List<SecretReferenceWithConfig>

  @Join(value = "secretConfig", type = Join.Type.FETCH)
  fun listByScopeTypeAndScopeIdIn(
    scopeType: SecretReferenceScopeType,
    scopeId: List<UUID>,
  ): List<SecretReferenceWithConfig>
}
