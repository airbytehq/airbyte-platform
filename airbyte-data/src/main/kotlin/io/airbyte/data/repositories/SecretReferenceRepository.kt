/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.SecretReference
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretReferenceScopeType
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface SecretReferenceRepository : PageableRepository<SecretReference, UUID> {
  fun listByScopeTypeAndScopeId(
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
  ): List<SecretReference>

  fun deleteByScopeTypeAndScopeId(
    scopeType: SecretReferenceScopeType,
    id: UUID,
  )

  fun deleteByScopeTypeAndScopeIdAndHydrationPath(
    scopeType: SecretReferenceScopeType,
    scopeId: UUID,
    hydrationPath: String?,
  )
}
