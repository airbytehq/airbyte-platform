/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.SecretStorage
import io.airbyte.db.instance.configs.jooq.generated.enums.SecretStorageScopeType
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface SecretStorageRepository : PageableRepository<SecretStorage, UUID> {
  fun listByScopeTypeAndScopeId(
    scopeType: SecretStorageScopeType,
    scopeId: UUID,
  ): List<SecretStorage>
}
