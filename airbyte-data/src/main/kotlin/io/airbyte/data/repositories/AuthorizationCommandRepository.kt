/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.AuthorizationCommand
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.GenericRepository
import java.util.Optional
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface AuthorizationCommandRepository : GenericRepository<AuthorizationCommand, String> {
  @Query(
    """
    SELECT c.id, c.workspace_id, c.organization_id FROM commands c
    JOIN workspace ws ON ws.id = c.workspace_id AND ws.organization_id = :organizationId
    WHERE c.id = :id AND c.organization_id = :organizationId
  """,
  )
  fun findByIdAndOrganizationId(
    id: String,
    organizationId: UUID,
  ): Optional<AuthorizationCommand>
}
