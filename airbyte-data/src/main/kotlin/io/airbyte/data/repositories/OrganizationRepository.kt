/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Organization
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.Optional
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface OrganizationRepository : PageableRepository<Organization, UUID> {
  @Query(
    """
    SELECT organization.* from organization
    INNER JOIN workspace
    ON organization.id = workspace.organization_id
    WHERE workspace.id = :workspaceId
    """,
  )
  fun findByWorkspaceId(workspaceId: UUID): Optional<Organization>
}
