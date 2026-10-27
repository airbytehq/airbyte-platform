/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.CommandAuthorizationMetadata
import io.airbyte.data.repositories.entities.WorkloadAuthorizationMetadata
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.GenericRepository
import java.util.Optional
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface CommandAuthorizationRepository : GenericRepository<CommandAuthorizationMetadata, String> {
  // ID-only Config API requests must discover saved ownership before the tenant-filtered read.
  @Query("SELECT id, workload_id, workspace_id, organization_id, command_type FROM commands WHERE id = :id")
  fun findOwnerById(id: String): Optional<CommandAuthorizationMetadata>

  @Query("SELECT id, workload_id, workspace_id, organization_id, command_type FROM commands WHERE id = :id AND organization_id = :organizationId")
  fun findByIdAndOrganizationId(
    id: String,
    organizationId: UUID,
  ): Optional<CommandAuthorizationMetadata>
}

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface WorkloadAuthorizationRepository : GenericRepository<WorkloadAuthorizationMetadata, String> {
  @Query("SELECT id, workspace_id, organization_id, type::text AS type FROM workload WHERE id = :id")
  fun findOwnerById(id: String): Optional<WorkloadAuthorizationMetadata>

  @Query(
    """
    SELECT w.id, w.workspace_id, w.organization_id, w.type::text AS type FROM workload w
    WHERE w.id = :id AND (w.organization_id = :organizationId OR
      (w.organization_id IS NULL AND w.type = 'spec' AND EXISTS
        (SELECT 1 FROM workspace ws WHERE ws.id = w.workspace_id AND ws.organization_id = :organizationId)))
  """,
  )
  fun findByIdAndOrganizationId(
    id: String,
    organizationId: UUID,
  ): Optional<WorkloadAuthorizationMetadata>
}
