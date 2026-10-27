/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.StreamStatusAuthorization
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.GenericRepository
import java.util.Optional
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface StreamStatusAuthorizationRepository : GenericRepository<StreamStatusAuthorization, UUID> {
  @Query(
    "SELECT id, workspace_id, connection_id, job_id, attempt_number, job_type::text AS job_type FROM stream_statuses WHERE id = :id AND workspace_id = :workspaceId",
  )
  fun findByIdAndWorkspaceId(
    id: UUID,
    workspaceId: UUID,
  ): Optional<StreamStatusAuthorization>
}
