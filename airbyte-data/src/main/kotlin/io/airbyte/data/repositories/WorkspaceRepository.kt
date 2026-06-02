/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Workspace
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface WorkspaceRepository : PageableRepository<Workspace, UUID> {
  fun findByNameAndOrganizationIdAndTombstoneFalse(
    name: String,
    organizationId: UUID,
  ): List<Workspace>

  /**
   * Returns workspaces whose `email` column matches (case-insensitively) the given email.
   *
   * The workspace `email` field is populated at workspace-creation time with the creator's email
   * and is what the GDPR / DSR (Data Subject Request) runbook uses to identify the workspaces
   * "owned by" a user. Used by `DsrDeletionService` to build the deletion manifest.
   */
  @io.micronaut.data.annotation.Query(
    """
    SELECT * FROM workspace
    WHERE lower(email) = lower(:email)
    """,
  )
  fun findByEmailIgnoreCase(email: String): List<Workspace>

  /**
   * Returns every workspace attached to one of the supplied organizations.
   *
   * DSR deletion removes the whole organization matched by `organization.email`; every workspace
   * under that organization must be included in the manifest so the later hard-delete does not FK
   * fail on workspaces whose `workspace.email` belongs to another member.
   */
  @io.micronaut.data.annotation.Query(
    """
    SELECT * FROM workspace
    WHERE organization_id IN (:organizationIds)
    """,
  )
  fun findByOrganizationIdIn(organizationIds: List<UUID>): List<Workspace>
}
