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

  @Query(
    """
    SELECT organization.* from organization
    INNER JOIN workspace
    ON organization.id = workspace.organization_id
    INNER JOIN actor
    ON actor.workspace_id = workspace.id
    INNER JOIN connection
    ON connection.source_id = actor.id
    WHERE connection.id = :connectionId
    """,
  )
  fun findByConnectionId(connectionId: UUID): Optional<Organization>

  @Query(
    """
    SELECT organization.* from organization
    INNER JOIN sso_config
    ON organization.id = sso_config.organization_id
    WHERE sso_config.keycloak_realm = :ssoConfigRealm
    """,
  )
  fun findBySsoConfigRealm(ssoConfigRealm: String): Optional<Organization>

  @Query(
    """
    SELECT DISTINCT organization.* FROM organization
    LEFT JOIN sso_config ON organization.id = sso_config.organization_id
    WHERE (organization.id IN (
        SELECT DISTINCT organization.id FROM organization
        INNER JOIN permission ON organization.id = permission.organization_id
        WHERE permission.user_id = :userId AND permission.organization_id IS NOT NULL
    ) OR organization.id IN (
        SELECT DISTINCT organization.id FROM organization
        INNER JOIN workspace ON organization.id = workspace.organization_id
        INNER JOIN permission ON workspace.id = permission.workspace_id
        WHERE permission.user_id = :userId AND permission.workspace_id IS NOT NULL
    ) OR EXISTS (
        SELECT 1 FROM permission
        WHERE permission.user_id = :userId
        AND permission.permission_type = 'instance_admin'
    ))
    AND (:includeDeleted = true OR organization.tombstone = false)
    AND (:keyword IS NULL OR LOWER(organization.name) LIKE LOWER(CONCAT('%', :keyword, '%')))
    ORDER BY organization.name ASC
    LIMIT :limit OFFSET :offset
    """,
  )
  fun findByUserIdPaginated(
    userId: UUID,
    keyword: String?,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
  ): List<Organization>

  @Query(
    """
    SELECT DISTINCT organization.* FROM organization
    LEFT JOIN sso_config ON organization.id = sso_config.organization_id
    WHERE (organization.id IN (
        SELECT DISTINCT organization.id FROM organization
        INNER JOIN permission ON organization.id = permission.organization_id
        WHERE permission.user_id = :userId AND permission.organization_id IS NOT NULL
    ) OR organization.id IN (
        SELECT DISTINCT organization.id FROM organization
        INNER JOIN workspace ON organization.id = workspace.organization_id
        INNER JOIN permission ON workspace.id = permission.workspace_id
        WHERE permission.user_id = :userId AND permission.workspace_id IS NOT NULL
    ) OR EXISTS (
        SELECT 1 FROM permission
        WHERE permission.user_id = :userId
        AND permission.permission_type = 'instance_admin'
    ))
    AND (:includeDeleted = true OR organization.tombstone = false)
    AND (:keyword IS NULL OR LOWER(organization.name) LIKE LOWER(CONCAT('%', :keyword, '%')))
    ORDER BY organization.name ASC
    """,
  )
  fun findByUserId(
    userId: UUID,
    keyword: String?,
    includeDeleted: Boolean,
  ): List<Organization>
}
