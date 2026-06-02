/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Organization
import io.airbyte.data.repositories.entities.OrganizationWithSsoRealm
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.Optional
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface OrganizationRepository : PageableRepository<Organization, UUID> {
  /**
   * Locks the organization row for the duration of the surrounding transaction.
   *
   * This is used to serialize org-scoped Data Worker capacity admission so concurrent reservations
   * cannot oversubscribe committed capacity.
   */
  @Query(
    """
    SELECT * FROM organization
    WHERE id = :organizationId
    FOR UPDATE
    """,
  )
  fun findByIdForUpdate(organizationId: UUID): Optional<Organization>

  /**
   * Returns organizations whose `email` column matches (case-insensitively) the given email.
   *
   * The organization `email` field is populated at organization-creation time with the creator's
   * email; the GDPR / DSR runbook uses this column to identify the orgs "owned by" a user. Used
   * by `DsrDeletionService` to build the deletion manifest.
   */
  @Query(
    """
    SELECT * FROM organization
    WHERE lower(email) = lower(:email)
    """,
  )
  fun findByEmailIgnoreCase(email: String): List<Organization>

  @Query(
    """
    UPDATE organization
    SET is_agentic = :isAgentic, updated_at = NOW()
    WHERE id = :organizationId AND tombstone = false
    """,
  )
  fun updateAgenticStatusById(
    organizationId: UUID,
    isAgentic: Boolean,
  ): Long

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

  /**
   * Finds organizations accessible by a user with SSO realm information.
   * Note: sso_config has a unique constraint on organization_id, so at most one SSO config per organization.
   */
  @Query(
    """
    SELECT
      organization.id,
      organization.name,
      organization.user_id,
      organization.email,
      organization.tombstone,
      organization.is_agentic,
      organization.created_at,
      organization.updated_at,
      sso_config.keycloak_realm
    FROM organization
    LEFT JOIN sso_config ON organization.id = sso_config.organization_id
    WHERE (EXISTS (
        SELECT 1 FROM permission
        WHERE permission.user_id = :userId
        AND permission.organization_id = organization.id
    ) OR EXISTS (
        SELECT 1 FROM workspace
        INNER JOIN permission ON workspace.id = permission.workspace_id
        WHERE permission.user_id = :userId
        AND workspace.organization_id = organization.id
    ) OR EXISTS (
        SELECT 1 FROM permission
        WHERE permission.user_id = :userId
        AND permission.permission_type = 'instance_admin'
    ))
    AND (:includeDeleted = true OR organization.tombstone = false)
    AND (:keyword IS NULL OR organization.name ILIKE CONCAT('%', :keyword, '%'))
    ORDER BY organization.name ASC
    """,
  )
  fun findByUserIdWithSsoRealm(
    userId: UUID,
    keyword: String?,
    includeDeleted: Boolean,
  ): List<OrganizationWithSsoRealm>

  /**
   * Finds organizations accessible by a user with SSO realm information (paginated).
   * Note: sso_config has a unique constraint on organization_id, so at most one SSO config per organization.
   */
  @Query(
    """
    SELECT
      organization.id,
      organization.name,
      organization.user_id,
      organization.email,
      organization.tombstone,
      organization.is_agentic,
      organization.created_at,
      organization.updated_at,
      sso_config.keycloak_realm
    FROM organization
    LEFT JOIN sso_config ON organization.id = sso_config.organization_id
    WHERE (EXISTS (
        SELECT 1 FROM permission
        WHERE permission.user_id = :userId
        AND permission.organization_id = organization.id
    ) OR EXISTS (
        SELECT 1 FROM workspace
        INNER JOIN permission ON workspace.id = permission.workspace_id
        WHERE permission.user_id = :userId
        AND workspace.organization_id = organization.id
    ) OR EXISTS (
        SELECT 1 FROM permission
        WHERE permission.user_id = :userId
        AND permission.permission_type = 'instance_admin'
    ))
    AND (:includeDeleted = true OR organization.tombstone = false)
    AND (:keyword IS NULL OR organization.name ILIKE CONCAT('%', :keyword, '%'))
    ORDER BY organization.name ASC
    LIMIT :limit OFFSET :offset
    """,
  )
  fun findByUserIdPaginatedWithSsoRealm(
    userId: UUID,
    keyword: String?,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
  ): List<OrganizationWithSsoRealm>

  /**
   * Finds non-agentic organizations accessible by a user with SSO realm information.
   *
   * Like [findByUserIdWithSsoRealm] but additionally excludes organizations flagged as agentic
   * (ADP-managed). This is the variant used by the Data Replication org list path, where
   * agentic orgs must stay hidden for non-instance-admin users.
   *
   * The instance-admin branch is intentionally absent — callers route instance admins to
   * [findAllWithSsoRealm] (which skips permission checks and the agentic filter both).
   */
  @Query(
    """
    SELECT
      organization.id,
      organization.name,
      organization.user_id,
      organization.email,
      organization.tombstone,
      organization.is_agentic,
      organization.created_at,
      organization.updated_at,
      sso_config.keycloak_realm
    FROM organization
    LEFT JOIN sso_config ON organization.id = sso_config.organization_id
    WHERE (EXISTS (
        SELECT 1 FROM permission
        WHERE permission.user_id = :userId
        AND permission.organization_id = organization.id
    ) OR EXISTS (
        SELECT 1 FROM workspace
        INNER JOIN permission ON workspace.id = permission.workspace_id
        WHERE permission.user_id = :userId
        AND workspace.organization_id = organization.id
    ))
    AND organization.is_agentic = false
    AND (:includeDeleted = true OR organization.tombstone = false)
    AND (:keyword IS NULL OR organization.name ILIKE CONCAT('%', :keyword, '%'))
    ORDER BY organization.name ASC
    """,
  )
  fun findNonAgenticByUserIdWithSsoRealm(
    userId: UUID,
    keyword: String?,
    includeDeleted: Boolean,
  ): List<OrganizationWithSsoRealm>

  /**
   * Finds non-agentic organizations accessible by a user with SSO realm information (paginated).
   *
   * Like [findByUserIdPaginatedWithSsoRealm] but additionally excludes organizations flagged as
   * agentic (ADP-managed). The filter is applied in SQL so that LIMIT/OFFSET pagination remains
   * accurate for infinite-scroll consumers that use page size to detect end-of-list.
   *
   * The instance-admin branch is intentionally absent — callers route instance admins to
   * [findAllPaginatedWithSsoRealm] (which skips permission checks and the agentic filter both).
   */
  @Query(
    """
    SELECT
      organization.id,
      organization.name,
      organization.user_id,
      organization.email,
      organization.tombstone,
      organization.is_agentic,
      organization.created_at,
      organization.updated_at,
      sso_config.keycloak_realm
    FROM organization
    LEFT JOIN sso_config ON organization.id = sso_config.organization_id
    WHERE (EXISTS (
        SELECT 1 FROM permission
        WHERE permission.user_id = :userId
        AND permission.organization_id = organization.id
    ) OR EXISTS (
        SELECT 1 FROM workspace
        INNER JOIN permission ON workspace.id = permission.workspace_id
        WHERE permission.user_id = :userId
        AND workspace.organization_id = organization.id
    ))
    AND organization.is_agentic = false
    AND (:includeDeleted = true OR organization.tombstone = false)
    AND (:keyword IS NULL OR organization.name ILIKE CONCAT('%', :keyword, '%'))
    ORDER BY organization.name ASC
    LIMIT :limit OFFSET :offset
    """,
  )
  fun findNonAgenticByUserIdPaginatedWithSsoRealm(
    userId: UUID,
    keyword: String?,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
  ): List<OrganizationWithSsoRealm>

  /**
   * Finds all organizations with SSO realm information (for instance admins).
   * This method skips permission checks for better performance.
   */
  @Query(
    """
    SELECT
      organization.id,
      organization.name,
      organization.user_id,
      organization.email,
      organization.tombstone,
      organization.is_agentic,
      organization.created_at,
      organization.updated_at,
      sso_config.keycloak_realm
    FROM organization
    LEFT JOIN sso_config ON organization.id = sso_config.organization_id
    WHERE (:includeDeleted = true OR organization.tombstone = false)
    AND (:keyword IS NULL OR organization.name ILIKE CONCAT('%', :keyword, '%'))
    ORDER BY organization.name ASC
    """,
  )
  fun findAllWithSsoRealm(
    keyword: String?,
    includeDeleted: Boolean,
  ): List<OrganizationWithSsoRealm>

  /**
   * Finds all organizations with SSO realm information (paginated, for instance admins).
   * This method skips permission checks for better performance.
   */
  @Query(
    """
    SELECT
      organization.id,
      organization.name,
      organization.user_id,
      organization.email,
      organization.tombstone,
      organization.is_agentic,
      organization.created_at,
      organization.updated_at,
      sso_config.keycloak_realm
    FROM organization
    LEFT JOIN sso_config ON organization.id = sso_config.organization_id
    WHERE (:includeDeleted = true OR organization.tombstone = false)
    AND (:keyword IS NULL OR organization.name ILIKE CONCAT('%', :keyword, '%'))
    ORDER BY organization.name ASC
    LIMIT :limit OFFSET :offset
    """,
  )
  fun findAllPaginatedWithSsoRealm(
    keyword: String?,
    includeDeleted: Boolean,
    limit: Int,
    offset: Int,
  ): List<OrganizationWithSsoRealm>
}
