/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ScimConfiguration
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.time.OffsetDateTime
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface ScimConfigurationRepository : PageableRepository<ScimConfiguration, UUID> {
  fun findByOrganizationId(organizationId: UUID): ScimConfiguration?

  @Query(
    """
    SELECT * FROM scim_configuration
    WHERE token_hash = :tokenHash
      AND enabled = TRUE
    """,
  )
  fun findEnabledByTokenHash(tokenHash: String): ScimConfiguration?

  @Query(
    """
    SELECT * FROM scim_configuration
    WHERE organization_id = :organizationId
    FOR UPDATE
    """,
  )
  fun findByOrganizationIdForUpdate(organizationId: UUID): ScimConfiguration?

  @Query(
    """
    SELECT * FROM scim_configuration
    WHERE id = :id
      AND organization_id = :organizationId
    FOR UPDATE
    """,
  )
  fun findByIdAndOrganizationIdForUpdate(
    id: UUID,
    organizationId: UUID,
  ): ScimConfiguration?

  @Query(
    """
    UPDATE scim_configuration
    SET token_hash = :tokenHash,
        token_issued_at = :tokenIssuedAt,
        token_issued_by_user_id = :tokenIssuedByUserId,
        updated_at = :updatedAt
    WHERE id = :id
      AND organization_id = :organizationId
    """,
  )
  fun rotateTokenByIdAndOrganizationId(
    id: UUID,
    organizationId: UUID,
    tokenHash: String,
    tokenIssuedAt: OffsetDateTime,
    tokenIssuedByUserId: UUID,
    updatedAt: OffsetDateTime,
  ): Long

  @Query(
    """
    UPDATE scim_configuration
    SET enabled = FALSE,
        token_hash = NULL,
        token_issued_at = NULL,
        token_issued_by_user_id = NULL,
        disabled_at = :disabledAt,
        disabled_by_user_id = :disabledByUserId,
        updated_at = :updatedAt
    WHERE id = :id
      AND organization_id = :organizationId
    """,
  )
  fun disableByIdAndOrganizationId(
    id: UUID,
    organizationId: UUID,
    disabledAt: OffsetDateTime,
    disabledByUserId: UUID,
    updatedAt: OffsetDateTime,
  ): Long
}
