/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.OrganizationDomainVerification
import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationStatus
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface OrganizationDomainVerificationRepository : PageableRepository<OrganizationDomainVerification, UUID> {
  @Query(
    """
      SELECT * FROM organization_domain_verification
      WHERE organization_id = :organizationId
      AND (:includeDeleted = true OR tombstone = false)
      ORDER BY created_at DESC
      """,
  )
  fun findByOrganizationId(
    organizationId: UUID,
    includeDeleted: Boolean,
  ): List<OrganizationDomainVerification>

  /**
   * Finds a specific domain verification by organization and domain.
   *
   * This method will always return _at most_ one non-tombstoned record. This is enforced by a partial
   * uniqueness index `ON (organization_id, domain) WHERE tombstone = false`.
   *
   * If `includeDeleted = true`, this method will return _0 or more_ tombstoned records.
   *
   * If the (org, domain) pair has no records, this method will return an empty list.
   */
  @Query(
    """
      SELECT * FROM organization_domain_verification
      WHERE organization_id = :organizationId
      AND domain = :domain
      AND (:includeDeleted = true OR tombstone = false)
      """,
  )
  fun findByOrganizationIdAndDomain(
    organizationId: UUID,
    domain: String,
    includeDeleted: Boolean,
  ): List<OrganizationDomainVerification>

  @Query(
    """
      SELECT * FROM organization_domain_verification
      WHERE status = :status
      AND (:includeDeleted = true OR tombstone = false)
      ORDER BY created_at DESC
      """,
  )
  fun findByStatus(
    status: DomainVerificationStatus,
    includeDeleted: Boolean,
  ): List<OrganizationDomainVerification>
}
