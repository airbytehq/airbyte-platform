/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.OrganizationEmailDomain
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface OrganizationEmailDomainRepository : PageableRepository<OrganizationEmailDomain, UUID> {
  /**
   * Returns email-domain claims belonging to active organizations only. Domains claimed by
   * tombstoned organizations are excluded so a deleted org cannot keep restricting logins
   * or block a new org from claiming the domain.
   */
  @Query(
    """
    SELECT organization_email_domain.* FROM organization_email_domain
    INNER JOIN organization
    ON organization.id = organization_email_domain.organization_id
    WHERE lower(organization_email_domain.email_domain) = lower(:emailDomain)
    AND organization.tombstone = false
    """,
  )
  fun findByEmailDomainIgnoreCase(emailDomain: String): List<OrganizationEmailDomain>

  fun deleteByOrganizationId(organizationId: UUID)

  fun findByOrganizationId(organizationId: UUID): List<OrganizationEmailDomain>

  fun existsByOrganizationIdAndEmailDomain(
    organizationId: UUID,
    emailDomain: String,
  ): Boolean

  fun existsByOrganizationIdAndEmailDomainIgnoreCase(
    organizationId: UUID,
    emailDomain: String,
  ): Boolean

  fun findByOrganizationIdAndEmailDomain(
    organizationId: UUID,
    emailDomain: String,
  ): OrganizationEmailDomain?
}
