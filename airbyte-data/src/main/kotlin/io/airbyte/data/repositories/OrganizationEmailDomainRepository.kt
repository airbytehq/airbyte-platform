/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.OrganizationEmailDomain
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface OrganizationEmailDomainRepository : PageableRepository<OrganizationEmailDomain, UUID> {
  fun findByEmailDomainIgnoreCase(emailDomain: String): List<OrganizationEmailDomain>

  fun deleteByOrganizationId(organizationId: UUID)

  fun findByOrganizationId(organizationId: UUID): List<OrganizationEmailDomain>

  fun existsByOrganizationIdAndEmailDomain(
    organizationId: UUID,
    emailDomain: String,
  ): Boolean

  fun findByOrganizationIdAndEmailDomain(
    organizationId: UUID,
    emailDomain: String,
  ): OrganizationEmailDomain?
}
