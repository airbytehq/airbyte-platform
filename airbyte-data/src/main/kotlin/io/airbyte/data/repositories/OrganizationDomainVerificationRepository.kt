/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.OrganizationDomainVerification
import io.airbyte.db.instance.configs.jooq.generated.enums.DomainVerificationStatus
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface OrganizationDomainVerificationRepository : PageableRepository<OrganizationDomainVerification, UUID> {
  fun findByOrganizationId(organizationId: UUID): List<OrganizationDomainVerification>

  fun findByOrganizationIdAndDomain(
    organizationId: UUID,
    domain: String,
  ): OrganizationDomainVerification?

  fun findByStatus(status: DomainVerificationStatus): List<OrganizationDomainVerification>
}
