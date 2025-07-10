/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.SsoConfig
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface SsoConfigRepository : PageableRepository<SsoConfig, UUID> {
  fun deleteByOrganizationId(organizationId: UUID)

  fun findByOrganizationId(organizationId: UUID): SsoConfig?
}
