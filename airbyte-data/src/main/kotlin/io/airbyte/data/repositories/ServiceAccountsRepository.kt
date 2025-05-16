/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ServiceAccount
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
internal interface ServiceAccountsRepository : PageableRepository<ServiceAccount, UUID> {
  fun findOne(
    id: UUID,
    secret: String,
  ): ServiceAccount?

  fun findOne(
    name: String,
    managed: Boolean,
  ): ServiceAccount?
}
