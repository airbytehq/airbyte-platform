/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Application
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface ApplicationRepository : PageableRepository<Application, UUID> {
  fun findByAuthUserId(authUserId: String): List<Application>

  fun findByAuthUserIdAndId(
    authUserId: String,
    applicationId: UUID,
  ): Application?

  fun findByClientIdAndClientSecret(
    clientId: String?,
    clientSecret: String?,
  ): Application?
}
