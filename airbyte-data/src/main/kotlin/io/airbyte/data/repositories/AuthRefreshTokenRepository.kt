/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.AuthRefreshToken
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.CrudRepository

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface AuthRefreshTokenRepository : CrudRepository<AuthRefreshToken, String> {
  fun findByValue(value: String): AuthRefreshToken?

  fun findBySessionId(sessionId: String): List<AuthRefreshToken>
}
