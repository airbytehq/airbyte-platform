package io.airbyte.config.persistence

import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.config.persistence.domain.StreamRefreshPK
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES)
interface StreamRefreshesRepository : PageableRepository<StreamRefresh, StreamRefreshPK> {
  fun findByPkConnectionId(connectionId: UUID): List<StreamRefresh>

  fun deleteByPkConnectionId(connectionId: UUID)
}
