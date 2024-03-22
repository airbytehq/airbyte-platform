package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.ConnectionTimelineEvent
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface ConnectionTimelineEventRepository : PageableRepository<ConnectionTimelineEvent, UUID> {
  fun findByConnectionId(connectionId: UUID): List<ConnectionTimelineEvent>
}
