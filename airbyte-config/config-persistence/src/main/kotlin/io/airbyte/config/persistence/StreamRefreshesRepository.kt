package io.airbyte.config.persistence

import io.airbyte.config.persistence.domain.StreamRefresh
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface StreamRefreshesRepository : PageableRepository<StreamRefresh, UUID> {
  fun findByConnectionId(connectionId: UUID): List<StreamRefresh>

  fun deleteByConnectionId(connectionId: UUID)

  @Query(
    value = """
            DELETE FROM stream_refreshes 
            WHERE connection_id = :connectionId 
            AND stream_name = :streamName
            AND ((:streamNamespace) IS NULL OR stream_namespace = :streamNamespace)
        """,
  )
  fun deleteByConnectionIdAndStreamNameAndStreamNamespace(
    connectionId: UUID,
    streamName: String,
    streamNamespace: String?,
  )

  fun existsByConnectionId(connectionId: UUID): Boolean
}
