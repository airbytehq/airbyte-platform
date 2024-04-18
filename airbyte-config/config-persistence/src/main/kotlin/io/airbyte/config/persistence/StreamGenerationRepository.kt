package io.airbyte.config.persistence

import io.airbyte.config.persistence.domain.Generation
import io.airbyte.config.persistence.domain.StreamGeneration
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface StreamGenerationRepository : PageableRepository<StreamGeneration, UUID> {
  fun findByConnectionId(connectionId: UUID): List<StreamGeneration>

  fun deleteByConnectionId(connectionId: UUID)

  @Query(
    value = """
            SELECT stream_name, stream_namespace, MAX(generation_id) as generation_id
            FROM stream_generation 
            WHERE connection_id = :connectionId 
            GROUP BY (stream_name, stream_namespace)
        """,
  )
  fun getMaxGenerationOfStreamsForConnectionId(connectionId: UUID): List<Generation>
}
