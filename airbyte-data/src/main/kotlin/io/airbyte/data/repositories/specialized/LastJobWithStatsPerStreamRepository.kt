package io.airbyte.data.repositories.specialized

import io.micronaut.core.annotation.Introspected
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.GenericRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface LastJobWithStatsPerStreamRepository : GenericRepository<StreamWithLastJobId, Long> {
  /**
   * Given a connectionId and list of stream names/namespaces, fetch the latest job ID that has
   * stream_stats for each stream in that connection.
   */
  @Query(
    """
      SELECT
          max(job_id) as job_id,
          stream_namespace,
          stream_name
      FROM
          stream_statuses ss 
      WHERE
          connection_id = :connectionId
          and run_state in ('complete', 'incomplete')
      GROUP BY
          stream_namespace,
          stream_name;
    """,
    readOnly = true,
  )
  fun findLastJobIdWithStatsPerStream(connectionId: UUID): List<StreamWithLastJobId>
}

@Introspected
data class StreamWithLastJobId(
  val jobId: Long,
  val streamName: String,
  val streamNamespace: String?,
)
