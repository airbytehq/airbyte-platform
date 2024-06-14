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
        WITH stream_pairs AS (
            SELECT * FROM unnest(:streamNames::text[], :streamNamespaces::text[]) as t(stream_name, stream_namespace)
        ),
        latest_attempt_per_stream AS 
        (
            SELECT
                MAX(a.id) AS attempt_id,
                ss.stream_name,
                ss.stream_namespace
            FROM stream_stats ss
            INNER JOIN attempts a ON ss.attempt_id = a.id
            INNER JOIN stream_pairs sp ON ss.stream_name = sp.stream_name AND 
                (ss.stream_namespace = sp.stream_namespace OR (ss.stream_namespace IS NULL AND sp.stream_namespace IS NULL))
            GROUP BY ss.stream_name, ss.stream_namespace
        )
        SELECT laps.stream_name, laps.stream_namespace, j.id as job_id
        FROM latest_attempt_per_stream laps
        INNER JOIN attempts a ON laps.attempt_id = a.id
        INNER JOIN jobs j ON j.id = a.job_id
        WHERE j.scope = CAST(:connectionId AS varchar)
    """,
    readOnly = true,
  )
  fun findLastJobIdWithStatsPerStream(
    connectionId: UUID,
    streamNames: Array<String>,
    streamNamespaces: Array<String?>,
  ): List<StreamWithLastJobId>
}

@Introspected
data class StreamWithLastJobId(
  val jobId: Long,
  val streamName: String,
  val streamNamespace: String?,
)
