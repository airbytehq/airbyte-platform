package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Job
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface JobsRepository : PageableRepository<Job, Long> {
  /**
   * Counts the number of failed jobs since the last successful job for a given scope.
   * If there are no successful jobs, it counts all failed jobs for that scope.
   *
   * @param scope The scope associated with the connection (UUID as String).
   * @return The count of failed jobs since the last successful job.
   */
  @Query(
    """
    SELECT COUNT(*)
    FROM jobs
    WHERE scope = :scope
      AND status = 'failed'
      AND (created_at > (
          SELECT MAX(created_at)
          FROM jobs
          WHERE scope = :scope
            AND status = 'succeeded'
      ) OR NOT EXISTS (
          SELECT 1
          FROM jobs
          WHERE scope = :scope
            AND status = 'succeeded'
      ))
    """,
  )
  fun countFailedJobsSinceLastSuccessForScope(scope: String): Int

  @Query(
    """
    SELECT *
    FROM jobs
    WHERE scope = :scope
      AND status = 'succeeded'
    ORDER BY created_at DESC
    LIMIT 1
    """,
  )
  fun lastSuccessfulJobForScope(scope: String): Job?

  @Query(
    """
    SELECT *
    FROM jobs
    WHERE scope = :scope
      AND created_at < (
          SELECT created_at
          FROM jobs
          WHERE id = :jobId
      )
      AND status = :status
    ORDER BY created_at DESC
    LIMIT 1
    """,
  )
  fun getPriorJobWithStatusForScopeAndJobId(
    scope: String,
    jobId: Long,
    status: JobStatus,
  ): Job?
}
