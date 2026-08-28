/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.Job
import io.airbyte.db.instance.jobs.jooq.generated.enums.JobStatus
import io.micronaut.core.annotation.Introspected
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.time.OffsetDateTime
import java.util.UUID

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
    ORDER BY created_at ASC
    LIMIT 1
    """,
  )
  fun firstSuccessfulJobForScope(scope: String): Job?

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

  @Query(
    """
    SELECT DISTINCT ON (s.scope)
      s.scope,
      j.status,
      -- Only return the config fields needed for LatestJobHealthSummaryRow. The full config is large.
      (j.replication_config->>'sourceDefinitionVersionId')::uuid AS source_definition_version_id,
      (j.replication_config->>'destinationDefinitionVersionId')::uuid AS destination_definition_version_id,
      (j.replication_config->>'sourceDockerImageIsDefault')::boolean AS source_docker_image_is_default,
      (j.replication_config->>'destinationDockerImageIsDefault')::boolean AS destination_docker_image_is_default
    FROM unnest(array[:scopes]::text[]) AS s(scope)
    -- Cast the arguments to the enum type so the WHERE clause has no cast and the index is used
    CROSS JOIN unnest(array[:configTypes]::job_config_type[]) AS t(config_type)
    JOIN LATERAL (
      SELECT
        jobs.status,
        jobs.created_at,
        -- replication_config is pulled out once so the (potentially TOASTed)
        -- config column is decompressed a single time per row rather than once per extracted field
        CASE jobs.config_type
          WHEN 'sync' THEN jobs.config->'sync'
          WHEN 'refresh' THEN jobs.config->'refresh'
          WHEN 'reset_connection' THEN jobs.config->'resetConnection'
        END AS replication_config
      FROM jobs
      WHERE jobs.scope = s.scope
      AND jobs.config_type = t.config_type
      AND jobs.created_at >= :createdAtStart
      ORDER BY jobs.created_at DESC
      LIMIT 1
    ) j ON true
    ORDER BY s.scope, j.created_at DESC;
    """,
    nativeQuery = true,
  )
  fun findLatestJobPerScope(
    configTypes: Collection<String>,
    scopes: Set<String>,
    createdAtStart: OffsetDateTime,
  ): List<LatestJobHealthSummaryRow>
}

@Introspected
data class LatestJobHealthSummaryRow(
  val scope: String,
  val status: JobStatus,
  val sourceDefinitionVersionId: UUID?,
  val destinationDefinitionVersionId: UUID?,
  val sourceDockerImageIsDefault: Boolean?,
  val destinationDockerImageIsDefault: Boolean?,
)
