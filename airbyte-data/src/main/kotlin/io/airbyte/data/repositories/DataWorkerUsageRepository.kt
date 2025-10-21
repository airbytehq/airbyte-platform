/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.repositories

import io.airbyte.data.repositories.entities.DataWorkerUsage
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.time.OffsetDateTime
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface DataWorkerUsageRepository : PageableRepository<DataWorkerUsage, Long> {
  @Query(
    """
    SELECT * from data_worker_usage
    WHERE organization_id = :organizationId
    AND job_start >= :startDate AND (job_end <= :endDate OR job_end IS NULL)
    """,
  )
  fun findByOrganizationIdAndJobStartBetween(
    organizationId: UUID,
    startDate: OffsetDateTime,
    endDate: OffsetDateTime,
  ): List<DataWorkerUsage>

  @Query(
    """
    UPDATE data_worker_usage
    SET job_end = :jobEnd
    WHERE job_id = :jobId
    """,
  )
  fun updateUsageByJobId(
    jobId: Long,
    jobEnd: OffsetDateTime,
  )
}
