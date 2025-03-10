/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.repository

import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadQueueItem
import io.airbyte.workload.repository.domain.WorkloadQueueStats
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES)
interface WorkloadQueueRepository : PageableRepository<WorkloadQueueItem, UUID> {
  @Query(
    """
      WITH ids AS MATERIALIZED (
         SELECT id FROM workload_queue
            WHERE
              (:dataplaneGroup IS NULL OR dataplane_group = :dataplaneGroup)
            AND 
              (:priority IS NULL OR priority = :priority)
            AND
              acked_at IS NULL
            AND
              now() > poll_deadline
         ORDER BY created_at ASC
         LIMIT :quantity
         FOR UPDATE SKIP LOCKED
      )
      UPDATE workload_queue AS q
         SET
            poll_deadline = now() + (:redeliveryWindowMins * interval '1 minute'),
            updated_at = now()
      FROM workload w
         WHERE q.id = ANY(SELECT id FROM ids)
         AND w.id = q.workload_id
      RETURNING
         w.*
      ;
    """,
  )
  fun pollWorkloadQueue(
    dataplaneGroup: String?,
    priority: Int?,
    quantity: Int = 1,
    redeliveryWindowMins: Int = 30,
  ): List<Workload>

  @Query(
    """
    INSERT INTO workload_queue (
       id,
       dataplane_group,
       priority,
       workload_id
    ) VALUES (
       gen_random_uuid(),
       :dataplaneGroup,
       :priority,
       :workloadId
  )
  """,
  )
  fun enqueueWorkload(
    dataplaneGroup: String,
    priority: Int,
    workloadId: String,
  ): WorkloadQueueItem

  @Query(
    """
    UPDATE workload_queue SET acked_at = now(), updated_at = now() WHERE workload_id = :workloadId
  """,
  )
  fun ackWorkloadQueueItem(workloadId: String)

  @Query(
    """
    SELECT count(*) FROM workload_queue
        WHERE acked_at IS NULL
        AND (:dataplaneGroup IS NULL OR dataplane_group = :dataplaneGroup)
        AND (:priority IS NULL OR priority = :priority)
        AND now() > poll_deadline
    """,
  )
  fun countEnqueuedWorkloads(
    dataplaneGroup: String?,
    priority: Int?,
  ): Long

  @Query(
    """
    SELECT count(*) as enqueued_count, dataplane_group, priority FROM workload_queue
        WHERE acked_at IS NULL
        AND now() > poll_deadline
        GROUP BY dataplane_group, priority
    """,
  )
  fun getEnqueuedWorkloadStats(): List<WorkloadQueueStats>
}
