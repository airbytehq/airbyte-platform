/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workload.repository

import io.airbyte.commons.annotation.InternalForTesting
import io.airbyte.workload.repository.domain.Workload
import io.airbyte.workload.repository.domain.WorkloadQueueItem
import io.airbyte.workload.repository.domain.WorkloadQueueStats
import io.micronaut.data.annotation.Join
import io.micronaut.data.annotation.Query
import io.micronaut.data.jdbc.annotation.JdbcRepository
import io.micronaut.data.model.query.builder.sql.Dialect
import io.micronaut.data.repository.PageableRepository
import java.util.UUID

@JdbcRepository(dialect = Dialect.POSTGRES, dataSource = "config")
interface WorkloadQueueRepository : PageableRepository<WorkloadQueueItem, UUID> {
  /**
   * This query explained:
   *
   * The first CTE `polled_q_ids` gets the enqueued workload ids from the workload_queue table.
   *
   * It leverages two PostgresQL features to this effect:
   *    1) MATERIALIZED — this ensures the CTE is computed first and can be referenced by the
   *       subsequent sub-queries.
   *    2) FOR UPDATE SKIP LOCKED — this locks the selected rows immediately, preventing other
   *       connections from seeing them. This guarantees exclusivity of the items and avoids
   *       locking in application code.
   *
   * Additionally, it provides a couple of other features. Namely:
   *    1) Re-delivery — the `now() > poll_deadline` limits exclusivity to the deadline window. Once
   *       the window has elapsed, the subsequent poll will see this workload item. We set the
   *       `poll_deadline` in the next CTE so stay tuned for the other half.
   *    2) FIFO guarantees — sorting by ASC ensures first in first out.
   *    3) Filtering acked messages.
   *
   * The second CTE, `workloads`, does the update to the workload_queue rows that sets the
   * `poll_deadline`. This is what guarantees exclusivity. Subsequent polls within the `poll_deadline`
   * window will not see these items.
   *
   * Additionally, it joins on the `workload` table "hydrating" the workload queue items with the
   * backing workload.
   *
   * The final select propagates all the workload information and joins on the workload labels.
   * The `workload_labels_` field aliases are a micronaut-data specific format that in concert with
   * the @Join annotation allow micronaut-data to properly hydrate the labels on the returned Workload
   * domain objects.
   */
  @Join(value = "workloadLabels")
  @Query(
    """
      WITH polled_q_ids AS MATERIALIZED (
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
      ),
      workloads AS (
        UPDATE workload_queue AS q
           SET
              poll_deadline = now() + (:redeliveryWindowSecs * interval '1 second'),
              updated_at = now()
        FROM workload w
              WHERE q.id = ANY(SELECT id FROM polled_q_ids)
              AND w.id = q.workload_id
        RETURNING
           w.*
	    )
    SELECT
        workloads.*,
        l.id AS workload_labels_id,
        l.key AS workload_labels_key,
        l.value AS workload_labels_value
    FROM workloads
        LEFT JOIN workload_label l
            ON l.workload_id = workloads.id;
    """,
  )
  fun pollWorkloadQueue(
    dataplaneGroup: String?,
    priority: Int?,
    quantity: Int = 1,
    redeliveryWindowSecs: Int = 300,
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
        GROUP BY dataplane_group, priority
    """,
  )
  fun getEnqueuedWorkloadStats(): List<WorkloadQueueStats>

  @Query(
    """
    WITH acked_workloads AS (
      SELECT id FROM workload_queue
        WHERE acked_at IS NOT NULL
        AND acked_at < now() - interval '1 week'
        limit :deletionLimit
    ) DELETE FROM workload_queue
      WHERE id IN (SELECT id FROM acked_workloads);
    """,
  )
  fun cleanUpAckedEntries(deletionLimit: Int): Unit

  @InternalForTesting
  fun findByDataplaneGroup(dataplaneGroup: String): List<WorkloadQueueItem>
}
