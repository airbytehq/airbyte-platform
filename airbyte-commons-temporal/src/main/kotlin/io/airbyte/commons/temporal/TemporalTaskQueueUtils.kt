/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

/**
 * Singleton helper object that provides translation between a Temporal job type and
 * the Temporal queue name.
 */
@Deprecated("Check and Discover queues are obsolete, SYNC should be migrated to TemporalQueueConfiguration")
object TemporalTaskQueueUtils {
  val DEFAULT_SYNC_TASK_QUEUE = TemporalJobType.SYNC.name
  val DEFAULT_CHECK_TASK_QUEUE = TemporalJobType.CHECK_CONNECTION.name
  val DEFAULT_DISCOVER_TASK_QUEUE = TemporalJobType.DISCOVER_SCHEMA.name

  /**
   * Returns the Temporal work queue name associated with the provided [TemporalJobType].
   *
   * @param jobType The type of the Temporal job to be scheduled.
   * @return The name of the Temporal task queue to submit the job to.
   * @throws IllegalArgumentException if there is no task queue associated with the provided [TemporalJobType].
   */
  @JvmStatic
  fun getTaskQueue(jobType: TemporalJobType): String =
    when (jobType) {
      TemporalJobType.CHECK_CONNECTION -> DEFAULT_CHECK_TASK_QUEUE
      TemporalJobType.DISCOVER_SCHEMA -> DEFAULT_DISCOVER_TASK_QUEUE
      TemporalJobType.SYNC -> DEFAULT_SYNC_TASK_QUEUE
      else -> throw IllegalArgumentException("Unexpected jobType $jobType")
    }
}
