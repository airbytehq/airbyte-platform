/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

/**
 * The statuses of a job.
 */
enum class JobStatus {
  PENDING,
  RUNNING,
  INCOMPLETE,
  FAILED,
  SUCCEEDED,
  CANCELLED,
  ;

  companion object {
    val TERMINAL_STATUSES: Set<JobStatus> = setOf(FAILED, SUCCEEDED, CANCELLED)

    val NON_TERMINAL_STATUSES: Set<JobStatus> = entries.toSet() - TERMINAL_STATUSES

    val VALID_STATUS_CHANGES: Map<JobStatus, Set<JobStatus>> =
      mapOf(
        PENDING to setOf(RUNNING, FAILED, CANCELLED, INCOMPLETE),
        RUNNING to setOf(INCOMPLETE, SUCCEEDED, FAILED, CANCELLED),
        INCOMPLETE to setOf(PENDING, RUNNING, FAILED, CANCELLED, INCOMPLETE, SUCCEEDED),
        SUCCEEDED to emptySet(),
        FAILED to setOf(FAILED),
        CANCELLED to emptySet(),
      )
  }
}
