/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

import com.google.common.collect.Sets

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
    @JvmField
    val TERMINAL_STATUSES: Set<JobStatus> = setOf(FAILED, SUCCEEDED, CANCELLED)

    @JvmField
    val NON_TERMINAL_STATUSES: Set<JobStatus> = Sets.difference(setOf(*entries.toTypedArray()), TERMINAL_STATUSES)

    val VALID_STATUS_CHANGES: Map<JobStatus, Set<JobStatus>> =
      java.util.Map.of(
        PENDING,
        setOf(RUNNING, FAILED, CANCELLED, INCOMPLETE),
        RUNNING,
        setOf(INCOMPLETE, SUCCEEDED, FAILED, CANCELLED),
        INCOMPLETE,
        setOf(PENDING, RUNNING, FAILED, CANCELLED, INCOMPLETE, SUCCEEDED),
        SUCCEEDED,
        setOf(),
        FAILED,
        setOf(FAILED),
        CANCELLED,
        setOf(),
      )
  }
}
