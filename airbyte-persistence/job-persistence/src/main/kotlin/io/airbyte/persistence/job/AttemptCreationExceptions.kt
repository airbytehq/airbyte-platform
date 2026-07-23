/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job

import io.airbyte.config.JobStatus

/** Thrown when an attempt cannot be created because the job is already in a terminal state. */
class JobInTerminalStateException(
  jobId: Long,
  scope: String,
  status: JobStatus,
) : IllegalStateException("Cannot create an attempt for a job id: $jobId (connection id: $scope) that is in a terminal state: $status")

/**
 * Thrown when the job already has a non-terminal (running) attempt. Carries the existing attempt
 * number for diagnostics/logging.
 */
class JobRunningAttemptExistsException(
  jobId: Long,
  scope: String,
  val existingAttemptNumber: Int,
) : IllegalStateException(
    "Cannot create an attempt for a job id: $jobId (connection id: $scope) that already has a running attempt: $existingAttemptNumber",
  )
