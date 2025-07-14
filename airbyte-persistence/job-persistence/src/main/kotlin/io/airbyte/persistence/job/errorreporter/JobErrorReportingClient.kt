/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.persistence.job.errorreporter

import io.airbyte.config.FailureReason
import io.airbyte.config.StandardWorkspace

/**
 * A generic interface for a client that reports errors.
 */
interface JobErrorReportingClient {
  /**
   * Report a job failure reason.
   */
  fun reportJobFailureReason(
    workspace: StandardWorkspace?,
    reason: FailureReason,
    dockerImage: String?,
    metadata: Map<String?, String?>?,
    attemptConfig: AttemptConfigReportingContext?,
  )
}
