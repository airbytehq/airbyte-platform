/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.config.Attempt

interface AttemptService {
  fun getAttempt(
    jobId: Long,
    attemptNumber: Long,
  ): Attempt
}
