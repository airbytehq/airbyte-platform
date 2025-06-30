/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config

/**
 * Attempt with some context about its parent job appended.
 */
class AttemptWithJobInfo(
  @JvmField val attempt: Attempt,
  @JvmField val jobInfo: JobInfo,
) {
  companion object {
    @JvmStatic
    fun fromJob(
      attempt: Attempt,
      job: Job,
    ): AttemptWithJobInfo =
      AttemptWithJobInfo(
        attempt,
        JobInfo(
          job.id,
          job.configType,
          job.scope,
          job.config,
          job.status,
        ),
      )
  }
}
