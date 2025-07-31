/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.workers.helpers.ProgressChecker
import jakarta.inject.Singleton
import java.io.IOException

/**
 * Concrete CheckRunProgressActivity.
 */
@Singleton
class CheckRunProgressActivityImpl(
  private val checker: ProgressChecker,
) : CheckRunProgressActivity {
  override fun checkProgress(input: CheckRunProgressActivity.Input): CheckRunProgressActivity.Output {
    try {
      val result = checker.check(input.jobId!!, input.attemptNo!!)

      return CheckRunProgressActivity.Output(result)
    } catch (e: IOException) {
      throw RetryableException(e)
    }
  }
}
