/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services.impls.data

import io.airbyte.config.Attempt
import io.airbyte.data.repositories.AttemptsRepository
import io.airbyte.data.services.AttemptService
import io.airbyte.data.services.impls.data.mappers.toConfigModel
import jakarta.inject.Singleton

@Singleton
class AttemptServiceImpl(
  private val attemptsRepository: AttemptsRepository,
) : AttemptService {
  override fun getAttempt(
    jobId: Long,
    attemptNumber: Long,
  ): Attempt =
    attemptsRepository.findByJobIdAndAttemptNumber(jobId, attemptNumber)?.toConfigModel()
      ?: throw NoSuchElementException("Attempt not found for jobId: $jobId and attemptNumber: $attemptNumber")
}
