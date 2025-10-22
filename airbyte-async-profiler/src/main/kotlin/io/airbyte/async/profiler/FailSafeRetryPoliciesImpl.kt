/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.async.profiler

import dev.failsafe.RetryPolicy
import dev.failsafe.event.ExecutionAttemptedEvent
import dev.failsafe.event.ExecutionCompletedEvent
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.IOException
import java.time.Duration

private val logger = KotlinLogging.logger {}

@Singleton
class FailSafeRetryPoliciesImpl : FailSafeRetryPolicies {
  companion object {
    private const val MAX_RETRIES_FOR_FILE_DOWNLOAD = 3
    private const val DELAY_SECONDS = 1L
    private const val MAX_RETRIES_FOR_PID = 30
  }

  override fun pidRetryPolicy(mainClassKeyword: String): RetryPolicy<Int?> =
    RetryPolicy
      .builder<Int?>()
      .handle(IOException::class.java)
      .handleResult(null)
      .withDelay(Duration.ofSeconds(DELAY_SECONDS))
      .withMaxRetries(MAX_RETRIES_FOR_PID)
      .onRetry { event: ExecutionAttemptedEvent<Int?> ->
        logger.warn(event.lastException) { "Process containing '$mainClassKeyword' not found yet. Retrying... (attempt #${event.attemptCount})" }
      }.onFailure { event: ExecutionCompletedEvent<Int?> ->
        logger.error(event.exception) { "Could not find process containing '$mainClassKeyword' after ${event.attemptCount} attempts." }
      }.onSuccess { event: ExecutionCompletedEvent<Int?> ->
        logger.info { "Found PID ${event.result} for process containing '$mainClassKeyword' after ${event.attemptCount} attempt(s)." }
      }.build()

  override fun fileDownloadRetryPolicy(url: String): RetryPolicy<Any?> =
    RetryPolicy
      .builder<Any?>()
      .handle(IOException::class.java)
      .withDelay(Duration.ofSeconds(DELAY_SECONDS))
      .withMaxRetries(MAX_RETRIES_FOR_FILE_DOWNLOAD)
      .onRetry { event: ExecutionAttemptedEvent<Any?> ->
        logger.warn(event.lastException) { "Retrying attempt ${event.attemptCount} for downloading the file from url $url" }
      }.onFailure { event: ExecutionCompletedEvent<Any?> ->
        logger.error(event.exception) { "Failed to download the file from $url after ${event.attemptCount} attempts" }
      }.onSuccess { event: ExecutionCompletedEvent<Any?> ->
        logger.info { "Successfully downloaded the file from $url after ${event.attemptCount} attempt(s)" }
      }.build()
}
