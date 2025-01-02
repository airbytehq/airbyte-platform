/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.temporal.scheduling.retries

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.apache.commons.lang3.time.DurationFormatUtils
import java.time.Duration

private const val DEFAULT_FAILURES = 0

/**
 * Keeps track of the number and sequence of adverse effects observed, determines whether we should
 * continue trying based off of configured values and delegates to provided backoff policies.
 *
 *
 * We categorize failures into two groups:
 *
 * 1. Complete Failures — Failed runs where no progress was observed. <br></br>
 * 2. Partial Failures — Failed runs where we observed some progress. <br></br>
 *
 * We track these failures separately and allow limits and back off to be configured independently
 * for each.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class RetryManager(
  val completeFailureBackoffPolicy: BackoffPolicy? = null,
  val partialFailureBackoffPolicy: BackoffPolicy? = null,
  val successiveCompleteFailureLimit: Int = Int.MAX_VALUE,
  val successivePartialFailureLimit: Int = Int.MAX_VALUE,
  val totalCompleteFailureLimit: Int = Int.MAX_VALUE,
  val totalPartialFailureLimit: Int = Int.MAX_VALUE,
  var successiveCompleteFailures: Int = DEFAULT_FAILURES,
  var successivePartialFailures: Int = DEFAULT_FAILURES,
  var totalCompleteFailures: Int = DEFAULT_FAILURES,
  var totalPartialFailures: Int = DEFAULT_FAILURES,
) {
  constructor(
    completeFailureBackoffPolicy: BackoffPolicy? = null,
    partialFailureBackoffPolicy: BackoffPolicy? = null,
    successiveCompleteFailureLimit: Int = Int.MAX_VALUE,
    successivePartialFailureLimit: Int = Int.MAX_VALUE,
    totalCompleteFailureLimit: Int = Int.MAX_VALUE,
    totalPartialFailureLimit: Int = Int.MAX_VALUE,
  ) : this(
    completeFailureBackoffPolicy = completeFailureBackoffPolicy,
    partialFailureBackoffPolicy = partialFailureBackoffPolicy,
    successiveCompleteFailureLimit = successiveCompleteFailureLimit,
    successivePartialFailureLimit = successivePartialFailureLimit,
    totalCompleteFailureLimit = totalCompleteFailureLimit,
    totalPartialFailureLimit = totalPartialFailureLimit,
    successiveCompleteFailures = DEFAULT_FAILURES,
    successivePartialFailures = DEFAULT_FAILURES,
    totalCompleteFailures = DEFAULT_FAILURES,
    totalPartialFailures = DEFAULT_FAILURES,
  )

  /**
   * Tracks partial failure state in the manager—we observed progress before we failed.
   */
  fun incrementPartialFailure() {
    successiveCompleteFailures = 0
    successivePartialFailures += 1
    totalPartialFailures += 1
  }

  /**
   * Tracks complete failure state in the manager—we observed _no_ progress before we failed.
   */
  fun incrementFailure() {
    successivePartialFailures = 0
    successiveCompleteFailures += 1
    totalCompleteFailures += 1
  }

  /**
   * Tracks failure state in the manager.
   *
   * @param isPartial whether the error observed is 'partial'—if we progress before we failed.
   */
  fun incrementFailure(isPartial: Boolean): Unit =
    if (isPartial) {
      incrementPartialFailure()
    } else {
      incrementFailure()
    }

  /**
   * Determines if we should retry.
   *
   * @return true if we should retry.
   */
  fun shouldRetry(): Boolean =
    successiveCompleteFailures < successiveCompleteFailureLimit &&
      totalCompleteFailures < totalCompleteFailureLimit &&
      successivePartialFailures < successivePartialFailureLimit &&
      totalPartialFailures < totalPartialFailureLimit

  /**
   * Delegates to relevant provided backoff policy.
   *
   * @return backoff duration to wait.
   */
  val backoff: Duration
    get() =
      when {
        successiveCompleteFailures > 0 && completeFailureBackoffPolicy != null ->
          completeFailureBackoffPolicy.getBackoff(
            successiveCompleteFailures.toLong(),
          )
        successivePartialFailures > 0 && partialFailureBackoffPolicy != null ->
          partialFailureBackoffPolicy.getBackoff(
            successivePartialFailures.toLong(),
          )
        else -> Duration.ZERO
      }

/**
   * Returns a human-optimized string of backoff time.
   *
   * @return backoff duration as a human-readable string.
   */
  val backoffString: String
    get() = DurationFormatUtils.formatDurationWords(backoff.toMillis(), true, true)
}
