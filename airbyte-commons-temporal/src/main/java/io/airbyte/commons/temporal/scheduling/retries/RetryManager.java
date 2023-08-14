/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal.scheduling.retries;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Duration;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.jackson.Jacksonized;
import org.apache.commons.lang3.time.DurationFormatUtils;

/**
 * Keeps track of the number and sequence of adverse effects observed, determines whether we should
 * continue trying based off of configured values and delegates to provided backoff policies.
 * <p>
 * We categorize failures into two groups:
 * </p>
 * 1. Complete Failures — Failed runs where no progress was observed. <br>
 * 2. Partial Failures — Failed runs where we observed some progress. <br>
 *
 * We track these failures separately and allow limits and back off to be configured independently
 * for each.
 */
@Builder
@Getter
@ToString
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class RetryManager {

  private BackoffPolicy completeFailureBackoffPolicy;
  private BackoffPolicy partialFailureBackoffPolicy;

  @Builder.Default
  private int successiveCompleteFailureLimit = Integer.MAX_VALUE;
  @Builder.Default
  private int totalCompleteFailureLimit = Integer.MAX_VALUE;

  @Builder.Default
  private int successivePartialFailureLimit = Integer.MAX_VALUE;
  @Builder.Default
  private int totalPartialFailureLimit = Integer.MAX_VALUE;

  @Builder.Default
  private int successiveCompleteFailures = 0;
  @Builder.Default
  private int totalCompleteFailures = 0;

  @Builder.Default
  private int successivePartialFailures = 0;
  @Builder.Default
  private int totalPartialFailures = 0;

  /**
   * Tracks partial failure state in the manager—we observed progress before we failed.
   */
  public void incrementPartialFailure() {
    successiveCompleteFailures = 0;
    successivePartialFailures += 1;
    totalPartialFailures += 1;
  }

  /**
   * Tracks complete failure state in the manager—we observed _no_ progress before we failed.
   */
  public void incrementFailure() {
    successivePartialFailures = 0;
    successiveCompleteFailures += 1;
    totalCompleteFailures += 1;
  }

  /**
   * Tracks failure state in the manager.
   *
   * @param isPartial whether the error observed is 'partial'—if we progress before we failed.
   */
  public void incrementFailure(final boolean isPartial) {
    if (isPartial) {
      incrementPartialFailure();
    } else {
      incrementFailure();
    }
  }

  private boolean passesLimitChecks() {
    return successiveCompleteFailures < successiveCompleteFailureLimit
        && totalCompleteFailures < totalCompleteFailureLimit
        && successivePartialFailures < successivePartialFailureLimit
        && totalPartialFailures < totalPartialFailureLimit;
  }

  /**
   * Determines if we should retry.
   *
   * @return true if we should retry.
   */
  public boolean shouldRetry() {
    return passesLimitChecks();
  }

  /**
   * Delegates to relevant provided backoff policy.
   *
   * @return backoff duration to wait.
   */
  public Duration getBackoff() {
    if (successiveCompleteFailures > 0 && completeFailureBackoffPolicy != null) {
      return completeFailureBackoffPolicy.getBackoff(successiveCompleteFailures);
    }
    if (successivePartialFailures > 0 && partialFailureBackoffPolicy != null) {
      return partialFailureBackoffPolicy.getBackoff(successivePartialFailures);
    }
    return Duration.ZERO;
  }

  /**
   * Returns a human-optimized string of backoff time.
   *
   * @return backoff duration as a human-readable string.
   */
  public String getBackoffString() {
    return DurationFormatUtils.formatDurationWords(getBackoff().toMillis(), true, true);
  }

}
