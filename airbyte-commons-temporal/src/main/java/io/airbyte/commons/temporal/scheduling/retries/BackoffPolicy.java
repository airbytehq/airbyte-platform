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

/**
 * Calculates how much we should wait based off the provided configuration. Uses exponential backoff
 * clamped to a min/max interval.
 */
@Builder
@Getter
@ToString
@Jacksonized
@JsonIgnoreProperties(ignoreUnknown = true)
public class BackoffPolicy {

  @Builder.Default
  private Duration minInterval = Duration.ofSeconds(10);
  @Builder.Default
  private Duration maxInterval = Duration.ofHours(1);
  @Builder.Default
  private long base = 2;

  /**
   * Calculates backoff based off provided steps.
   *
   * @param ordinal the number of adverse effects we have observed.
   * @return backoff duration to wait.
   */
  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  public Duration getBackoff(final long ordinal) {
    if (ordinal < 1) {
      return Duration.ZERO;
    }

    final var coefficient = Math.pow(base, ordinal - 1);
    final var calculated = (long) (minInterval.toMillis() * coefficient);
    final var clamped = Math.min(calculated, maxInterval.toMillis());

    return Duration.ofMillis(clamped);
  }

}
