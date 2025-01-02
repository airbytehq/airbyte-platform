/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import io.airbyte.api.client.model.generated.AttemptStats;
import jakarta.annotation.Nonnull;
import jakarta.inject.Singleton;
import java.util.Objects;

/**
 * Simple predicates for judging progress based on domain data.
 */
@Singleton
public class ProgressCheckerPredicates {

  public static final long RECORDS_COMMITTED_THRESHOLD = 1;

  /**
   * Checks progress for AttemptStats.
   *
   * @param stats AttemptStats — must not be null.
   * @return true if we consider progress to have been made, false otherwise.
   */
  public boolean test(@Nonnull final AttemptStats stats) {
    Objects.requireNonNull(stats, "Attempt stats null. Cannot test progress.");
    // If recordsCommitted is null, treat this as a 0
    final var recordsCommitted = Objects.requireNonNullElse(stats.getRecordsCommitted(), 0L);

    return recordsCommitted >= RECORDS_COMMITTED_THRESHOLD;
  }

}
