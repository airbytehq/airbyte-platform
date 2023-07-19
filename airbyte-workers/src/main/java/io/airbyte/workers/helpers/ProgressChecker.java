/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import io.airbyte.api.client.generated.AttemptApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.AttemptStats;
import io.airbyte.api.client.model.generated.GetAttemptStatsRequestBody;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

/**
 * Composes all the business and request logic for checking progress of a run.
 */
@Slf4j
@Singleton
public class ProgressChecker {

  private final AttemptApi attemptApi;
  private final ProgressCheckerPredicates predicate;

  public ProgressChecker(final AttemptApi attemptApi, final ProgressCheckerPredicates predicate) {
    this.attemptApi = attemptApi;
    this.predicate = predicate;
  }

  /**
   * Fetches an attempt stats and evaluates it against a given progress predicate.
   *
   * @param jobId Job id for run in question
   * @param attemptNo Attempt number for run in question — 0-based
   * @return whether we made progress. Returns false if we failed to check.
   */
  public boolean check(final long jobId, final int attemptNo) {
    final var resp = fetchAttemptStats(jobId, attemptNo);

    return resp
        .map(predicate::test)
        .orElse(false);
  }

  private Optional<AttemptStats> fetchAttemptStats(final long jobId, final int attemptNo) throws RetryableException {
    final var req = new GetAttemptStatsRequestBody()
        .attemptNumber(attemptNo)
        .jobId(jobId);

    AttemptStats resp;

    try {
      resp = attemptApi.getAttemptCombinedStats(req);
    } catch (final ApiException e) {
      // Retry unexpected 4xx/5xx status codes.
      // 404 is an expected status code and should not be retried.
      if (e.getCode() != HttpStatus.NOT_FOUND.getCode()) {
        throw new RetryableException(e);
      }
      resp = null;
    }

    return Optional.ofNullable(resp);
  }

}
