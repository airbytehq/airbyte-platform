/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.AttemptStats;
import io.airbyte.api.client.model.generated.GetAttemptStatsRequestBody;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Optional;
import org.openapitools.client.infrastructure.ClientException;

/**
 * Composes all the business and request logic for checking progress of a run.
 */
@Singleton
public class ProgressChecker {

  private final AirbyteApiClient airbyteApiClient;
  private final ProgressCheckerPredicates predicate;

  public ProgressChecker(final AirbyteApiClient airbyteApiClient, final ProgressCheckerPredicates predicate) {
    this.airbyteApiClient = airbyteApiClient;
    this.predicate = predicate;
  }

  /**
   * Fetches an attempt stats and evaluates it against a given progress predicate.
   *
   * @param jobId Job id for run in question
   * @param attemptNo Attempt number for run in question — 0-based
   * @return whether we made progress. Returns false if we failed to check.
   * @throws IOException Rethrows the OkHttp execute method exception
   */
  public boolean check(final long jobId, final int attemptNo) throws IOException {
    final var resp = fetchAttemptStats(jobId, attemptNo);

    return resp
        .map(predicate::test)
        .orElse(false);
  }

  private Optional<AttemptStats> fetchAttemptStats(final long jobId, final int attemptNo) throws RetryableException {
    final var req = new GetAttemptStatsRequestBody(jobId, attemptNo);

    AttemptStats resp;

    try {
      resp = airbyteApiClient.getAttemptApi().getAttemptCombinedStats(req);
    } catch (final ClientException e) {
      // Retry unexpected 4xx/5xx status codes.
      // 404 is an expected status code and should not be retried.
      if (e.getStatusCode() != HttpStatus.NOT_FOUND.getCode()) {
        throw new RetryableException(e);
      }
      resp = null;
    } catch (final IOException e) {
      throw new RetryableException(e);
    }

    return Optional.ofNullable(resp);
  }

}
