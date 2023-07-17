/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.JobRetryStatesApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.JobIdRequestBody;
import io.airbyte.api.client.model.generated.JobRetryStateRequestBody;
import io.airbyte.api.client.model.generated.RetryStateRead;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.commons.temporal.scheduling.retries.BackoffPolicy;
import io.airbyte.commons.temporal.scheduling.retries.RetryManager;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.Optional;
import java.util.UUID;

/**
 * Business and request logic for retrieving and persisting retry state data.
 */
@Singleton
public class RetryStateClient {

  final JobRetryStatesApi jobRetryStatesApi;
  final BackoffPolicy completeFailureBackoffPolicy;
  final Integer successiveCompleteFailureLimit;
  final Integer totalCompleteFailureLimit;
  final Integer successivePartialFailureLimit;
  final Integer totalPartialFailureLimit;

  public RetryStateClient(final JobRetryStatesApi jobRetryStatesApi,
                          @Named("completeFailureBackoffPolicy") final BackoffPolicy completeFailureBackoffPolicy,
                          @Value("${airbyte.retries.complete—failures.max-successive}") final Integer successiveCompleteFailureLimit,
                          @Value("${airbyte.retries.complete—failures.max-total}") final Integer totalCompleteFailureLimit,
                          @Value("${airbyte.retries.partial—failures.max-successive}") final Integer successivePartialFailureLimit,
                          @Value("${airbyte.retries.partial—failures.max-total}") final Integer totalPartialFailureLimit) {
    this.jobRetryStatesApi = jobRetryStatesApi;
    this.completeFailureBackoffPolicy = completeFailureBackoffPolicy;
    this.successiveCompleteFailureLimit = successiveCompleteFailureLimit;
    this.totalCompleteFailureLimit = totalCompleteFailureLimit;
    this.successivePartialFailureLimit = successivePartialFailureLimit;
    this.totalPartialFailureLimit = totalPartialFailureLimit;
  }

  /**
   * Returns a RetryManager hydrated from persistence or a fresh RetryManager if there's no persisted
   * data.
   *
   * @param jobId — the job in question. May be null if there is no job yet.
   * @return RetryManager — a hydrated RetryManager or new RetryManager if no state exists in
   *         persistence or null job id passed.
   * @throws RetryableException — Delegates to Temporal to retry for now (retryWithJitter swallowing
   *         404's is problematic).
   */
  public RetryManager hydrateRetryState(final Long jobId) throws RetryableException {
    final var manager = RetryManager.builder()
        .completeFailureBackoffPolicy(completeFailureBackoffPolicy)
        .successiveCompleteFailureLimit(successiveCompleteFailureLimit)
        .successivePartialFailureLimit(successivePartialFailureLimit)
        .totalCompleteFailureLimit(totalCompleteFailureLimit)
        .totalPartialFailureLimit(totalPartialFailureLimit);

    final var state = Optional.ofNullable(jobId).flatMap(this::fetchRetryState);

    // if there is retry state we hydrate
    // otherwise we will build with default 0 values
    state.ifPresent(s -> manager
        .totalCompleteFailures(s.getTotalCompleteFailures())
        .totalPartialFailures(s.getTotalPartialFailures())
        .successiveCompleteFailures(s.getSuccessiveCompleteFailures())
        .successivePartialFailures(s.getSuccessivePartialFailures()));

    return manager.build();
  }

  private Optional<RetryStateRead> fetchRetryState(final long jobId) throws RetryableException {
    final var req = new JobIdRequestBody().id(jobId);

    RetryStateRead resp;

    try {
      resp = jobRetryStatesApi.get(req);
    } catch (final ApiException e) {
      if (e.getCode() != HttpStatus.NOT_FOUND.getCode()) {
        throw new RetryableException(e);
      }
      resp = null;
    }

    return Optional.ofNullable(resp);
  }

  /**
   * Persists our RetryManager's state to be picked up on the next run, or queried for debugging.
   *
   * @param jobId — the job in question.
   * @param connectionId — the connection in question.
   * @param manager — the RetryManager we want to persist.
   * @return true if successful, otherwise false.
   */
  public boolean persistRetryState(final long jobId, final UUID connectionId, final RetryManager manager) {
    final var req = new JobRetryStateRequestBody()
        .jobId(jobId)
        .connectionId(connectionId)
        .totalCompleteFailures(manager.getTotalCompleteFailures())
        .totalPartialFailures(manager.getTotalPartialFailures())
        .successiveCompleteFailures(manager.getSuccessiveCompleteFailures())
        .successivePartialFailures(manager.getSuccessivePartialFailures());

    final var resp = AirbyteApiClient.retryWithJitter(
        () -> {
          jobRetryStatesApi.createOrUpdateWithHttpInfo(req);
          return null;
        }, // retryWithJitter doesn't like `void` "consumer" fn's
        String.format("Persisting retry state for job: %d connection: %s", req.getJobId(), req.getConnectionId()));

    return resp != null;
  }

}
