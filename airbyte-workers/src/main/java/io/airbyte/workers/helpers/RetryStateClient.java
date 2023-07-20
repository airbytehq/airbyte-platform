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
import io.airbyte.featureflag.CompleteFailureBackoffBase;
import io.airbyte.featureflag.CompleteFailureBackoffMaxInterval;
import io.airbyte.featureflag.CompleteFailureBackoffMinInterval;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.SuccessiveCompleteFailureLimit;
import io.airbyte.featureflag.SuccessivePartialFailureLimit;
import io.airbyte.featureflag.TotalCompleteFailureLimit;
import io.airbyte.featureflag.TotalPartialFailureLimit;
import io.airbyte.featureflag.Workspace;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Business and request logic for retrieving and persisting retry state data.
 */
@Singleton
public class RetryStateClient {

  final JobRetryStatesApi jobRetryStatesApi;
  final FeatureFlagClient featureFlagClient;
  final Integer successiveCompleteFailureLimit;
  final Integer totalCompleteFailureLimit;
  final Integer successivePartialFailureLimit;
  final Integer totalPartialFailureLimit;
  final Integer minInterval;
  final Integer maxInterval;
  final Integer backoffBase;

  public RetryStateClient(final JobRetryStatesApi jobRetryStatesApi,
                          final FeatureFlagClient featureFlagClient,
                          @Value("${airbyte.retries.complete-failures.max-successive}") final Integer successiveCompleteFailureLimit,
                          @Value("${airbyte.retries.complete-failures.max-total}") final Integer totalCompleteFailureLimit,
                          @Value("${airbyte.retries.partial-failures.max-successive}") final Integer successivePartialFailureLimit,
                          @Value("${airbyte.retries.partial-failures.max-total}") final Integer totalPartialFailureLimit,
                          @Value("${airbyte.retries.complete-failures.backoff.min-interval-s}") final Integer minInterval,
                          @Value("${airbyte.retries.complete-failures.backoff.max-interval-s}") final Integer maxInterval,
                          @Value("${airbyte.retries.complete-failures.backoff.base}") final Integer backoffBase) {
    this.jobRetryStatesApi = jobRetryStatesApi;
    this.featureFlagClient = featureFlagClient;
    this.successiveCompleteFailureLimit = successiveCompleteFailureLimit;
    this.totalCompleteFailureLimit = totalCompleteFailureLimit;
    this.successivePartialFailureLimit = successivePartialFailureLimit;
    this.totalPartialFailureLimit = totalPartialFailureLimit;
    this.minInterval = minInterval;
    this.maxInterval = maxInterval;
    this.backoffBase = backoffBase;
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
  public RetryManager hydrateRetryState(final Long jobId, final UUID workspaceId) throws RetryableException {
    final var manager = initializeBuilder(workspaceId);

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

  /**
   * We initialize our values via FF if possible. These will be used for rollout, such that we can
   * tweak values on the fly without requiring redeployment. Eventually we plan to finalize the
   * default values and remove these FF'd values.
   */
  private RetryManager.RetryManagerBuilder initializeBuilder(final UUID workspaceId) {
    final var ffSuccessiveCompleteFailureLimit = featureFlagClient.intVariation(SuccessiveCompleteFailureLimit.INSTANCE, new Workspace(workspaceId));
    final var ffTotalCompleteFailureLimit = featureFlagClient.intVariation(TotalCompleteFailureLimit.INSTANCE, new Workspace(workspaceId));
    final var ffSuccessivePartialFailureLimit = featureFlagClient.intVariation(SuccessivePartialFailureLimit.INSTANCE, new Workspace(workspaceId));
    final var ffTotalPartialFailureLimit = featureFlagClient.intVariation(TotalPartialFailureLimit.INSTANCE, new Workspace(workspaceId));

    return RetryManager.builder()
        .completeFailureBackoffPolicy(buildBackOffPolicy(workspaceId))
        .successiveCompleteFailureLimit(initializedOrElse(ffSuccessiveCompleteFailureLimit, successiveCompleteFailureLimit))
        .successivePartialFailureLimit(initializedOrElse(ffSuccessivePartialFailureLimit, successivePartialFailureLimit))
        .totalCompleteFailureLimit(initializedOrElse(ffTotalCompleteFailureLimit, totalCompleteFailureLimit))
        .totalPartialFailureLimit(initializedOrElse(ffTotalPartialFailureLimit, totalPartialFailureLimit));
  }

  private BackoffPolicy buildBackOffPolicy(final UUID workspaceId) {
    final var ffMin = featureFlagClient.intVariation(CompleteFailureBackoffMinInterval.INSTANCE, new Workspace(workspaceId));
    final var ffMax = featureFlagClient.intVariation(CompleteFailureBackoffMaxInterval.INSTANCE, new Workspace(workspaceId));
    final var ffBase = featureFlagClient.intVariation(CompleteFailureBackoffBase.INSTANCE, new Workspace(workspaceId));

    return BackoffPolicy.builder()
        .minInterval(Duration.ofSeconds(initializedOrElse(ffMin, minInterval)))
        .maxInterval(Duration.ofSeconds(initializedOrElse(ffMax, maxInterval)))
        .base(initializedOrElse(ffBase, backoffBase))
        .build();
  }

  /**
   * Utility method for falling back to injected values when FFs are not initialized properly.
   */
  private int initializedOrElse(final int a, final int b) {
    return a == -1 ? b : a;
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

    final var result = AirbyteApiClient.retryWithJitter(
        () -> {
          jobRetryStatesApi.createOrUpdateWithHttpInfo(req);
          return true;
        }, // retryWithJitter doesn't like `void` "consumer" fn's
        String.format("Persisting retry state for job: %d connection: %s", req.getJobId(), req.getConnectionId()));

    // retryWithJitter returns null if unsuccessful
    return result != null;
  }

}
