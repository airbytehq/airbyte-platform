/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.model.generated.JobIdRequestBody;
import io.airbyte.api.client.model.generated.JobRetryStateRequestBody;
import io.airbyte.api.client.model.generated.RetryStateRead;
import io.airbyte.api.client.model.generated.WorkspaceIdRequestBody;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.commons.temporal.scheduling.retries.BackoffPolicy;
import io.airbyte.commons.temporal.scheduling.retries.RetryManager;
import io.airbyte.featureflag.CompleteFailureBackoffBase;
import io.airbyte.featureflag.CompleteFailureBackoffMaxInterval;
import io.airbyte.featureflag.CompleteFailureBackoffMinInterval;
import io.airbyte.featureflag.Context;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.Multi;
import io.airbyte.featureflag.Organization;
import io.airbyte.featureflag.SuccessiveCompleteFailureLimit;
import io.airbyte.featureflag.SuccessivePartialFailureLimit;
import io.airbyte.featureflag.TotalCompleteFailureLimit;
import io.airbyte.featureflag.TotalPartialFailureLimit;
import io.airbyte.featureflag.Workspace;
import io.micronaut.context.annotation.Value;
import io.micronaut.http.HttpStatus;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.openapitools.client.infrastructure.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Business and request logic for retrieving and persisting retry state data.
 */
@Singleton
public class RetryStateClient {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final AirbyteApiClient airbyteApiClient;
  final FeatureFlagClient featureFlagClient;
  final Integer successiveCompleteFailureLimit;
  final Integer totalCompleteFailureLimit;
  final Integer successivePartialFailureLimit;
  final Integer totalPartialFailureLimit;
  final Integer minInterval;
  final Integer maxInterval;
  final Integer backoffBase;

  public RetryStateClient(final AirbyteApiClient airbyteApiClient,
                          final FeatureFlagClient featureFlagClient,
                          @Value("${airbyte.retries.complete-failures.max-successive}") final Integer successiveCompleteFailureLimit,
                          @Value("${airbyte.retries.complete-failures.max-total}") final Integer totalCompleteFailureLimit,
                          @Value("${airbyte.retries.partial-failures.max-successive}") final Integer successivePartialFailureLimit,
                          @Value("${airbyte.retries.partial-failures.max-total}") final Integer totalPartialFailureLimit,
                          @Value("${airbyte.retries.complete-failures.backoff.min-interval-s}") final Integer minInterval,
                          @Value("${airbyte.retries.complete-failures.backoff.max-interval-s}") final Integer maxInterval,
                          @Value("${airbyte.retries.complete-failures.backoff.base}") final Integer backoffBase) {
    this.airbyteApiClient = airbyteApiClient;
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
    try {
      final var organizationId = fetchOrganizationId(workspaceId);

      final var manager = initializeRetryManager(workspaceId, organizationId);

      final var state = Optional.ofNullable(jobId).flatMap(this::fetchRetryState);

      // if there is retry state we hydrate
      // otherwise we will build with default 0 values
      state.ifPresent(s -> {
        manager.setTotalCompleteFailures(s.getTotalCompleteFailures());
        manager.setTotalPartialFailures(s.getTotalPartialFailures());
        manager.setSuccessiveCompleteFailures(s.getSuccessiveCompleteFailures());
        manager.setSuccessivePartialFailures(s.getSuccessivePartialFailures());
      });

      return manager;
    } catch (final RetryableException e) {
      throw e;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * We look up the organization id instead of passing it in because the caller
   * (ConnectionManagerWorkflowImpl) doesn't have it before hydrating retry state. If this is changed
   * in the future, we can simply take the organization id as an argument to the public methods.
   */
  private UUID fetchOrganizationId(final UUID workspaceId) {
    UUID organizationId = null;
    try {
      final var workspaceRead = airbyteApiClient.getWorkspaceApi().getWorkspace(new WorkspaceIdRequestBody(workspaceId, false));
      if (workspaceRead != null) {
        organizationId = workspaceRead.getOrganizationId();
      }
    } catch (final Exception e) {
      log.warn(String.format("Failed to fetch organization from workspace_ud: %s", workspaceId), e);
    }

    return organizationId;
  }

  /**
   * We initialize our values via FF if possible. These will be used for rollout, such that we can
   * tweak values on the fly without requiring redeployment. Eventually we plan to finalize the
   * default values and remove these FF'd values.
   */
  private RetryManager initializeRetryManager(final UUID workspaceId, final UUID organizationId) {
    final var ffContext = organizationId == null
        ? new Workspace(workspaceId)
        : new Multi(List.of(new Workspace(workspaceId), new Organization(organizationId)));

    final var ffSuccessiveCompleteFailureLimit = featureFlagClient.intVariation(SuccessiveCompleteFailureLimit.INSTANCE, ffContext);
    final var ffTotalCompleteFailureLimit = featureFlagClient.intVariation(TotalCompleteFailureLimit.INSTANCE, ffContext);
    final var ffSuccessivePartialFailureLimit = featureFlagClient.intVariation(SuccessivePartialFailureLimit.INSTANCE, ffContext);
    final var ffTotalPartialFailureLimit = featureFlagClient.intVariation(TotalPartialFailureLimit.INSTANCE, ffContext);

    return new RetryManager(
        buildBackOffPolicy(ffContext),
        null,
        initializedOrElse(ffSuccessiveCompleteFailureLimit, successiveCompleteFailureLimit),
        initializedOrElse(ffSuccessivePartialFailureLimit, successivePartialFailureLimit),
        initializedOrElse(ffTotalCompleteFailureLimit, totalCompleteFailureLimit),
        initializedOrElse(ffTotalPartialFailureLimit, totalPartialFailureLimit));
  }

  private BackoffPolicy buildBackOffPolicy(final Context ffContext) {
    final var ffMin = featureFlagClient.intVariation(CompleteFailureBackoffMinInterval.INSTANCE, ffContext);
    final var ffMax = featureFlagClient.intVariation(CompleteFailureBackoffMaxInterval.INSTANCE, ffContext);
    final var ffBase = featureFlagClient.intVariation(CompleteFailureBackoffBase.INSTANCE, ffContext);

    return new BackoffPolicy(
        Duration.ofSeconds(initializedOrElse(ffMin, minInterval)),
        Duration.ofSeconds(initializedOrElse(ffMax, maxInterval)),
        initializedOrElse(ffBase, backoffBase));
  }

  /**
   * Utility method for falling back to injected values when FFs are not initialized properly.
   */
  private int initializedOrElse(final int a, final int b) {
    return a == -1 ? b : a;
  }

  private Optional<RetryStateRead> fetchRetryState(final long jobId) throws RetryableException {
    final var req = new JobIdRequestBody(jobId);

    RetryStateRead resp;

    try {
      resp = airbyteApiClient.getJobRetryStatesApi().get(req);
    } catch (final ClientException e) {
      if (e.getStatusCode() != HttpStatus.NOT_FOUND.getCode()) {
        throw new RetryableException(e);
      }
      resp = null;
    } catch (final IOException e) {
      throw new RetryableException(e);
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
  public boolean persistRetryState(final long jobId, final UUID connectionId, final RetryManager manager) throws IOException {
    final var req = new JobRetryStateRequestBody(
        connectionId,
        jobId,
        manager.getSuccessiveCompleteFailures(),
        manager.getTotalCompleteFailures(),
        manager.getSuccessivePartialFailures(),
        manager.getTotalPartialFailures(),
        null);

    final var result = airbyteApiClient.getJobRetryStatesApi().createOrUpdateWithHttpInfo(req);

    // retryWithJitter returns null if unsuccessful
    return result != null;
  }

}
