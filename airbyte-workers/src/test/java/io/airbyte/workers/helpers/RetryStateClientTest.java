/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.generated.JobRetryStatesApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.RetryStateRead;
import io.airbyte.commons.temporal.scheduling.retries.BackoffPolicy;
import io.airbyte.commons.temporal.scheduling.retries.BackoffPolicy.BackoffPolicyBuilder;
import io.micronaut.http.HttpStatus;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

class RetryStateClientTest {

  @Mock
  private JobRetryStatesApi mJobRetryStatesApi;

  @BeforeEach
  public void setup() {
    mJobRetryStatesApi = Mockito.mock(JobRetryStatesApi.class);
  }

  @Test
  void hydratesBackoffAndLimitsFromConstructor() throws Exception {
    final var backoffPolicy = Fixtures.backoff().build();

    final var client = new RetryStateClient(
        mJobRetryStatesApi,
        backoffPolicy,
        Fixtures.successiveCompleteFailureLimit,
        Fixtures.totalCompleteFailureLimit,
        Fixtures.successivePartialFailureLimit,
        Fixtures.totalPartialFailureLimit);

    final var manager = client.hydrateRetryState(Fixtures.jobId);

    assertEquals(Fixtures.successiveCompleteFailureLimit, manager.getSuccessiveCompleteFailureLimit());
    assertEquals(Fixtures.totalCompleteFailureLimit, manager.getTotalCompleteFailureLimit());
    assertEquals(Fixtures.successivePartialFailureLimit, manager.getSuccessivePartialFailureLimit());
    assertEquals(Fixtures.totalPartialFailureLimit, manager.getTotalPartialFailureLimit());
    assertEquals(backoffPolicy, manager.getCompleteFailureBackoffPolicy());
  }

  @Test
  void hydratesFailureCountsFromApiIfPresent() throws Exception {
    final var retryStateRead = new RetryStateRead()
        .totalCompleteFailures(Fixtures.totalCompleteFailures)
        .totalPartialFailures(Fixtures.totalPartialFailures)
        .successiveCompleteFailures(Fixtures.successiveCompleteFailures)
        .successivePartialFailures(Fixtures.successivePartialFailures);

    when(mJobRetryStatesApi.get(any()))
        .thenReturn(retryStateRead);

    final var client = new RetryStateClient(
        mJobRetryStatesApi,
        Fixtures.backoff().build(),
        Fixtures.successiveCompleteFailureLimit,
        Fixtures.totalCompleteFailureLimit,
        Fixtures.successivePartialFailureLimit,
        Fixtures.totalPartialFailureLimit);

    final var manager = client.hydrateRetryState(Fixtures.jobId);

    assertEquals(Fixtures.totalCompleteFailures, manager.getTotalCompleteFailures());
    assertEquals(Fixtures.totalPartialFailures, manager.getTotalPartialFailures());
    assertEquals(Fixtures.successiveCompleteFailures, manager.getSuccessiveCompleteFailures());
    assertEquals(Fixtures.successivePartialFailures, manager.getSuccessivePartialFailures());
  }

  @Test
  void initializesFailureCountsFreshWhenApiReturnsNothing() throws Exception {
    when(mJobRetryStatesApi.get(any()))
        .thenThrow(new ApiException(HttpStatus.NOT_FOUND.getCode(), "Not Found."));

    final var client = new RetryStateClient(
        mJobRetryStatesApi,
        Fixtures.backoff().build(),
        Fixtures.successiveCompleteFailureLimit,
        Fixtures.totalCompleteFailureLimit,
        Fixtures.successivePartialFailureLimit,
        Fixtures.totalPartialFailureLimit);

    final var manager = client.hydrateRetryState(Fixtures.jobId);

    assertEquals(0, manager.getTotalCompleteFailures());
    assertEquals(0, manager.getTotalPartialFailures());
    assertEquals(0, manager.getSuccessiveCompleteFailures());
    assertEquals(0, manager.getSuccessivePartialFailures());
  }

  @Test
  void initializesFailureCountsFreshWhenJobIdNull() {
    final var client = new RetryStateClient(
        mJobRetryStatesApi,
        Fixtures.backoff().build(),
        Fixtures.successiveCompleteFailureLimit,
        Fixtures.totalCompleteFailureLimit,
        Fixtures.successivePartialFailureLimit,
        Fixtures.totalPartialFailureLimit);

    final var manager = client.hydrateRetryState(null);

    assertEquals(0, manager.getTotalCompleteFailures());
    assertEquals(0, manager.getTotalPartialFailures());
    assertEquals(0, manager.getSuccessiveCompleteFailures());
    assertEquals(0, manager.getSuccessivePartialFailures());
  }

  static class Fixtures {

    static long jobId = ThreadLocalRandom.current().nextLong();

    static int successiveCompleteFailureLimit = ThreadLocalRandom.current().nextInt();
    static int totalCompleteFailureLimit = ThreadLocalRandom.current().nextInt();
    static int successivePartialFailureLimit = ThreadLocalRandom.current().nextInt();
    static int totalPartialFailureLimit = ThreadLocalRandom.current().nextInt();

    static int totalCompleteFailures = ThreadLocalRandom.current().nextInt();
    static int totalPartialFailures = ThreadLocalRandom.current().nextInt();
    static int successiveCompleteFailures = ThreadLocalRandom.current().nextInt();
    static int successivePartialFailures = ThreadLocalRandom.current().nextInt();

    static BackoffPolicyBuilder backoff() {
      return BackoffPolicy.builder();
    }

  }

}
