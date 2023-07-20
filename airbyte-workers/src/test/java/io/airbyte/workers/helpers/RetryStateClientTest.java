/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.generated.JobRetryStatesApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.RetryStateRead;
import io.airbyte.featureflag.CompleteFailureBackoffBase;
import io.airbyte.featureflag.CompleteFailureBackoffMaxInterval;
import io.airbyte.featureflag.CompleteFailureBackoffMinInterval;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.SuccessiveCompleteFailureLimit;
import io.airbyte.featureflag.SuccessivePartialFailureLimit;
import io.airbyte.featureflag.TestClient;
import io.airbyte.featureflag.TotalCompleteFailureLimit;
import io.airbyte.featureflag.TotalPartialFailureLimit;
import io.micronaut.http.HttpStatus;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

class RetryStateClientTest {

  @Mock
  private JobRetryStatesApi mJobRetryStatesApi;

  @Mock
  private FeatureFlagClient mFeatureFlagClient;

  @BeforeEach
  public void setup() {
    mJobRetryStatesApi = Mockito.mock(JobRetryStatesApi.class);
    mFeatureFlagClient = Mockito.mock(TestClient.class);
    when(mFeatureFlagClient.intVariation(Mockito.any(), Mockito.any())).thenReturn(-1);
  }

  @Test
  void hydratesBackoffAndLimitsFromConstructor() {
    final var client = new RetryStateClient(
        mJobRetryStatesApi,
        mFeatureFlagClient,
        Fixtures.successiveCompleteFailureLimit,
        Fixtures.totalCompleteFailureLimit,
        Fixtures.successivePartialFailureLimit,
        Fixtures.totalPartialFailureLimit,
        Fixtures.minInterval,
        Fixtures.maxInterval,
        Fixtures.base);

    final var manager = client.hydrateRetryState(Fixtures.jobId, Fixtures.workspaceId);

    assertEquals(Fixtures.successiveCompleteFailureLimit, manager.getSuccessiveCompleteFailureLimit());
    assertEquals(Fixtures.totalCompleteFailureLimit, manager.getTotalCompleteFailureLimit());
    assertEquals(Fixtures.successivePartialFailureLimit, manager.getSuccessivePartialFailureLimit());
    assertEquals(Fixtures.totalPartialFailureLimit, manager.getTotalPartialFailureLimit());

    final var backoffPolicy = manager.getCompleteFailureBackoffPolicy();
    assertEquals(Duration.ofSeconds(Fixtures.minInterval), backoffPolicy.getMinInterval());
    assertEquals(Duration.ofSeconds(Fixtures.maxInterval), backoffPolicy.getMaxInterval());
    assertEquals(Fixtures.base, backoffPolicy.getBase());
  }

  @Test
  void featureFlagsOverrideValues() {
    final var client = new RetryStateClient(
        mJobRetryStatesApi,
        mFeatureFlagClient,
        Fixtures.successiveCompleteFailureLimit,
        Fixtures.totalCompleteFailureLimit,
        Fixtures.successivePartialFailureLimit,
        Fixtures.totalPartialFailureLimit,
        Fixtures.minInterval,
        Fixtures.maxInterval,
        Fixtures.base);

    when(mFeatureFlagClient.intVariation(eq(SuccessiveCompleteFailureLimit.INSTANCE), Mockito.any())).thenReturn(91);
    when(mFeatureFlagClient.intVariation(eq(TotalCompleteFailureLimit.INSTANCE), Mockito.any())).thenReturn(92);
    when(mFeatureFlagClient.intVariation(eq(SuccessivePartialFailureLimit.INSTANCE), Mockito.any())).thenReturn(93);
    when(mFeatureFlagClient.intVariation(eq(TotalPartialFailureLimit.INSTANCE), Mockito.any())).thenReturn(94);
    when(mFeatureFlagClient.intVariation(eq(CompleteFailureBackoffMinInterval.INSTANCE), Mockito.any())).thenReturn(0);
    when(mFeatureFlagClient.intVariation(eq(CompleteFailureBackoffMaxInterval.INSTANCE), Mockito.any())).thenReturn(96);
    when(mFeatureFlagClient.intVariation(eq(CompleteFailureBackoffBase.INSTANCE), Mockito.any())).thenReturn(97);

    final var manager = client.hydrateRetryState(Fixtures.jobId, Fixtures.workspaceId);

    assertEquals(91, manager.getSuccessiveCompleteFailureLimit());
    assertEquals(92, manager.getTotalCompleteFailureLimit());
    assertEquals(93, manager.getSuccessivePartialFailureLimit());
    assertEquals(94, manager.getTotalPartialFailureLimit());

    final var backoffPolicy = manager.getCompleteFailureBackoffPolicy();
    assertEquals(Duration.ofSeconds(0), backoffPolicy.getMinInterval());
    assertEquals(Duration.ofSeconds(96), backoffPolicy.getMaxInterval());
    assertEquals(97, backoffPolicy.getBase());
    assertEquals(Duration.ZERO, backoffPolicy.getBackoff(0));
    assertEquals(Duration.ZERO, backoffPolicy.getBackoff(1));
    assertEquals(Duration.ZERO, backoffPolicy.getBackoff(2));
    assertEquals(Duration.ZERO, backoffPolicy.getBackoff(3));
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
        mFeatureFlagClient,
        Fixtures.successiveCompleteFailureLimit,
        Fixtures.totalCompleteFailureLimit,
        Fixtures.successivePartialFailureLimit,
        Fixtures.totalPartialFailureLimit,
        Fixtures.minInterval,
        Fixtures.maxInterval,
        Fixtures.base);

    final var manager = client.hydrateRetryState(Fixtures.jobId, Fixtures.workspaceId);

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
        mFeatureFlagClient,
        Fixtures.successiveCompleteFailureLimit,
        Fixtures.totalCompleteFailureLimit,
        Fixtures.successivePartialFailureLimit,
        Fixtures.totalPartialFailureLimit,
        Fixtures.minInterval,
        Fixtures.maxInterval,
        Fixtures.base);

    final var manager = client.hydrateRetryState(Fixtures.jobId, Fixtures.workspaceId);

    assertEquals(0, manager.getTotalCompleteFailures());
    assertEquals(0, manager.getTotalPartialFailures());
    assertEquals(0, manager.getSuccessiveCompleteFailures());
    assertEquals(0, manager.getSuccessivePartialFailures());
  }

  @Test
  void initializesFailureCountsFreshWhenJobIdNull() {
    final var client = new RetryStateClient(
        mJobRetryStatesApi,
        mFeatureFlagClient,
        Fixtures.successiveCompleteFailureLimit,
        Fixtures.totalCompleteFailureLimit,
        Fixtures.successivePartialFailureLimit,
        Fixtures.totalPartialFailureLimit,
        Fixtures.minInterval,
        Fixtures.maxInterval,
        Fixtures.base);

    final var manager = client.hydrateRetryState(null, Fixtures.workspaceId);

    assertEquals(0, manager.getTotalCompleteFailures());
    assertEquals(0, manager.getTotalPartialFailures());
    assertEquals(0, manager.getSuccessiveCompleteFailures());
    assertEquals(0, manager.getSuccessivePartialFailures());
  }

  static class Fixtures {

    static long jobId = ThreadLocalRandom.current().nextLong();
    static UUID workspaceId = UUID.randomUUID();

    static int successiveCompleteFailureLimit = ThreadLocalRandom.current().nextInt();
    static int totalCompleteFailureLimit = ThreadLocalRandom.current().nextInt();
    static int successivePartialFailureLimit = ThreadLocalRandom.current().nextInt();
    static int totalPartialFailureLimit = ThreadLocalRandom.current().nextInt();

    static int totalCompleteFailures = ThreadLocalRandom.current().nextInt();
    static int totalPartialFailures = ThreadLocalRandom.current().nextInt();
    static int successiveCompleteFailures = ThreadLocalRandom.current().nextInt();
    static int successivePartialFailures = ThreadLocalRandom.current().nextInt();

    static int minInterval = 10;
    static int maxInterval = 1000;
    static int base = 2;

  }

}
