/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.AirbyteApiClient;
import io.airbyte.api.client.generated.AttemptApi;
import io.airbyte.api.client.model.generated.AttemptStats;
import io.micronaut.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.openapitools.client.infrastructure.ClientException;

class ProgressCheckerTest {

  @Mock
  private AirbyteApiClient mAirbyteApiClient;

  @Mock
  private AttemptApi mAttemptApi;

  @Mock
  private ProgressCheckerPredicates mPredicates;

  @BeforeEach
  public void setup() {
    mAirbyteApiClient = mock(AirbyteApiClient.class);
    mAttemptApi = mock(AttemptApi.class);
    mPredicates = mock(ProgressCheckerPredicates.class);

    when(mAirbyteApiClient.getAttemptApi()).thenReturn(mAttemptApi);
  }

  @Test
  void noRespReturnsFalse() throws Exception {
    final ProgressChecker activity = new ProgressChecker(mAirbyteApiClient, mPredicates);
    when(mAttemptApi.getAttemptCombinedStats(Mockito.any()))
        .thenReturn(null);

    final var result = activity.check(Fixtures.jobId1, Fixtures.attemptNo1);

    assertFalse(result);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void respReturnsCheckedValue(final boolean madeProgress) throws Exception {
    final ProgressChecker activity = new ProgressChecker(mAirbyteApiClient, mPredicates);
    when(mAttemptApi.getAttemptCombinedStats(Mockito.any()))
        .thenReturn(new AttemptStats());
    when(mPredicates.test(Mockito.any()))
        .thenReturn(madeProgress);

    final var result = activity.check(Fixtures.jobId1, Fixtures.attemptNo1);

    assertEquals(madeProgress, result);
  }

  @Test
  void notFoundsAreTreatedAsNoProgress() throws Exception {
    final ProgressChecker activity = new ProgressChecker(mAirbyteApiClient, mPredicates);
    when(mAttemptApi.getAttemptCombinedStats(Mockito.any()))
        .thenThrow(new ClientException("Not Found.", HttpStatus.NOT_FOUND.getCode(), null));

    final var result = activity.check(Fixtures.jobId1, Fixtures.attemptNo1);

    assertFalse(result);
  }

  private static class Fixtures {

    static long jobId1 = 1;

    static int attemptNo1 = 0;

  }

}
