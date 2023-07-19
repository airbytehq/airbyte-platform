/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

import io.airbyte.api.client.generated.AttemptApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.AttemptStats;
import io.micronaut.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;

class ProgressCheckerTest {

  @Mock
  private AttemptApi mAttemptApi;

  @Mock
  private ProgressCheckerPredicates mPredicates;

  @BeforeEach
  public void setup() {
    mAttemptApi = Mockito.mock(AttemptApi.class);
    mPredicates = Mockito.mock(ProgressCheckerPredicates.class);
  }

  @Test
  void noRespReturnsFalse() throws Exception {
    final ProgressChecker activity = new ProgressChecker(mAttemptApi, mPredicates);
    when(mAttemptApi.getAttemptCombinedStats(Mockito.any()))
        .thenReturn(null);

    final var result = activity.check(Fixtures.jobId1, Fixtures.attemptNo1);

    assertFalse(result);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void respReturnsCheckedValue(final boolean madeProgress) throws Exception {
    final ProgressChecker activity = new ProgressChecker(mAttemptApi, mPredicates);
    when(mAttemptApi.getAttemptCombinedStats(Mockito.any()))
        .thenReturn(new AttemptStats());
    when(mPredicates.test(Mockito.any()))
        .thenReturn(madeProgress);

    final var result = activity.check(Fixtures.jobId1, Fixtures.attemptNo1);

    assertEquals(madeProgress, result);
  }

  @Test
  void notFoundsAreTreatedAsNoProgress() throws Exception {
    final ProgressChecker activity = new ProgressChecker(mAttemptApi, mPredicates);
    when(mAttemptApi.getAttemptCombinedStats(Mockito.any()))
        .thenThrow(new ApiException(HttpStatus.NOT_FOUND.getCode(), "Not Found."));

    final var result = activity.check(Fixtures.jobId1, Fixtures.attemptNo1);

    assertFalse(result);
  }

  private static class Fixtures {

    static long jobId1 = 1;

    static int attemptNo1 = 0;

  }

}
