/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.airbyte.workers.helpers.ProgressChecker;
import io.airbyte.workers.temporal.scheduling.activities.CheckRunProgressActivity.Input;
import java.io.IOException;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;

class CheckRunProgressActivityTest {

  @Mock
  private ProgressChecker mProgressChecker;

  @BeforeEach
  public void setup() {
    mProgressChecker = Mockito.mock(ProgressChecker.class);
  }

  @ParameterizedTest
  @MethodSource("jobAttemptMatrix")
  void delegatesToProgressChecker(final long jobId, final int attemptNo, final boolean madeProgress) throws IOException {
    final CheckRunProgressActivity activity = new CheckRunProgressActivityImpl(mProgressChecker);
    when(mProgressChecker.check(jobId, attemptNo)).thenReturn(madeProgress);

    final CheckRunProgressActivity.Input input = new Input(jobId, attemptNo, UUID.randomUUID());
    final var result = activity.checkProgress(input);

    verify(mProgressChecker, times(1)).check(jobId, attemptNo);

    assertEquals(madeProgress, result.madeProgress());
  }

  public static Stream<Arguments> jobAttemptMatrix() {
    return Stream.of(
        Arguments.of(1L, 0, true),
        Arguments.of(134512351235L, 7812387, false),
        Arguments.of(8L, 32, true),
        Arguments.of(8L, 32, false),
        Arguments.of(999L, 99, false));
  }

}
