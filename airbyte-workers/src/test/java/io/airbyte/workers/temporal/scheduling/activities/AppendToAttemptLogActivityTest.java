/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.commons.logging.LogClientManager;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity.LogInput;
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity.LogLevel;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;

class AppendToAttemptLogActivityTest {

  @Mock
  private Logger mLogger;

  @Mock
  private LogClientManager mLogClientManager;

  @BeforeEach
  public void setup() throws Exception {
    mLogger = Mockito.mock(Logger.class);
    mLogClientManager = Mockito.mock(LogClientManager.class);
  }

  @ParameterizedTest
  @MethodSource("pathEnvJobAttemptMatrix")
  void setsMdc(final Path path, final long jobId, final int attemptNumber) {
    final var activity = new AppendToAttemptLogActivityImpl(mLogClientManager, path);
    activity.logger = mLogger;
    final var input = new LogInput(jobId, attemptNumber, "msg", LogLevel.INFO);

    activity.log(input);

    final var expectedPath = TemporalUtils.getJobRoot(path, String.valueOf(jobId), attemptNumber);

    Mockito.verify(mLogClientManager, Mockito.times(1)).setJobMdc(expectedPath);
  }

  public static Stream<Arguments> pathEnvJobAttemptMatrix() {
    return Stream.of(
        Arguments.of(Fixtures.path1, 91L, 2),
        Arguments.of(Fixtures.path1, 90L, 1),
        Arguments.of(Fixtures.path2, 91L, 2),
        Arguments.of(Fixtures.path2, 91L, 0),
        Arguments.of(Fixtures.path1, 9158L, 23),
        Arguments.of(Fixtures.path1, 1251L, 0),
        Arguments.of(Fixtures.path2, 65234L, 22),
        Arguments.of(Fixtures.path2, 97801L, 10));
  }

  @ParameterizedTest
  @MethodSource("nullPathEnvJobAttemptMatrix")
  void doesNotSetMdcOrLogIfInputsNull(final Path path, final Long jobId, final Integer attemptNumber) {
    final var activity = new AppendToAttemptLogActivityImpl(mLogClientManager, path);
    activity.logger = mLogger;
    final var input = new LogInput(jobId, attemptNumber, "msg", LogLevel.INFO);

    activity.log(input);

    Mockito.verify(mLogClientManager, Mockito.never()).setJobMdc(Mockito.any());
    Mockito.verify(mLogger, Mockito.never()).info(Mockito.any());
  }

  public static Stream<Arguments> nullPathEnvJobAttemptMatrix() {
    return Stream.of(
        Arguments.of(Fixtures.path1, null, 2),
        Arguments.of(Fixtures.path1, 90L, null),
        Arguments.of(Fixtures.path2, 91L, null),
        Arguments.of(Fixtures.path2, null, 0),
        Arguments.of(Fixtures.path1, null, null),
        Arguments.of(Fixtures.path1, null, null),
        Arguments.of(Fixtures.path2, null, 22),
        Arguments.of(Fixtures.path2, 97801L, null));
  }

  @ParameterizedTest
  @MethodSource("msgMatrix")
  void logsMsgInfo(final String msg) {
    final var activity = new AppendToAttemptLogActivityImpl(mLogClientManager, Fixtures.path1);
    activity.logger = mLogger;
    final var input = new LogInput(1234L, 9, msg, LogLevel.INFO);

    activity.log(input);

    Mockito.verify(mLogger, Mockito.times(1)).info(msg);
  }

  @ParameterizedTest
  @MethodSource("msgMatrix")
  void logsMsgError(final String msg) {
    final var activity = new AppendToAttemptLogActivityImpl(mLogClientManager, Fixtures.path1);
    activity.logger = mLogger;
    final var input = new LogInput(1234L, 9, msg, LogLevel.ERROR);

    activity.log(input);

    Mockito.verify(mLogger, Mockito.times(1)).error(msg);
  }

  @ParameterizedTest
  @MethodSource("msgMatrix")
  void logsMsgWarn(final String msg) {
    final var activity = new AppendToAttemptLogActivityImpl(mLogClientManager, Fixtures.path1);
    activity.logger = mLogger;
    final var input = new LogInput(1234L, 9, msg, LogLevel.WARN);

    activity.log(input);

    Mockito.verify(mLogger, Mockito.times(1)).warn(msg);
  }

  public static Stream<Arguments> msgMatrix() {
    return Stream.of(
        Arguments.of("Something is borked."),
        Arguments.of("You messed up the thing."),
        Arguments.of("Total chaos!"),
        Arguments.of("Gurllllll."),
        Arguments.of("Like whatever."),
        Arguments.of("Fetch."));
  }

  private static final class Fixtures {

    static Path path1 = Path.of("test/path/");
    static Path path2 = Path.of("test/path/2");

  }

}
