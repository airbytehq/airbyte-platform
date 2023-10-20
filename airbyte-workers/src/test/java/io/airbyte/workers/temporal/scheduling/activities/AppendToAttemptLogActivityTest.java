/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.helpers.LogClientSingleton;
import io.airbyte.config.helpers.LogConfigs;
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
  private LogClientSingleton mLogClientSingleton;

  @Mock
  private LogConfigs mLogConfigs;

  @BeforeEach
  public void setup() throws Exception {
    mLogger = Mockito.mock(Logger.class);
    mLogClientSingleton = Mockito.mock(LogClientSingleton.class);
    mLogConfigs = Mockito.mock(LogConfigs.class);
  }

  @ParameterizedTest
  @MethodSource("pathEnvJobAttemptMatrix")
  void setsMdc(final Path path, final WorkerEnvironment env, final long jobId, final int attemptNumber) {
    final var activity = new AppendToAttemptLogActivityImpl(mLogClientSingleton, mLogConfigs, path, env);
    activity.logger = mLogger;
    final var input = new LogInput(jobId, attemptNumber, "msg", LogLevel.INFO);

    activity.log(input);

    final var expectedPath = TemporalUtils.getJobRoot(path, String.valueOf(jobId), attemptNumber);

    Mockito.verify(mLogClientSingleton, Mockito.times(1)).setJobMdc(env, mLogConfigs, expectedPath);
  }

  public static Stream<Arguments> pathEnvJobAttemptMatrix() {
    return Stream.of(
        Arguments.of(Fixtures.path1, Fixtures.kube, 91L, 2),
        Arguments.of(Fixtures.path1, Fixtures.docker, 90L, 1),
        Arguments.of(Fixtures.path2, Fixtures.kube, 91L, 2),
        Arguments.of(Fixtures.path2, Fixtures.docker, 91L, 0),
        Arguments.of(Fixtures.path1, Fixtures.kube, 9158L, 23),
        Arguments.of(Fixtures.path1, Fixtures.docker, 1251L, 0),
        Arguments.of(Fixtures.path2, Fixtures.kube, 65234L, 22),
        Arguments.of(Fixtures.path2, Fixtures.docker, 97801L, 10));
  }

  @ParameterizedTest
  @MethodSource("nullPathEnvJobAttemptMatrix")
  void doesNotSetMdcOrLogIfInputsNull(final Path path, final WorkerEnvironment env, final Long jobId, final Integer attemptNumber) {
    final var activity = new AppendToAttemptLogActivityImpl(mLogClientSingleton, mLogConfigs, path, env);
    activity.logger = mLogger;
    final var input = new LogInput(jobId, attemptNumber, "msg", LogLevel.INFO);

    activity.log(input);

    Mockito.verify(mLogClientSingleton, Mockito.never()).setJobMdc(Mockito.any(), Mockito.any(), Mockito.any());
    Mockito.verify(mLogger, Mockito.never()).info(Mockito.any());
  }

  public static Stream<Arguments> nullPathEnvJobAttemptMatrix() {
    return Stream.of(
        Arguments.of(Fixtures.path1, Fixtures.kube, null, 2),
        Arguments.of(Fixtures.path1, Fixtures.docker, 90L, null),
        Arguments.of(Fixtures.path2, Fixtures.kube, 91L, null),
        Arguments.of(Fixtures.path2, Fixtures.docker, null, 0),
        Arguments.of(Fixtures.path1, Fixtures.kube, null, null),
        Arguments.of(Fixtures.path1, Fixtures.docker, null, null),
        Arguments.of(Fixtures.path2, Fixtures.kube, null, 22),
        Arguments.of(Fixtures.path2, Fixtures.docker, 97801L, null));
  }

  @ParameterizedTest
  @MethodSource("msgMatrix")
  void logsMsgInfo(final String msg) {
    final var activity = new AppendToAttemptLogActivityImpl(mLogClientSingleton, mLogConfigs, Fixtures.path1, Fixtures.docker);
    activity.logger = mLogger;
    final var input = new LogInput(1234L, 9, msg, LogLevel.INFO);

    activity.log(input);

    Mockito.verify(mLogger, Mockito.times(1)).info(msg);
  }

  @ParameterizedTest
  @MethodSource("msgMatrix")
  void logsMsgError(final String msg) {
    final var activity = new AppendToAttemptLogActivityImpl(mLogClientSingleton, mLogConfigs, Fixtures.path1, Fixtures.docker);
    activity.logger = mLogger;
    final var input = new LogInput(1234L, 9, msg, LogLevel.ERROR);

    activity.log(input);

    Mockito.verify(mLogger, Mockito.times(1)).error(msg);
  }

  @ParameterizedTest
  @MethodSource("msgMatrix")
  void logsMsgWarn(final String msg) {
    final var activity = new AppendToAttemptLogActivityImpl(mLogClientSingleton, mLogConfigs, Fixtures.path1, Fixtures.docker);
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

    static WorkerEnvironment docker = WorkerEnvironment.DOCKER;
    static WorkerEnvironment kube = WorkerEnvironment.KUBERNETES;

  }

}
