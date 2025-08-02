/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.temporal.TemporalUtils.Companion.getJobRoot
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity.LogInput
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.ArgumentMatchers
import org.mockito.Mock
import org.mockito.Mockito
import org.slf4j.Logger
import java.nio.file.Path
import java.util.stream.Stream

internal class AppendToAttemptLogActivityTest {
  @Mock
  private lateinit var mLogger: Logger

  @Mock
  private lateinit var mLogClientManager: LogClientManager

  @BeforeEach
  @Throws(Exception::class)
  fun setup() {
    mLogger = Mockito.mock<Logger>(Logger::class.java)
    mLogClientManager = Mockito.mock<LogClientManager>(LogClientManager::class.java)
  }

  @ParameterizedTest
  @MethodSource("pathEnvJobAttemptMatrix")
  fun setsMdc(
    path: Path,
    jobId: Long,
    attemptNumber: Int,
  ) {
    val activity = AppendToAttemptLogActivityImpl(mLogClientManager, path)
    activity.logger = mLogger
    val input = LogInput(jobId, attemptNumber, "msg", AppendToAttemptLogActivity.LogLevel.INFO)

    activity.log(input)

    val expectedPath = getJobRoot(path, jobId.toString(), attemptNumber.toLong())

    Mockito.verify(mLogClientManager, Mockito.times(1)).setJobMdc(expectedPath)
  }

  @ParameterizedTest
  @MethodSource("nullPathEnvJobAttemptMatrix")
  fun doesNotSetMdcOrLogIfInputsNull(
    path: Path,
    jobId: Long?,
    attemptNumber: Int?,
  ) {
    val activity = AppendToAttemptLogActivityImpl(mLogClientManager, path)
    activity.logger = mLogger
    val input = LogInput(jobId, attemptNumber, "msg", AppendToAttemptLogActivity.LogLevel.INFO)

    activity.log(input)

    Mockito.verify(mLogClientManager, Mockito.never()).setJobMdc(ArgumentMatchers.any<Path>())
    Mockito.verify(mLogger, Mockito.never()).info(ArgumentMatchers.any<String>())
  }

  @ParameterizedTest
  @MethodSource("msgMatrix")
  fun logsMsgInfo(msg: String?) {
    val activity = AppendToAttemptLogActivityImpl(mLogClientManager, Fixtures.path1)
    activity.logger = mLogger
    val input = LogInput(1234L, 9, msg, AppendToAttemptLogActivity.LogLevel.INFO)

    activity.log(input)

    Mockito.verify(mLogger, Mockito.times(1)).info(msg)
  }

  @ParameterizedTest
  @MethodSource("msgMatrix")
  fun logsMsgError(msg: String?) {
    val activity = AppendToAttemptLogActivityImpl(mLogClientManager, Fixtures.path1)
    activity.logger = mLogger
    val input = LogInput(1234L, 9, msg, AppendToAttemptLogActivity.LogLevel.ERROR)

    activity.log(input)

    Mockito.verify(mLogger, Mockito.times(1)).error(msg)
  }

  @ParameterizedTest
  @MethodSource("msgMatrix")
  fun logsMsgWarn(msg: String?) {
    val activity = AppendToAttemptLogActivityImpl(mLogClientManager, Fixtures.path1)
    activity.logger = mLogger
    val input = LogInput(1234L, 9, msg, AppendToAttemptLogActivity.LogLevel.WARN)

    activity.log(input)

    Mockito.verify(mLogger, Mockito.times(1)).warn(msg)
  }

  private object Fixtures {
    var path1: Path = Path.of("test/path/")
    var path2: Path = Path.of("test/path/2")
  }

  companion object {
    @JvmStatic
    fun pathEnvJobAttemptMatrix(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(Fixtures.path1, 91L, 2),
        Arguments.of(Fixtures.path1, 90L, 1),
        Arguments.of(Fixtures.path2, 91L, 2),
        Arguments.of(Fixtures.path2, 91L, 0),
        Arguments.of(Fixtures.path1, 9158L, 23),
        Arguments.of(Fixtures.path1, 1251L, 0),
        Arguments.of(Fixtures.path2, 65234L, 22),
        Arguments.of(Fixtures.path2, 97801L, 10),
      )

    @JvmStatic
    fun nullPathEnvJobAttemptMatrix(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(Fixtures.path1, null, 2),
        Arguments.of(Fixtures.path1, 90L, null),
        Arguments.of(Fixtures.path2, 91L, null),
        Arguments.of(Fixtures.path2, null, 0),
        Arguments.of(Fixtures.path1, null, null),
        Arguments.of(Fixtures.path1, null, null),
        Arguments.of(Fixtures.path2, null, 22),
        Arguments.of(Fixtures.path2, 97801L, null),
      )

    @JvmStatic
    fun msgMatrix(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of("Something is borked."),
        Arguments.of("You messed up the thing."),
        Arguments.of("Total chaos!"),
        Arguments.of("Gurllllll."),
        Arguments.of("Like whatever."),
        Arguments.of("Fetch."),
      )
  }
}
