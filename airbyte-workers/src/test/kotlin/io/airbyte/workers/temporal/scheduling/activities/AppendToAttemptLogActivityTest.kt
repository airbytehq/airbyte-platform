/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.temporal.TemporalUtils.Companion.getJobRoot
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity.LogInput
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.slf4j.Logger
import java.nio.file.Path

internal class AppendToAttemptLogActivityTest {
  private lateinit var mLogger: Logger
  private lateinit var mLogClientManager: LogClientManager

  @BeforeEach
  fun setup() {
    mLogger = mockk(relaxed = true)
    mLogClientManager = mockk(relaxed = true)
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

    verify(exactly = 1) { mLogClientManager.setJobMdc(expectedPath) }
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

    verify(exactly = 0) { mLogClientManager.setJobMdc(any<Path>()) }
    verify(exactly = 0) { mLogger.info(any<String>()) }
  }

  @ParameterizedTest
  @MethodSource("msgMatrix")
  fun logsMsgInfo(msg: String?) {
    val activity = AppendToAttemptLogActivityImpl(mLogClientManager, Fixtures.path1)
    activity.logger = mLogger
    val input = LogInput(1234L, 9, msg, AppendToAttemptLogActivity.LogLevel.INFO)

    activity.log(input)

    verify(exactly = 1) { mLogger.info(msg) }
  }

  @ParameterizedTest
  @MethodSource("msgMatrix")
  fun logsMsgError(msg: String?) {
    val activity = AppendToAttemptLogActivityImpl(mLogClientManager, Fixtures.path1)
    activity.logger = mLogger
    val input = LogInput(1234L, 9, msg, AppendToAttemptLogActivity.LogLevel.ERROR)

    activity.log(input)

    verify(exactly = 1) { mLogger.error(msg) }
  }

  @ParameterizedTest
  @MethodSource("msgMatrix")
  fun logsMsgWarn(msg: String?) {
    val activity = AppendToAttemptLogActivityImpl(mLogClientManager, Fixtures.path1)
    activity.logger = mLogger
    val input = LogInput(1234L, 9, msg, AppendToAttemptLogActivity.LogLevel.WARN)

    activity.log(input)

    verify(exactly = 1) { mLogger.warn(msg) }
  }

  private object Fixtures {
    var path1: Path = Path.of("test/path/")
    var path2: Path = Path.of("test/path/2")
  }

  companion object {
    @JvmStatic
    fun pathEnvJobAttemptMatrix() =
      listOf<Arguments?>(
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
    fun nullPathEnvJobAttemptMatrix() =
      listOf<Arguments?>(
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
    fun msgMatrix() =
      listOf<Arguments?>(
        Arguments.of("Something is borked."),
        Arguments.of("You messed up the thing."),
        Arguments.of("Total chaos!"),
        Arguments.of("Gurllllll."),
        Arguments.of("Like whatever."),
        Arguments.of("Fetch."),
      )
  }
}
