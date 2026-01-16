/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.workers.helpers.ProgressChecker
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID

internal class CheckRunProgressActivityTest {
  private var mProgressChecker: ProgressChecker? = null

  @BeforeEach
  fun setup() {
    mProgressChecker = mockk<ProgressChecker>()
  }

  @ParameterizedTest
  @MethodSource("jobAttemptMatrix")
  fun delegatesToProgressChecker(
    jobId: Long,
    attemptNo: Int,
    madeProgress: Boolean,
  ) {
    val activity: CheckRunProgressActivity = CheckRunProgressActivityImpl(mProgressChecker!!)
    every { mProgressChecker!!.check(jobId, attemptNo) } returns madeProgress

    val input = CheckRunProgressActivity.Input(jobId, attemptNo, UUID.randomUUID())
    val result = activity.checkProgress(input)

    verify(exactly = 1) { mProgressChecker!!.check(jobId, attemptNo) }

    assertEquals(madeProgress, result.madeProgress())
  }

  @ParameterizedTest
  @CsvSource("1,", ",1", ",")
  fun testNullJobOrAttemptNumber(
    jobId: Long?,
    attemptNumber: Int?,
  ) {
    val activity: CheckRunProgressActivity = CheckRunProgressActivityImpl(mProgressChecker!!)
    assertEquals(false, activity.checkProgress(CheckRunProgressActivity.Input(jobId, attemptNumber, UUID.randomUUID())).madeProgress())
  }

  companion object {
    @JvmStatic
    fun jobAttemptMatrix() =
      listOf<Arguments?>(
        Arguments.of(1L, 0, true),
        Arguments.of(134512351235L, 7812387, false),
        Arguments.of(8L, 32, true),
        Arguments.of(8L, 32, false),
        Arguments.of(999L, 99, false),
      )
  }
}
