/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import io.airbyte.workers.helpers.ProgressChecker
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.Mock
import org.mockito.Mockito
import java.io.IOException
import java.util.UUID
import java.util.stream.Stream

internal class CheckRunProgressActivityTest {
  @Mock
  private var mProgressChecker: ProgressChecker? = null

  @BeforeEach
  fun setup() {
    mProgressChecker = Mockito.mock<ProgressChecker>(ProgressChecker::class.java)
  }

  @ParameterizedTest
  @MethodSource("jobAttemptMatrix")
  @Throws(IOException::class)
  fun delegatesToProgressChecker(
    jobId: Long,
    attemptNo: Int,
    madeProgress: Boolean,
  ) {
    val activity: CheckRunProgressActivity = CheckRunProgressActivityImpl(mProgressChecker!!)
    Mockito.`when`<Boolean?>(mProgressChecker!!.check(jobId, attemptNo)).thenReturn(madeProgress)

    val input = CheckRunProgressActivity.Input(jobId, attemptNo, UUID.randomUUID())
    val result = activity.checkProgress(input)

    Mockito.verify<ProgressChecker?>(mProgressChecker, Mockito.times(1)).check(jobId, attemptNo)

    Assertions.assertEquals(madeProgress, result.madeProgress())
  }

  companion object {
    @JvmStatic
    fun jobAttemptMatrix(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(1L, 0, true),
        Arguments.of(134512351235L, 7812387, false),
        Arguments.of(8L, 32, true),
        Arguments.of(8L, 32, false),
        Arguments.of(999L, 99, false),
      )
  }
}
