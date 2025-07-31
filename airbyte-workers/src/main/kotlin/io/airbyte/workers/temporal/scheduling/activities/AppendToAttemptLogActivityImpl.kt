/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities

import com.google.common.annotations.VisibleForTesting
import io.airbyte.commons.logging.LogClientManager
import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.logging.MdcScope
import io.airbyte.commons.temporal.TemporalUtils.Companion.getJobRoot
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity.LogInput
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity.LogOutput
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.file.Path

/**
 * Concrete implementation of AppendToAttemptLogActivity.
 */
@Singleton
class AppendToAttemptLogActivityImpl(
  private val logClientManager: LogClientManager,
  @param:Named("workspaceRoot") private val workspaceRoot: Path,
) : AppendToAttemptLogActivity {
  @JvmField
  @VisibleForTesting
  var logger: Logger = LoggerFactory.getLogger(AppendToAttemptLogActivityImpl::class.java)

  override fun log(input: LogInput): LogOutput {
    if (input.jobId == null || input.attemptNumber == null) {
      return LogOutput(false)
    }

    setMdc(input)

    try {
      val msg = input.message
      MdcScope.Builder().setExtraMdcEntries(LogSource.PLATFORM.toMdc().toMap()).build().use { mdcScope ->
        when (input.level) {
          AppendToAttemptLogActivity.LogLevel.ERROR -> logger.error(msg)
          AppendToAttemptLogActivity.LogLevel.WARN -> logger.warn(msg)
          else -> logger.info(msg)
        }
      }
      return LogOutput(true)
    } finally {
      unsetMdc()
    }
  }

  private fun setMdc(input: LogInput) {
    val jobRoot =
      getJobRoot(
        workspaceRoot,
        input.jobId.toString(),
        input.attemptNumber!!.toLong(),
      )

    logClientManager.setJobMdc(jobRoot)
  }

  private fun unsetMdc() {
    logClientManager.setJobMdc(null)
  }
}
