package io.airbyte.workload.handler

import jakarta.inject.Singleton
import java.time.OffsetDateTime

@Singleton
class DefaultDeadlineValues {
  companion object {
    private const val MINUTE_TIMEOUT: Long = 10
    private const val HOUR_TIMEOUT: Long = 2
  }

  fun offsetDateTime(): OffsetDateTime = OffsetDateTime.now()

  fun createStepDeadline(): OffsetDateTime {
    return offsetDateTime().plusHours(HOUR_TIMEOUT)
  }

  fun claimStepDeadline(): OffsetDateTime {
    return offsetDateTime().plusMinutes(MINUTE_TIMEOUT * 2)
  }

  fun runningStepDeadline(): OffsetDateTime {
    return offsetDateTime().plusMinutes(MINUTE_TIMEOUT)
  }

  fun launchStepDeadline(): OffsetDateTime {
    return offsetDateTime().plusMinutes(MINUTE_TIMEOUT)
  }

  fun heartbeatDeadline(): OffsetDateTime {
    return offsetDateTime().plusMinutes(MINUTE_TIMEOUT)
  }
}
