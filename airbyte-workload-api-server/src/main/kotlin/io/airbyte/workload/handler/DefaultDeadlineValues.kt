package io.airbyte.workload.handler

import jakarta.inject.Singleton
import java.time.Duration
import java.time.OffsetDateTime

@Singleton
class DefaultDeadlineValues {
  companion object {
    private val MAX_CREATE_TO_CLAIMED_INTERVAL = Duration.ofHours(2)
    private val MAX_CLAIMED_TO_LAUNCHED_INTERVAL = Duration.ofHours(1)
    private val MAX_DEFAULT_INTERVAL = Duration.ofMinutes(10)
  }

  fun offsetDateTime(): OffsetDateTime = OffsetDateTime.now()

  /**
   * This is the initial deadline we set after creation. We expect the workload to be set to 'claimed' before we hit this deadline.
   */
  fun createStepDeadline(): OffsetDateTime {
    return offsetDateTime().plus(MAX_CREATE_TO_CLAIMED_INTERVAL)
  }

  /**
   * This is the new deadline value we set once a workload is set to 'claimed'. We expect the workload to be set to 'launched' before we hit this
   * deadline. This is longer because we may be blocked on launching as our cluster scales up. This should be longer than the time we wait for pods to
   * start up.
   */
  fun claimStepDeadline(): OffsetDateTime {
    return offsetDateTime().plus(MAX_CLAIMED_TO_LAUNCHED_INTERVAL)
  }

  /**
   * This is the new deadline value we set once a workload is set to 'launched'.  We expect the workload to be set to 'running' before we hit this
   * deadline.
   */
  fun launchStepDeadline(): OffsetDateTime {
    return offsetDateTime().plus(MAX_DEFAULT_INTERVAL)
  }

  /**
   * This is the new deadline value we set once a workload is set to 'running'. We expect the workload to complete or heartbeat before we hit this
   * deadline.
   */
  fun runningStepDeadline(): OffsetDateTime {
    return offsetDateTime().plus(MAX_DEFAULT_INTERVAL)
  }

  /**
   * This is the new deadline value we set after we receive a heartbeat for that workload.
   */
  fun heartbeatDeadline(): OffsetDateTime {
    return offsetDateTime().plus(MAX_DEFAULT_INTERVAL)
  }
}
