/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */
package io.airbyte.commons.temporal.utils

import io.temporal.api.enums.v1.TimeoutType
import io.temporal.failure.ActivityFailure
import io.temporal.failure.TimeoutFailure

object ActivityFailureClassifier {
  @JvmStatic
  fun classifyException(e: Exception): TemporalFailureReason =
    when (e) {
      is ActivityFailure ->
        when (e.cause) {
          is TimeoutFailure ->
            when ((e.cause as TimeoutFailure).timeoutType) {
              // ScheduleToClose or StartToClose happen when the activity runs longer than the configured timeout.
              // This is most likely an issue with the computation itself more than the infra.
              TimeoutType.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE, TimeoutType.TIMEOUT_TYPE_START_TO_CLOSE -> TemporalFailureReason.OPERATION_TIMEOUT

              // This is because we failed our background heartbeat.
              // Either the app in charge of heartbeat disappeared or got stuck.
              TimeoutType.TIMEOUT_TYPE_HEARTBEAT -> TemporalFailureReason.HEARTBEAT

              // We consider the rest as infra issue, we were most likely not able to start the task within the allocated time.
              // Here is most likely TimeoutType.TIMEOUT_TYPE_SCHEDULE_TO_START or TimeoutType.UNRECOGNIZED
              else -> TemporalFailureReason.SCHEDULER_OVERLOADED
            }

          // This is a temporal error unrelated to a timeout. We do not have a more precised classification at the moment.
          else -> TemporalFailureReason.NOT_A_TIMEOUT
        }

      // This isn't an ActivityFailure exception, should be classified outside of this method
      else -> TemporalFailureReason.UNKNOWN
    }

  enum class TemporalFailureReason {
    UNKNOWN,
    NOT_A_TIMEOUT,
    SCHEDULER_OVERLOADED,
    HEARTBEAT,
    OPERATION_TIMEOUT,
  }
}
