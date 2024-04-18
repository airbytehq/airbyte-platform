package io.airbyte.workers.temporal

import io.airbyte.commons.temporal.utils.ActivityFailureClassifier
import io.airbyte.config.ActorType
import io.airbyte.config.FailureReason
import org.apache.commons.lang3.exception.ExceptionUtils
import org.slf4j.LoggerFactory
import java.lang.String
import kotlin.time.Duration
import kotlin.time.toKotlinDuration

class FailureConverter {
  @JvmOverloads
  fun getFailureReason(
    commandName: String,
    actorType: ActorType,
    e: Exception,
    timeout: java.time.Duration? = null,
  ): FailureReason = getFailureReason(commandName, actorType, e, timeout?.toKotlinDuration())

  fun getFailureReason(
    commandName: String,
    actorType: ActorType,
    e: Exception,
    timeout: Duration?,
  ): FailureReason {
    val failureReason =
      FailureReason()
        .withFailureOrigin(if (actorType == ActorType.SOURCE) FailureReason.FailureOrigin.SOURCE else FailureReason.FailureOrigin.DESTINATION)
        .withStacktrace(ExceptionUtils.getStackTrace(e))
    val classifiedExc = ActivityFailureClassifier.classifyException(e)
    LoggerFactory.getLogger("test").error("exception classified as $classifiedExc")
    when (classifiedExc) {
      ActivityFailureClassifier.TemporalFailureReason.HEARTBEAT ->
        failureReason
          .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
          .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
          .withExternalMessage("$commandName connection failed because of an internal error.")
          .withInternalMessage("$commandName pod failed to heartbeat, verify resource and heath of the worker/check pods.")

      ActivityFailureClassifier.TemporalFailureReason.SCHEDULER_OVERLOADED ->
        failureReason
          .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
          .withFailureType(FailureReason.FailureType.TRANSIENT_ERROR)
          .withExternalMessage("Airbyte Platform is experiencing a higher than usual load, please try again later.")
          .withInternalMessage("$commandName wasn't able to start within the expected time, verify scheduler and worker load.")

      ActivityFailureClassifier.TemporalFailureReason.OPERATION_TIMEOUT ->
        failureReason
          .withExternalMessage("$commandName took too long.")
          .withInternalMessage("$commandName exceeded the timeout${timeout?.let { " of ${it.inWholeMinutes} minutes" }.orEmpty()}.")

      ActivityFailureClassifier.TemporalFailureReason.UNKNOWN, ActivityFailureClassifier.TemporalFailureReason.NOT_A_TIMEOUT ->
        failureReason
          .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
          .withExternalMessage("$commandName failed because of an internal error")
          .withInternalMessage("$commandName failed because of an internal error")

      else ->
        failureReason
          .withFailureOrigin(FailureReason.FailureOrigin.AIRBYTE_PLATFORM)
          .withExternalMessage("$commandName failed because of an internal error")
          .withInternalMessage("$commandName failed because of an internal error")
    }
    return failureReason
  }
}
