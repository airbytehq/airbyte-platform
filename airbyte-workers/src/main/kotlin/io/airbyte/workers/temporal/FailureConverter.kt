/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal

import io.airbyte.commons.temporal.utils.ActivityFailureClassifier
import io.airbyte.config.ActorType
import io.airbyte.config.FailureReason
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Duration
import kotlin.time.toKotlinDuration

private val log = KotlinLogging.logger {}

class FailureConverter {
  @JvmOverloads
  fun getFailureReason(
    commandName: String,
    actorType: ActorType,
    e: Exception,
    timeout: Duration? = null,
  ): FailureReason = getFailureReason(commandName, actorType, e, timeout?.toKotlinDuration())

  fun getFailureReason(
    commandName: String,
    actorType: ActorType,
    e: Exception,
    timeout: kotlin.time.Duration?,
  ): FailureReason {
    val failureReason =
      FailureReason()
        .withFailureOrigin(if (actorType == ActorType.SOURCE) FailureReason.FailureOrigin.SOURCE else FailureReason.FailureOrigin.DESTINATION)
        .withStacktrace(e.stackTraceToString())
    val classifiedExc = ActivityFailureClassifier.classifyException(e)
    log.error { "exception classified as $classifiedExc" }
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
