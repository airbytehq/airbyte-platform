/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import com.cronutils.descriptor.CronDescriptor
import com.cronutils.model.Cron
import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import jakarta.inject.Singleton
import java.time.ZonedDateTime
import java.util.Locale

/**
 * Helper service for validating, describing, and working with cron expressions.
 * Enforces Airbyte-specific constraints like minimum execution intervals.
 */
@Singleton
class CronExpressionHelper {
  companion object {
    private const val ONE_MINUTE_IN_SECONDS = 60L
    private const val FIFTEEN_MINUTES_IN_SECONDS = 15 * ONE_MINUTE_IN_SECONDS
    private const val ONE_HOUR_IN_SECONDS = 60 * ONE_MINUTE_IN_SECONDS
  }

  fun validateCronExpression(cronExpression: String): Cron {
    val cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ)
    val cron = CronParser(cronDefinition).parse(cronExpression)

    return try {
      cron.validate()
      // Airbyte never accepts cron expressions that execute more than once per minute
      checkDoesNotExecuteMoreThanOncePerMinute(cron)
      cron
    } catch (e: IllegalArgumentException) {
      throw IllegalArgumentException("Invalid cron expression: ${e.message}")
    }
  }

  /**
   * Converts a parsed cron expression to a human-readable description in English.
   *
   * @param cron the parsed cron expression
   * @return a human-readable description of when the cron will execute
   */
  fun describeCronExpression(cron: Cron): String = CronDescriptor.instance(Locale.ENGLISH).describe(cron)

  fun getNextExecutions(
    cron: Cron,
    numberOfExecutions: Int,
  ): List<Long> {
    val executionTime = ExecutionTime.forCron(cron)
    val nextExecutions = mutableListOf<Long>()
    var nextExecution = ZonedDateTime.now()

    for (i in 1..numberOfExecutions) {
      nextExecution = executionTime.nextExecution(nextExecution).orElse(null) ?: break
      nextExecutions.add(nextExecution.toEpochSecond())
    }

    return nextExecutions
  }

  fun executesMoreThanOncePerHour(cronExpression: String): Boolean = executesMoreThanOncePerHour(validateCronExpression(cronExpression))

  fun executesMoreThanOncePerHour(cron: Cron): Boolean = executesMoreThanOncePerInterval(cron, ONE_HOUR_IN_SECONDS)

  fun executesMoreThanOncePerFifteenMinutes(cronExpression: String): Boolean =
    executesMoreThanOncePerFifteenMinutes(validateCronExpression(cronExpression))

  fun executesMoreThanOncePerFifteenMinutes(cron: Cron): Boolean = executesMoreThanOncePerInterval(cron, FIFTEEN_MINUTES_IN_SECONDS)

  fun executesMoreThanOncePerInterval(
    cronExpression: String,
    minimumIntervalInSeconds: Long,
  ): Boolean = executesMoreThanOncePerInterval(validateCronExpression(cronExpression), minimumIntervalInSeconds)

  fun executesMoreThanOncePerInterval(
    cron: Cron,
    minimumIntervalInSeconds: Long,
  ): Boolean {
    val nextExecutions = getNextExecutions(cron, 3)
    // Make sure the time difference between the next 3 executions does not exceed the minimum interval.

    nextExecutions.zipWithNext { prev, next ->
      if (next - prev < minimumIntervalInSeconds) {
        return true
      }
    }
    return false
  }

  fun checkDoesNotExecuteMoreThanOncePerHour(cron: Cron) {
    checkDoesNotExecuteMoreThanOncePerInterval(cron, ONE_HOUR_IN_SECONDS, "1 hour")
  }

  fun checkDoesNotExecuteMoreThanOncePerFifteenMinutes(cron: Cron) {
    checkDoesNotExecuteMoreThanOncePerInterval(cron, FIFTEEN_MINUTES_IN_SECONDS, "15 minutes")
  }

  fun checkDoesNotExecuteMoreThanOncePerMinute(cron: Cron) {
    checkDoesNotExecuteMoreThanOncePerInterval(cron, ONE_MINUTE_IN_SECONDS, "1 minute")
  }

  fun checkDoesNotExecuteMoreThanOncePerInterval(
    cron: Cron,
    minimumIntervalInSeconds: Long,
    minimumIntervalDescription: String,
  ) {
    if (executesMoreThanOncePerInterval(cron, minimumIntervalInSeconds)) {
      throw IllegalArgumentException("Cron executions must be at least $minimumIntervalDescription apart")
    }
  }
}
