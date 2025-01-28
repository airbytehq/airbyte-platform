/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.helpers

import com.cronutils.descriptor.CronDescriptor
import com.cronutils.model.Cron
import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import jakarta.inject.Singleton
import java.time.ZonedDateTime
import java.util.Locale

@Singleton
class CronExpressionHelper {
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

  fun checkDoesNotExecuteMoreThanOncePerHour(cron: Cron) {
    val nextExecutions = getNextExecutions(cron, 3)
    // Make sure the time difference between the next 3 executions does not exceed 1 hour

    nextExecutions.zipWithNext { prev, next ->
      if (next - prev < 3600) {
        throw IllegalArgumentException(
          "Cron executions must be more than 1 hour apart",
        )
      }
    }
  }

  fun checkDoesNotExecuteMoreThanOncePerMinute(cron: Cron) {
    val nextExecutions = getNextExecutions(cron, 3)
    // Make sure the time difference between the next 3 executions does not exceed 1 minute

    nextExecutions.zipWithNext { prev, next ->
      if (next - prev < 60) {
        throw IllegalArgumentException(
          "Cron executions must be more than 1 minute apart",
        )
      }
    }
  }
}
