/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config.helpers

import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.parser.CronParser
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

const val EVERY_HOUR = "0 0 * * * ?"
const val EVERY_MINUTE = "0 * * * * ?"
const val EVERY_SECOND = "* * * * * ?"
const val EVERY_HALF_MINUTE = "*/2 * * * * ?"
const val SOMETIMES_EVERY_SECOND = "59,0 0 12,18 * * ?"
const val Y2K = "0 0 0 1 1 ? 2000"

class CronExpressionHelperTest {
  private var cronExpressionHelper: CronExpressionHelper = CronExpressionHelper()
  private val cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ)

  @Test
  fun testNextExecutionsEveryHour() {
    val everyHour = cronExpressionHelper.validateCronExpression(EVERY_HOUR)
    val result = cronExpressionHelper.getNextExecutions(everyHour, 2)

    assertEquals(2, result.size)
    assertEquals(3600, result[1] - result[0])
  }

  @ParameterizedTest
  @ValueSource(strings = [EVERY_SECOND, SOMETIMES_EVERY_SECOND, EVERY_HALF_MINUTE])
  fun testCheckDoesNotExecuteMoreThanOncePerMinuteThrows(cronExpression: String) {
    val cron = CronParser(cronDefinition).parse(cronExpression)
    assertThrows<IllegalArgumentException> {
      cronExpressionHelper.checkDoesNotExecuteMoreThanOncePerMinute(cron)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = [EVERY_HOUR, Y2K, EVERY_MINUTE])
  fun testCheckDoesNotExecuteMoreThanOncePerMinutePasses(cronExpression: String) {
    val cron = CronParser(cronDefinition).parse(cronExpression)
    assertDoesNotThrow {
      cronExpressionHelper.checkDoesNotExecuteMoreThanOncePerMinute(cron)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = [EVERY_SECOND, SOMETIMES_EVERY_SECOND, EVERY_HALF_MINUTE])
  fun testCheckDoesNotExecuteMoreThanOncePerHourThrows(cronExpression: String) {
    val cron = CronParser(cronDefinition).parse(cronExpression)
    assertThrows<IllegalArgumentException> {
      cronExpressionHelper.checkDoesNotExecuteMoreThanOncePerHour(cron)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = [EVERY_HOUR])
  fun testCheckDoesNotExecuteMoreThanOncePerHourPasses(cronExpression: String) {
    val cron = CronParser(cronDefinition).parse(cronExpression)
    assertDoesNotThrow {
      cronExpressionHelper.checkDoesNotExecuteMoreThanOncePerHour(cron)
    }
  }
}
