/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import io.airbyte.api.model.generated.WebBackendDescribeCronExpressionRequestBody
import io.airbyte.api.problems.throwable.generated.CronValidationInvalidExpressionProblem
import io.airbyte.config.helpers.CronExpressionHelper
import junit.framework.TestCase.assertEquals
import junit.framework.TestCase.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

const val CRON_EVERY_HOUR = "0 0 * * * ?"
const val CRON_EVERY_MINUTE = "0 * * * * ?"
const val Y2K = "0 0 0 1 1 ? 2000"

class WebBackendCronExpressionHandlerTest {
  private val cronExpressionHelper: CronExpressionHelper = CronExpressionHelper()
  private val webBackendCronExpressionHandler: WebBackendCronExpressionHandler = WebBackendCronExpressionHandler(cronExpressionHelper)

  @Test
  fun testDescribeEveryHourCronExpression() {
    val body = WebBackendDescribeCronExpressionRequestBody().cronExpression(CRON_EVERY_HOUR)
    val result = webBackendCronExpressionHandler.describeCronExpression(body)

    assertNotNull(result)
    if (result != null) {
      assertEquals(CRON_EVERY_HOUR, result.cronExpression)
      assertEquals(3, result.nextExecutions.size)
      assertEquals(result.nextExecutions[1] - result.nextExecutions[0], 3600)
    }
  }

  @Test
  fun testDescribeEveryMinuteCronExpression() {
    val body = WebBackendDescribeCronExpressionRequestBody().cronExpression(CRON_EVERY_MINUTE)
    val result = webBackendCronExpressionHandler.describeCronExpression(body)

    assertNotNull(result)
    if (result != null) {
      assertEquals(CRON_EVERY_MINUTE, result.cronExpression)
      assertEquals(3, result.nextExecutions.size)
      assertEquals(result.nextExecutions[1] - result.nextExecutions[0], 60)
    }
  }

  @Test
  fun testDescribeY2KCronExpression() {
    val body = WebBackendDescribeCronExpressionRequestBody().cronExpression(Y2K)
    val result = webBackendCronExpressionHandler.describeCronExpression(body)

    assertNotNull(result)
    if (result != null) {
      assertEquals(Y2K, result.cronExpression)
      // Y2K already passed, so there are no future executions
      assertEquals(0, result.nextExecutions.size)
    }
  }

  @Test
  fun testThrowsInvalidCronExpression() {
    val body = WebBackendDescribeCronExpressionRequestBody().cronExpression("invalid")
    assertThrows<CronValidationInvalidExpressionProblem> {
      webBackendCronExpressionHandler.describeCronExpression(body)
    }
  }
}
