/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import io.airbyte.api.model.generated.WebBackendCronExpressionDescription
import io.airbyte.api.model.generated.WebBackendDescribeCronExpressionRequestBody
import io.airbyte.api.problems.model.generated.ProblemCronExpressionData
import io.airbyte.api.problems.throwable.generated.CronValidationInvalidExpressionProblem
import io.airbyte.config.helpers.CronExpressionHelper
import jakarta.inject.Singleton

@Singleton
open class WebBackendCronExpressionHandler(
  private val cronExpressionHelper: CronExpressionHelper,
) {
  fun describeCronExpression(body: WebBackendDescribeCronExpressionRequestBody): WebBackendCronExpressionDescription? {
    try {
      val cron = cronExpressionHelper.validateCronExpression(body.cronExpression)

      return WebBackendCronExpressionDescription()
        .cronExpression(body.cronExpression)
        .description(cronExpressionHelper.describeCronExpression(cron))
        .nextExecutions(cronExpressionHelper.getNextExecutions(cron, 3))
    } catch (e: IllegalArgumentException) {
      throw CronValidationInvalidExpressionProblem(
        ProblemCronExpressionData()
          .cronExpression(body.cronExpression)
          .validationErrorMessage(e.message),
      )
    }
  }
}
