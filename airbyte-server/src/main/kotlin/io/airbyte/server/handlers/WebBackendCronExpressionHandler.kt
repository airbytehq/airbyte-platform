package io.airbyte.server.handlers

import com.cronutils.descriptor.CronDescriptor
import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import io.airbyte.api.model.generated.WebBackendCronExpressionDescription
import io.airbyte.api.model.generated.WebBackendDescribeCronExpressionRequestBody
import io.airbyte.api.problems.model.generated.ProblemCronExpressionData
import io.airbyte.api.problems.throwable.generated.CronValidationInvalidExpressionProblem
import jakarta.inject.Singleton
import java.time.ZonedDateTime
import java.util.Locale

@Singleton
class WebBackendCronExpressionHandler {
  fun describeCronExpression(body: WebBackendDescribeCronExpressionRequestBody): WebBackendCronExpressionDescription? {
    val cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ)

    try {
      val cron = CronParser(cronDefinition).parse(body.cronExpression)
      cron.validate()

      val description = CronDescriptor.instance(Locale.ENGLISH).describe(cron)

      val executionTime = ExecutionTime.forCron(cron)
      val nextExecutions = mutableListOf<Long>()
      var nextExecution = ZonedDateTime.now()

      for (i in 1..3) {
        nextExecution = executionTime.nextExecution(nextExecution).orElse(null) ?: break
        nextExecutions.add(nextExecution.toEpochSecond())
      }

      return WebBackendCronExpressionDescription()
        .cronExpression(body.cronExpression)
        .description(description)
        .nextExecutions(nextExecutions)
    } catch (e: IllegalArgumentException) {
      throw CronValidationInvalidExpressionProblem(ProblemCronExpressionData().cronExpression(body.cronExpression).validationErrorMessage(e.message))
    }
  }
}
