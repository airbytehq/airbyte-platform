/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import datadog.trace.api.Trace
import io.airbyte.commons.server.handlers.AttemptHandler
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.config.Attempt
import io.airbyte.config.DestinationConnection
import io.airbyte.config.Job
import io.airbyte.config.SourceConnection
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.domain.services.llm.OpenAIChatCompletionService
import io.airbyte.domain.services.llm.OpenAIProjectId
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
class JobExplanationHandler(
  private val connectionService: ConnectionService,
  private val destinationService: DestinationService,
  private val destinationHandler: DestinationHandler,
  private val sourceService: SourceService,
  private val sourceHandler: SourceHandler,
  private val attemptHandler: AttemptHandler,
  private val jobHistoryHandler: JobHistoryHandler,
  private val openAIChatCompletionService: OpenAIChatCompletionService,
  @Named("jsonSecretsProcessorWithCopy") private val secretsProcessor: JsonSecretsProcessor,
) {
  private val systemPrompt =
    """
    You are an assistant to a user of the Airbyte Cloud webapp. The user is requesting your help while debugging a failed sync job. 
      
    Your job is to help provide short, simple steps that they can take as users of the Airbyte webapp to debug their failed job. 
    
    The user's goal is to understand why their sync failed and whether they can take steps to fix it themselves.
    
    When the user sends you debugging information, summarize how you understand the failure in one or two sentences.
    
    After the summary, provide the user with maximum three things the user could do to narrow down the issue or try to fix the sync. Format the three suggestions using bullet points.
    
    It is possible that the user cannot fix the sync themselves. If, based on the information you receive, the user has no good steps to fix the issue on their own, you may suggest they contact Airbyte support. 
    Recommend contacting support as a last resort, only if you cannot provide suggestions that are likely to fix the connection.
    
    The user cannot make any code changes to the Airbyte Platform or connectors. The user does not have access to any additional logs, and cannot enable further logging.
    
    Do not ask the user to engage with you any further. The user will only see your initial response.
    """.trimIndent()

  @Trace
  fun getJobExplanation(
    jobId: Long,
    workspaceId: UUID,
  ): String {
    val hydratedUserPrompt = hydratePromptTemplate(jobId)

    ApmTraceUtils.addTagsToTrace(
      mapOf(
        Pair(
          MetricTags.JOB_ID,
          jobId.toString(),
        ),
        Pair(
          MetricTags.WORKSPACE_ID,
          workspaceId.toString(),
        ),
        Pair(
          MetricTags.PROMPT_LENGTH,
          hydratedUserPrompt.length,
        ),
      ),
    )

    val response = openAIChatCompletionService.getChatResponse(OpenAIProjectId.FailedSyncAssistant, hydratedUserPrompt, systemPrompt)

    return response
  }

  private fun hydratePromptTemplate(jobId: Long): String {
    val job = jobHistoryHandler.getJob(jobId)

    val connection = connectionService.getStandardSync(UUID.fromString(job.scope))

    val source = sourceService.getSourceConnection(connection.sourceId)
    val sourceDefinition = sourceService.getStandardSourceDefinition(source.sourceDefinitionId)

    val destination = destinationService.getDestinationConnection(connection.destinationId)
    val destinationDefinition = destinationService.getStandardDestinationDefinition(destination.destinationDefinitionId)

    val finalAttempt = getFinalAttemptForJob(jobId)

    val failuresSummary = getFailuresSummaryAsText(finalAttempt)

    val hydratedUserPrompt =
      """
      The connection name is ${connection.name}
      
      The source connector is ${sourceDefinition.name}
      
      The current source connector configuration is:
      ${getSourceConfiguration(source)}
      
      ${getSourceConfigMayHaveChangedWarning(job, source)}
      
      The current destination connector is ${destinationDefinition.name}
      ${getDestinationConfiguration(destination)}
      
      ${getDestinationConfigMayHaveChangedwarning(job, destination)}
      
      The following failures were detected:
      
      $failuresSummary
      """.trimIndent()

    logger.debug { "Hydrated prompt: $hydratedUserPrompt" }

    return hydratedUserPrompt
  }

  private fun getFinalAttemptForJob(jobId: Long): Attempt {
    val info = jobHistoryHandler.getJobInfoWithoutLogs(jobId) ?: throw RuntimeException("Could not find job with id $jobId")
    return attemptHandler
      .getAttemptForJob(
        jobId,
        info.attempts.size - 1,
      )
  }

  private fun getSourceConfigMayHaveChangedWarning(
    job: Job,
    source: SourceConnection,
  ): String {
    if (source.updatedAt > job.createdAtInSecond) {
      return """
        The source was updated since this job ran, so the configuration may have changed.
        """.trimIndent()
    } else {
      return "The source has not been updated since this job ran, so the configuration when the job ran was the same as it is now."
    }
  }

  private fun getDestinationConfigMayHaveChangedwarning(
    job: Job,
    destination: DestinationConnection,
  ): String {
    if (destination.updatedAt > job.createdAtInSecond) {
      return """
        The destination was updated since this job ran, so the configuration may have changed.
        """.trimIndent()
    } else {
      return "The destination has not been updated since this job ran, so the configuration when the job ran was the same as it is now."
    }
  }

  private fun getFailuresSummaryAsText(attempt: Attempt): String =
    attempt.failureSummary
      ?.let { summary ->
        val failures = summary.failures
        if (failures.isNullOrEmpty()) {
          null
        } else {
          failures.joinToString("\n") { failure ->
            buildString {
              append("Failure ${failures.indexOf(failure) + 1}: \n")
              append("External error message: ${truncateLongString(failure.externalMessage)}\n")
              append("Internal error message: ${truncateLongString(failure.internalMessage)}\n")
              append("Stack trace: ${truncateLongString(failure.stacktrace)}\n")
              append("Origin: ${failure.failureOrigin}\n\n")
            }
          }
        }
      } ?: "No failures were stored with this job."

  private fun truncateLongString(string: String?): String {
    val limit = 5000
    val safeString = string ?: ""
    if (safeString.length > limit) {
      return "${safeString.take(limit)} ... [truncated at $limit characters]"
    } else {
      return safeString
    }
  }

  private fun getSourceConfiguration(source: SourceConnection): String {
    val sourceSpec = sourceHandler.getSourceVersionForWorkspaceId(source.sourceDefinitionId, source.workspaceId).spec
    val maskedSourceConfig = secretsProcessor.prepareSecretsForOutput(source.configuration, sourceSpec.connectionSpecification)

    return """
      The current source configuration is:
      
      $maskedSourceConfig
      """.trimIndent()
  }

  private fun getDestinationConfiguration(destination: DestinationConnection): String {
    val destinationSpec =
      destinationHandler
        .getDestinationVersionForDestinationId(
          destination.destinationDefinitionId,
          destination.workspaceId,
          destination.destinationId,
        ).spec
    val maskedDestinationConfig = secretsProcessor.prepareSecretsForOutput(destination.configuration, destinationSpec.connectionSpecification)

    return """
      The current destination configuration is:
      
      $maskedDestinationConfig
      """.trimIndent()
  }
}
