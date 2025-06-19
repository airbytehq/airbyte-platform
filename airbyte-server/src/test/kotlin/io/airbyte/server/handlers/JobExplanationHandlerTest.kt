/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.airbyte.api.model.generated.AttemptInfoRead
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.commons.server.handlers.AttemptHandler
import io.airbyte.commons.server.handlers.DestinationHandler
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.SourceHandler
import io.airbyte.config.Attempt
import io.airbyte.config.AttemptFailureSummary
import io.airbyte.config.AttemptStatus
import io.airbyte.config.DestinationConnection
import io.airbyte.config.FailureReason
import io.airbyte.config.Job
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.SourceConnection
import io.airbyte.config.StandardDestinationDefinition
import io.airbyte.config.StandardSourceDefinition
import io.airbyte.config.StandardSync
import io.airbyte.config.secrets.JsonSecretsProcessor
import io.airbyte.data.services.ConnectionService
import io.airbyte.data.services.DestinationService
import io.airbyte.data.services.SourceService
import io.airbyte.db.jdbc.JdbcUtils
import io.airbyte.domain.services.llm.OpenAIChatCompletionService
import io.airbyte.metrics.MetricClient
import io.airbyte.protocol.models.JsonSchemaType
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.Field
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class JobExplanationHandlerTest {
  private val connectionService = mockk<ConnectionService>()
  private val destinationService = mockk<DestinationService>()
  private val destinationHandler = mockk<DestinationHandler>()
  private val sourceService = mockk<SourceService>()
  private val sourceHandler = mockk<SourceHandler>()
  private val attemptHandler = mockk<AttemptHandler>()
  private val jobHistoryHandler = mockk<JobHistoryHandler>()
  private val openAIService = mockk<OpenAIChatCompletionService>()
  private val secretsProcessor = mockk<JsonSecretsProcessor>()
  private val metricClient = mockk<MetricClient>(relaxed = true)

  private lateinit var handler: JobExplanationHandler

  private val jobId = 42L
  private val connectionId = UUID.randomUUID()
  private val workspaceId = UUID.randomUUID()
  private val sourceId = UUID.randomUUID()
  private val destinationId = UUID.randomUUID()
  private val sourceDefinitionId = UUID.randomUUID()
  private val destinationDefinitionId = UUID.randomUUID()

  @BeforeEach
  fun setup() {
    handler =
      JobExplanationHandler(
        connectionService,
        destinationService,
        destinationHandler,
        sourceService,
        sourceHandler,
        attemptHandler,
        jobHistoryHandler,
        openAIService,
        secretsProcessor,
      )
  }

  @Test
  fun `getJobExplanation should return LLM response`() {
    val job = Job(1, ConfigType.SYNC, connectionId.toString(), null, null, null, null, 13, 37, true)

    val connection =
      StandardSync().also {
        it.name = "Test Connection"
        it.sourceId = sourceId
        it.destinationId = destinationId
      }

    val source =
      SourceConnection().also {
        it.sourceDefinitionId = sourceDefinitionId
        it.updatedAt = 500L
        it.configuration = jacksonObjectMapper().valueToTree(mapOf("sourceKey" to "sourceVal"))
        it.workspaceId = workspaceId
      }

    val sourceDef =
      StandardSourceDefinition().apply {
        name = "Hubspot"
      }

    val destination =
      DestinationConnection().also {
        it.destinationDefinitionId = destinationDefinitionId
        it.updatedAt = 500L
        it.configuration = jacksonObjectMapper().valueToTree(mapOf("destinationKey" to "destinationValue"))
        it.workspaceId = workspaceId
        it.destinationId = destinationId
      }

    val destinationDef =
      StandardDestinationDefinition().apply {
        name = "Bigquery"
      }

    val failureSummary =
      AttemptFailureSummary().withFailures(
        listOf(
          FailureReason()
            .withExternalMessage("external message")
            .withInternalMessage("internal message")
            .withStacktrace("stack trace")
            .withFailureOrigin(FailureReason.FailureOrigin.SOURCE),
        ),
      )
    val attempt =
      Attempt(0, 0, null, null, null, AttemptStatus.FAILED, null, failureSummary, 0, 0, 0L).apply {
      }

    val jobInfo =
      JobInfoRead().apply {
        attempts = listOf(AttemptInfoRead())
      }

    val spec =
      CatalogHelpers.fieldsToJsonSchema(
        Field.of(JdbcUtils.USERNAME_KEY, JsonSchemaType.STRING),
        Field.of(JdbcUtils.PASSWORD_KEY, JsonSchemaType.STRING),
      )
    val connectorSpecification = ConnectorSpecification().withConnectionSpecification(spec)

    every { jobHistoryHandler.getJob(jobId) } returns job
    every { connectionService.getStandardSync(connectionId) } returns connection
    every { sourceService.getSourceConnection(sourceId) } returns source
    every { sourceService.getStandardSourceDefinition(sourceDefinitionId) } returns sourceDef
    every { destinationService.getDestinationConnection(destinationId) } returns destination
    every { destinationService.getStandardDestinationDefinition(destinationDefinitionId) } returns destinationDef
    every { jobHistoryHandler.getJobInfoWithoutLogs(jobId) } returns jobInfo
    every { attemptHandler.getAttemptForJob(jobId, 0) } returns attempt
    every { sourceHandler.getSpecFromSourceDefinitionIdForWorkspace(sourceDefinitionId, workspaceId) } returns connectorSpecification
    every { destinationHandler.getSpecForDestinationId(destinationDefinitionId, workspaceId, destinationId) } returns connectorSpecification
    every { secretsProcessor.prepareSecretsForOutput(any(), any()) } answers { firstArg() }
    every { openAIService.getChatResponse(any(), any(), any()) } returns "This is a mocked LLM response"

    val result = handler.getJobExplanation(jobId, workspaceId)

    assert(result == "This is a mocked LLM response")

    verify {
      openAIService.getChatResponse(
        any(),
        match {
          it.contains("Test Connection")
          it.contains("The source connector is Hubspot")
          it.contains("{\"sourceKey\":\"sourceValue\"}")
          it.contains("The destination connector is Bigquery")
          it.contains("{\"destinationKey\":\"destinationValue\"}")
          it.contains("external message")
          it.contains("internal message")
          it.contains("stack trace")
          it.contains("Origin: source")
        },
        any(),
      )
    }
  }
}
