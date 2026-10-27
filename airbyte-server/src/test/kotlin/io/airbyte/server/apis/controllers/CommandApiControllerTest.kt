/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.problems.throwable.generated.BadRequestProblem
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.api.server.generated.models.CancelCommandRequest
import io.airbyte.api.server.generated.models.CancelCommandResponse
import io.airbyte.api.server.generated.models.CheckCommandOutputRequest
import io.airbyte.api.server.generated.models.CheckCommandOutputResponse
import io.airbyte.api.server.generated.models.CommandGetRequest
import io.airbyte.api.server.generated.models.CommandStatusRequest
import io.airbyte.api.server.generated.models.CommandStatusResponse
import io.airbyte.api.server.generated.models.DiscoverCommandOutputRequest
import io.airbyte.api.server.generated.models.DiscoverCommandOutputResponse
import io.airbyte.api.server.generated.models.FailureOrigin
import io.airbyte.api.server.generated.models.FailureType
import io.airbyte.api.server.generated.models.ReplicateCommandOutputRequest
import io.airbyte.api.server.generated.models.ReplicateCommandOutputResponse
import io.airbyte.api.server.generated.models.RunCheckCommandRequest
import io.airbyte.api.server.generated.models.RunCheckCommandResponse
import io.airbyte.api.server.generated.models.RunDiscoverCommandRequest
import io.airbyte.api.server.generated.models.RunReplicateCommandRequest
import io.airbyte.api.server.generated.models.RunSpecCommandRequest
import io.airbyte.api.server.generated.models.RunSpecCommandResponse
import io.airbyte.api.server.generated.models.SpecCommandOutputRequest
import io.airbyte.api.server.generated.models.SpecCommandOutputResponse
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.logging.LogEvent
import io.airbyte.commons.logging.LogEvents
import io.airbyte.commons.logging.LogSource
import io.airbyte.commons.server.converters.ApiPojoConverters
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.config.ActorCatalog
import io.airbyte.config.FailureReason
import io.airbyte.config.Metadata
import io.airbyte.config.ReplicationAttemptSummary
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.WorkloadPriority
import io.airbyte.domain.models.auth.AuthorizationResourceNotFoundException
import io.airbyte.domain.models.auth.WorkspaceAndOrganization
import io.airbyte.domain.services.auth.AuthorizationDataService
import io.airbyte.domain.services.auth.SavedExecutionAuthorizationDataService
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.ConnectorSpecification
import io.airbyte.protocol.models.v0.DestinationCatalog
import io.airbyte.protocol.models.v0.DestinationOperation
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.airbyte.server.auth.OperationPermissions
import io.airbyte.server.auth.ReadCommandRule
import io.airbyte.server.auth.RunDiscoverCommandRule
import io.airbyte.server.auth.RunReplicateCommandRule
import io.airbyte.server.auth.RunSpecCommandRule
import io.airbyte.server.services.CommandService
import io.airbyte.server.services.CommandStatus
import io.micronaut.security.authentication.Authentication
import io.mockk.Called
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID
import io.airbyte.api.server.generated.models.AirbyteCatalog as ApiAirbyteCatalog
import io.airbyte.api.server.generated.models.AirbyteStream as ApiAirbyteStream
import io.airbyte.api.server.generated.models.AirbyteStreamAndConfiguration as ApiAirbyteStreamAndConfiguration
import io.airbyte.api.server.generated.models.DestinationCatalog as ApiDestinationCatalog
import io.airbyte.api.server.generated.models.DestinationOperation as ApiDestinationOperation
import io.airbyte.api.server.generated.models.DestinationSyncMode as ApiDestinationSyncMode
import io.airbyte.api.server.generated.models.FailureReason as ApiFailureReason
import io.airbyte.api.server.generated.models.LogFormatType as ApiLogFormatType
import io.airbyte.api.server.generated.models.StreamDescriptor as ApiStreamDescriptor

class CommandApiControllerTest {
  private lateinit var controller: CommandApiController

  private lateinit var catalogConverter: CatalogConverter
  private lateinit var commandService: CommandService
  private lateinit var apiToPojoConverters: ApiPojoConverters

  companion object {
    const val TEST_COMMAND_ID = "my-command"
    const val TEST_JOB_ID = "my-test-job"
    const val TEST_ATTEMPT_NUMBER = 1L
    const val TEST_SIGNAL_INPUT = "my-signal"
    val TEST_ACTOR_ID: UUID = UUID.randomUUID()
    val TEST_ACTOR_DEFINITION_ID: UUID = UUID.randomUUID()
    val TEST_WORKSPACE_ID: UUID = UUID.randomUUID()
    val TEST_CONFIG: JsonNode = Jsons.deserialize("""{"json":"blob"}""")
  }

  @BeforeEach
  fun setup() {
    catalogConverter = mockk(relaxed = true)
    commandService = mockk(relaxed = true)
    apiToPojoConverters = ApiPojoConverters(mockk(relaxed = true))

    controller =
      CommandApiController(
        catalogConverter = catalogConverter,
        commandService = commandService,
        logUtils = mockk(relaxed = true),
        apiPojoConverters = apiToPojoConverters,
      )
  }

  @Test
  fun `cancel should call commandService cancel`() {
    val output = controller.cancelCommand(CancelCommandRequest(id = TEST_COMMAND_ID))
    assertEquals(CancelCommandResponse(id = TEST_COMMAND_ID), output)
    verify { commandService.cancel(TEST_COMMAND_ID) }
  }

  @Test
  fun `status should call commandService status`() {
    every { commandService.getStatus(TEST_COMMAND_ID) } returns CommandStatus.RUNNING

    val output = controller.getCommandStatus(CommandStatusRequest(id = TEST_COMMAND_ID))

    assertEquals(CommandStatusResponse(id = TEST_COMMAND_ID, status = CommandStatusResponse.Status.RUNNING), output)
    verify { commandService.getStatus(TEST_COMMAND_ID) }
  }

  @Test
  fun `getCheckCommandOutput can return a successful response`() {
    every { commandService.getCheckJobOutput(TEST_COMMAND_ID, any()) } returns
      CommandService.CheckJobOutput(
        status = StandardCheckConnectionOutput.Status.SUCCEEDED,
        connectorConfigUpdated = false,
        message = "",
        failureReason = null,
        logs = null,
      )

    val output = controller.getCheckCommandOutput(CheckCommandOutputRequest(id = TEST_COMMAND_ID))
    assertEquals(false, output.connectorConfigurationUpdated)
    assertEquals(TEST_COMMAND_ID, output.id)
    assertEquals(CheckCommandOutputResponse.Status.SUCCEEDED, output.status)
    assertEquals(null, output.logs)
    verify { commandService.getCheckJobOutput(TEST_COMMAND_ID, any()) }
  }

  @Test
  fun `getCheckCommandOutput can return a failure message`() {
    val message = "why things failed"
    every { commandService.getCheckJobOutput(TEST_COMMAND_ID, any()) } returns
      CommandService.CheckJobOutput(
        status = StandardCheckConnectionOutput.Status.FAILED,
        connectorConfigUpdated = false,
        message = message,
        failureReason = null,
        logs = null,
      )

    val output = controller.getCheckCommandOutput(CheckCommandOutputRequest(id = TEST_COMMAND_ID))
    assertEquals(
      CheckCommandOutputResponse(
        id = TEST_COMMAND_ID,
        connectorConfigurationUpdated = false,
        status = CheckCommandOutputResponse.Status.FAILED,
        message = message,
      ),
      output,
    )
    verify { commandService.getCheckJobOutput(TEST_COMMAND_ID, any()) }
  }

  @Test
  fun `getCheckCommandOutput can return a failure response`() {
    every { commandService.getCheckJobOutput(TEST_COMMAND_ID, any()) } returns
      CommandService.CheckJobOutput(
        status = StandardCheckConnectionOutput.Status.FAILED,
        connectorConfigUpdated = true,
        message = "",
        failureReason =
          FailureReason()
            .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
            .withFailureType(FailureReason.FailureType.CONFIG_ERROR)
            .withExternalMessage("external facing message")
            .withInternalMessage("internal facing message")
            .withStacktrace("my stacktrace")
            .withTimestamp(2)
            .withRetryable(false),
        logs = null,
      )

    val output = controller.getCheckCommandOutput(CheckCommandOutputRequest(id = TEST_COMMAND_ID))
    assertEquals(TEST_COMMAND_ID, output.id)
    assertEquals(true, output.connectorConfigurationUpdated)
    assertEquals(CheckCommandOutputResponse.Status.FAILED, output.status)
    assertEquals(
      ApiFailureReason(
        failureOrigin = FailureOrigin.SOURCE,
        failureType = FailureType.CONFIG_ERROR,
        externalMessage = "external facing message",
        internalMessage = "internal facing message",
        stacktrace = "my stacktrace",
        timestamp = 2,
        retryable = false,
      ),
      output.failureReason,
    )
    assertEquals(null, output.logs)
    verify { commandService.getCheckJobOutput(TEST_COMMAND_ID, any()) }
  }

  @Test
  fun `getDiscoverCommandOutput can return a successful response`() {
    val catalogId = UUID.randomUUID()
    val protocolCatalog = AirbyteCatalog().withStreams(listOf(AirbyteStream().withName("streamname")))
    val domainCatalog = ActorCatalog().withCatalog(Jsons.jsonNode(protocolCatalog))
    every { commandService.getDiscoverJobOutput(TEST_COMMAND_ID, any()) } returns
      CommandService.DiscoverJobOutput(
        catalogId = catalogId,
        catalog = domainCatalog,
        failureReason = null,
        logs = null,
        destinationCatalog = null,
      )

    val apiCatalog =
      ApiAirbyteCatalog(
        streams =
          listOf(
            ApiAirbyteStreamAndConfiguration(stream = ApiAirbyteStream(name = "streamname")),
          ),
      )
    every { catalogConverter.toApi(protocolCatalog, any()) } returns apiCatalog

    val output = controller.getDiscoverCommandOutput(DiscoverCommandOutputRequest(id = TEST_COMMAND_ID))
    assertEquals(
      DiscoverCommandOutputResponse(
        id = TEST_COMMAND_ID,
        catalogId = catalogId,
        catalog = apiCatalog,
        status = DiscoverCommandOutputResponse.Status.SUCCEEDED,
      ),
      output,
    )
    verify { commandService.getDiscoverJobOutput(TEST_COMMAND_ID, any()) }
  }

  @Test
  fun `getDiscoverCommandOutput can return a failure response`() {
    every { commandService.getDiscoverJobOutput(TEST_COMMAND_ID, any()) } returns
      CommandService.DiscoverJobOutput(
        catalogId = null,
        catalog = null,
        failureReason =
          FailureReason()
            .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
            .withFailureType(FailureReason.FailureType.CONFIG_ERROR)
            .withExternalMessage("external discover facing message")
            .withInternalMessage("internal discover facing message")
            .withStacktrace("my discover stacktrace")
            .withTimestamp(2)
            .withRetryable(false),
        logs = null,
        destinationCatalog = null,
      )

    val output = controller.getDiscoverCommandOutput(DiscoverCommandOutputRequest(id = TEST_COMMAND_ID))
    assertEquals(
      DiscoverCommandOutputResponse(
        id = TEST_COMMAND_ID,
        status = DiscoverCommandOutputResponse.Status.FAILED,
        failureReason =
          ApiFailureReason(
            failureOrigin = FailureOrigin.SOURCE,
            failureType = FailureType.CONFIG_ERROR,
            externalMessage = "external discover facing message",
            internalMessage = "internal discover facing message",
            stacktrace = "my discover stacktrace",
            timestamp = 2,
            retryable = false,
          ),
      ),
      output,
    )
    verify { commandService.getDiscoverJobOutput(TEST_COMMAND_ID, any()) }
  }

  @Test
  fun `getReplicateCommandOutput returns all the fields`() {
    val persistedReplicationOutput =
      ReplicationOutput()
        .withReplicationAttemptSummary(
          ReplicationAttemptSummary()
            .withStatus(StandardSyncSummary.ReplicationStatus.COMPLETED),
        ).withFailures(
          listOf(
            FailureReason().withFailureOrigin(FailureReason.FailureOrigin.SOURCE).withExternalMessage("Something to validate").withTimestamp(1),
          ),
        )
    every { commandService.getReplicationOutput(TEST_COMMAND_ID) } returns persistedReplicationOutput

    val output = controller.getReplicateCommandOutput(ReplicateCommandOutputRequest(id = TEST_COMMAND_ID))
    assertEquals(
      ReplicateCommandOutputResponse(
        id = TEST_COMMAND_ID,
        attemptSummary = persistedReplicationOutput.replicationAttemptSummary,
        failures =
          listOf(
            ApiFailureReason(failureOrigin = FailureOrigin.SOURCE, externalMessage = "Something to validate", timestamp = 1),
          ),
      ),
      output,
    )
    verify { commandService.getReplicationOutput(TEST_COMMAND_ID) }
  }

  @Test
  fun `FailureReason toApi returns all fields`() {
    val input =
      FailureReason()
        .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        .withFailureType(FailureReason.FailureType.CONFIG_ERROR)
        .withInternalMessage("example internal message")
        .withExternalMessage("example external message")
        .withMetadata(Metadata().withAdditionalProperty("from_trace_message", true))
        .withStacktrace("example stacktrace")
        .withRetryable(true)
        .withTimestamp(42)
        .withStreamDescriptor(StreamDescriptor().withName("example name").withNamespace("example namespace"))

    val output = apiToPojoConverters.failureReasonToApi(input)

    assertEquals(
      ApiFailureReason(
        failureOrigin = FailureOrigin.SOURCE,
        failureType = FailureType.CONFIG_ERROR,
        internalMessage = "example internal message",
        externalMessage = "example external message",
        fromTraceMessage = true,
        stacktrace = "example stacktrace",
        retryable = true,
        timestamp = 42,
        streamDescriptor = ApiStreamDescriptor(name = "example name", namespace = "example namespace"),
      ),
      output,
    )
  }

  @Test
  fun `FailureReason toApi handles null fields`() {
    assertEquals(ApiFailureReason(timestamp = 0), apiToPojoConverters.failureReasonToApi(FailureReason().withTimestamp(0)))
  }

  @Test
  fun `run check with an actor id`() {
    val request =
      RunCheckCommandRequest(id = TEST_COMMAND_ID, actorId = TEST_ACTOR_ID)
    val output = controller.runCheckCommand(request)
    assertEquals(RunCheckCommandResponse(id = TEST_COMMAND_ID), output)
    verify {
      commandService.createCheckCommand(
        commandId = TEST_COMMAND_ID,
        actorId = TEST_ACTOR_ID,
        jobId = null,
        attemptNumber = null,
        workloadPriority = WorkloadPriority.DEFAULT,
        signalInput = null,
        commandInput = any(),
      )
    }
  }

  @Test
  fun `run check with an actor id returns 200 if the command already existed`() {
    val request =
      RunCheckCommandRequest(id = TEST_COMMAND_ID, actorId = TEST_ACTOR_ID)
    val output = controller.runCheckCommand(request)
    assertEquals(RunCheckCommandResponse(id = TEST_COMMAND_ID), output)
    verify {
      commandService.createCheckCommand(
        commandId = TEST_COMMAND_ID,
        actorId = TEST_ACTOR_ID,
        jobId = null,
        attemptNumber = null,
        workloadPriority = WorkloadPriority.DEFAULT,
        signalInput = null,
        commandInput = any(),
      )
    }
  }

  @Test
  fun `run check with an actor id all relevant optional fields`() {
    val request =
      RunCheckCommandRequest(
        id = TEST_COMMAND_ID,
        actorId = TEST_ACTOR_ID,
        jobId = TEST_JOB_ID,
        attemptNumber = TEST_ATTEMPT_NUMBER.toInt(),
        priority = "high",
        signalInput = TEST_SIGNAL_INPUT,
      )
    val output = controller.runCheckCommand(request)
    assertEquals(RunCheckCommandResponse(id = TEST_COMMAND_ID), output)
    verify {
      commandService.createCheckCommand(
        commandId = TEST_COMMAND_ID,
        actorId = TEST_ACTOR_ID,
        jobId = TEST_JOB_ID,
        attemptNumber = TEST_ATTEMPT_NUMBER,
        workloadPriority = WorkloadPriority.HIGH,
        signalInput = TEST_SIGNAL_INPUT,
        commandInput = Jsons.jsonNode(request),
      )
    }
  }

  @Test
  fun `run check with an actor definition id and a config`() {
    val sanitizedConfig = Jsons.deserialize("""{"sanitized":"config"}""")
    val context = CommandService.CheckCommandContext(TEST_ACTOR_DEFINITION_ID, TEST_WORKSPACE_ID)

    every {
      commandService.getCheckCommandContext(
        actorId = null,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        workspaceId = TEST_WORKSPACE_ID,
      )
    } returns context
    every {
      commandService.processCheckConfig(
        actorId = null,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        workspaceId = TEST_WORKSPACE_ID,
        providedConfig = TEST_CONFIG,
      )
    } returns sanitizedConfig

    val request =
      RunCheckCommandRequest(
        id = TEST_COMMAND_ID,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        workspaceId = TEST_WORKSPACE_ID,
        config = TEST_CONFIG,
      )
    val output = controller.runCheckCommand(request)
    assertEquals(RunCheckCommandResponse(id = TEST_COMMAND_ID), output)

    val expectedCommandInput =
      RunCheckCommandRequest(
        id = TEST_COMMAND_ID,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        workspaceId = TEST_WORKSPACE_ID,
        config = sanitizedConfig,
      )
    verify {
      commandService.createCheckCommand(
        commandId = TEST_COMMAND_ID,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        workspaceId = TEST_WORKSPACE_ID,
        configuration = sanitizedConfig,
        workloadPriority = WorkloadPriority.DEFAULT,
        signalInput = null,
        commandInput = Jsons.jsonNode(expectedCommandInput),
      )
    }
  }

  @Test
  fun `run check with an actor definition id and a config and relevant optional fields`() {
    val sanitizedConfig = Jsons.deserialize("""{"sanitized":"config"}""")
    val context = CommandService.CheckCommandContext(TEST_ACTOR_DEFINITION_ID, TEST_WORKSPACE_ID)

    every {
      commandService.getCheckCommandContext(
        actorId = null,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        workspaceId = TEST_WORKSPACE_ID,
      )
    } returns context
    every {
      commandService.processCheckConfig(
        actorId = null,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        workspaceId = TEST_WORKSPACE_ID,
        providedConfig = TEST_CONFIG,
      )
    } returns sanitizedConfig

    val request =
      RunCheckCommandRequest(
        id = TEST_COMMAND_ID,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        workspaceId = TEST_WORKSPACE_ID,
        config = TEST_CONFIG,
        priority = "HIGH",
        signalInput = TEST_SIGNAL_INPUT,
      )
    val output = controller.runCheckCommand(request)

    assertEquals(RunCheckCommandResponse(id = TEST_COMMAND_ID), output)
    val expectedCommandInput =
      RunCheckCommandRequest(
        id = TEST_COMMAND_ID,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        workspaceId = TEST_WORKSPACE_ID,
        config = sanitizedConfig,
        priority = "HIGH",
        signalInput = TEST_SIGNAL_INPUT,
      )
    verify {
      commandService.createCheckCommand(
        commandId = TEST_COMMAND_ID,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        workspaceId = TEST_WORKSPACE_ID,
        configuration = sanitizedConfig,
        workloadPriority = WorkloadPriority.HIGH,
        signalInput = TEST_SIGNAL_INPUT,
        commandInput = Jsons.jsonNode(expectedCommandInput),
      )
    }
  }

  @Test
  fun `run check with actor id and custom config (hybrid mode)`() {
    val sanitizedConfig = Jsons.deserialize("""{"sanitized":"config"}""")
    val context = CommandService.CheckCommandContext(TEST_ACTOR_DEFINITION_ID, TEST_WORKSPACE_ID)

    every {
      commandService.getCheckCommandContext(
        actorId = TEST_ACTOR_ID,
        actorDefinitionId = null,
        workspaceId = null,
      )
    } returns context
    every {
      commandService.processCheckConfig(
        actorId = TEST_ACTOR_ID,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        workspaceId = TEST_WORKSPACE_ID,
        providedConfig = TEST_CONFIG,
      )
    } returns sanitizedConfig

    val request =
      RunCheckCommandRequest(id = TEST_COMMAND_ID, actorId = TEST_ACTOR_ID, config = TEST_CONFIG)
    val output = controller.runCheckCommand(request)
    assertEquals(RunCheckCommandResponse(id = TEST_COMMAND_ID), output)

    val expectedCommandInput =
      RunCheckCommandRequest(id = TEST_COMMAND_ID, actorId = TEST_ACTOR_ID, config = sanitizedConfig)
    verify {
      commandService.createCheckCommand(
        commandId = TEST_COMMAND_ID,
        actorId = TEST_ACTOR_ID,
        configuration = sanitizedConfig,
        workloadPriority = WorkloadPriority.DEFAULT,
        signalInput = null,
        commandInput = Jsons.jsonNode(expectedCommandInput),
      )
    }
  }

  @Test
  fun `discover authorization resolves the exact actor before command work`() {
    val data = mockk<AuthorizationDataService>()
    every { data.commandExistsInWorkspace(TEST_COMMAND_ID, WorkspaceAndOrganization(TEST_WORKSPACE_ID, UUID(0, 2))) } returns false
    val permissions = mockk<OperationPermissions>(relaxed = true)
    val rule = RunDiscoverCommandRule(data, permissions)
    val owner = WorkspaceAndOrganization(TEST_WORKSPACE_ID, UUID(0, 2))
    every { data.actorWorkspaceAndOrganization(TEST_ACTOR_ID) } returns owner
    rule.authorize(Authentication.build("caller"), rule.readInput(arrayOf(RunDiscoverCommandRequest(id = TEST_COMMAND_ID, actorId = TEST_ACTOR_ID))))
    verify(exactly = 1) { data.actorWorkspaceAndOrganization(TEST_ACTOR_ID) }
    verify { commandService wasNot Called }
  }

  @Test
  fun `command authorization uses metadata without reading command input`() {
    val data = mockk<SavedExecutionAuthorizationDataService>()
    val rule = ReadCommandRule(data, mockk(relaxed = true))
    every { data.command(TEST_COMMAND_ID) } returns WorkspaceAndOrganization(TEST_WORKSPACE_ID, UUID(0, 2))
    rule.authorize(Authentication.build("caller"), rule.readInput(arrayOf(CommandGetRequest(id = TEST_COMMAND_ID))))
    verify(exactly = 1) { data.command(TEST_COMMAND_ID) }
    verify { commandService wasNot Called }
  }

  @Test
  fun `replication authorization binds exact connection and saved job attempt`() {
    val connection = UUID.randomUUID()
    val data = mockk<AuthorizationDataService>()
    every { data.commandExistsInWorkspace(TEST_COMMAND_ID, WorkspaceAndOrganization(TEST_WORKSPACE_ID, UUID(0, 2))) } returns false
    val rule = RunReplicateCommandRule(data, mockk(relaxed = true))
    every { data.connectionWorkspaceAndOrganization(connection) } returns WorkspaceAndOrganization(TEST_WORKSPACE_ID, UUID(0, 2))
    every { data.savedAttemptConnectionId(17, 0) } returns connection
    rule.authorize(
      Authentication.build("caller"),
      rule.readInput(
        arrayOf(
          RunReplicateCommandRequest(id = TEST_COMMAND_ID, connectionId = connection, jobId = "17", attemptNumber = 0),
        ),
      ),
    )
    verify(exactly = 1) { data.connectionWorkspaceAndOrganization(connection) }
    verify(exactly = 1) { data.savedAttemptConnectionId(17, 0) }
    verify { commandService wasNot Called }
  }

  @Test
  fun `spec authorization resolves the actual saved workspace`() {
    val data = mockk<AuthorizationDataService>()
    every { data.commandExistsInWorkspace(TEST_COMMAND_ID, WorkspaceAndOrganization(TEST_WORKSPACE_ID, UUID(0, 2))) } returns false
    val rule = RunSpecCommandRule(data, mockk(relaxed = true))
    every { data.workspaceAndOrganization(TEST_WORKSPACE_ID) } returns WorkspaceAndOrganization(TEST_WORKSPACE_ID, UUID(0, 2))
    rule.authorize(
      Authentication.build("caller"),
      rule.readInput(
        arrayOf(
          RunSpecCommandRequest(id = TEST_COMMAND_ID, workspaceId = TEST_WORKSPACE_ID, dockerImageTag = "latest", dockerImage = "fixture/image"),
        ),
      ),
    )
    verify(exactly = 1) { data.workspaceAndOrganization(TEST_WORKSPACE_ID) }
    verify { commandService wasNot Called }
  }

  @Test
  fun `command authorization rejects unsupported input types before lookups`() {
    val data = mockk<SavedExecutionAuthorizationDataService>()
    val rule = ReadCommandRule(data, mockk())
    assertThrows<BadRequestProblem> { rule.readInput(arrayOf(UUID.randomUUID())) }
    verify { data wasNot Called }
    verify { commandService wasNot Called }
  }

  @Test
  fun `missing command ownership stays forbidden before permissions or command work`() {
    val data = mockk<SavedExecutionAuthorizationDataService>()
    val permissions = mockk<OperationPermissions>()
    val rule = ReadCommandRule(data, permissions)
    every { data.command(TEST_COMMAND_ID) } throws AuthorizationResourceNotFoundException()
    assertThrows<ForbiddenProblem> {
      rule.authorize(Authentication.build("caller"), rule.readInput(arrayOf(CommandGetRequest(id = TEST_COMMAND_ID))))
    }
    verify { permissions wasNot Called }
    verify { commandService wasNot Called }
  }

  @Test
  fun `getCheckCommandOutput returns structured logs when withLogs is true`() {
    val logEvents =
      LogEvents(
        events =
          listOf(
            LogEvent(
              timestamp = 1234567890L,
              message = "Test log message",
              level = "INFO",
              logSource = LogSource.PLATFORM,
            ),
          ),
        version = "1",
      )
    every { commandService.getCheckJobOutput(TEST_COMMAND_ID, withLogs = true) } returns
      CommandService.CheckJobOutput(
        status = StandardCheckConnectionOutput.Status.SUCCEEDED,
        connectorConfigUpdated = false,
        message = "",
        failureReason = null,
        logs = CommandService.JobLogs.createStructuredLogs(logEvents),
      )

    val output = controller.getCheckCommandOutput(CheckCommandOutputRequest(id = TEST_COMMAND_ID, withLogs = true))

    assertEquals(CheckCommandOutputResponse.Status.SUCCEEDED, output.status)
    assertEquals(ApiLogFormatType.STRUCTURED, output.logs?.logType)
    assertEquals(
      1,
      output.logs
        ?.logEvents
        ?.events
        ?.size,
    )
    assertEquals("1", output.logs?.logEvents?.version)
    verify { commandService.getCheckJobOutput(TEST_COMMAND_ID, withLogs = true) }
  }

  @Test
  fun `getCheckCommandOutput returns formatted logs when withLogs is true and logs are formatted`() {
    val logLines = listOf("line 1", "line 2", "line 3")
    every { commandService.getCheckJobOutput(TEST_COMMAND_ID, withLogs = true) } returns
      CommandService.CheckJobOutput(
        status = StandardCheckConnectionOutput.Status.SUCCEEDED,
        connectorConfigUpdated = false,
        message = "",
        failureReason = null,
        logs = CommandService.JobLogs.createFormattedLogs(logLines),
      )

    val output = controller.getCheckCommandOutput(CheckCommandOutputRequest(id = TEST_COMMAND_ID, withLogs = true))

    assertEquals(CheckCommandOutputResponse.Status.SUCCEEDED, output.status)
    assertEquals(ApiLogFormatType.FORMATTED, output.logs?.logType)
    assertEquals(3, output.logs?.logLines?.size)
    assertEquals("line 1", output.logs?.logLines?.get(0))
    verify { commandService.getCheckJobOutput(TEST_COMMAND_ID, withLogs = true) }
  }

  @Test
  fun `getCheckCommandOutput returns no logs when withLogs is false`() {
    every { commandService.getCheckJobOutput(TEST_COMMAND_ID, withLogs = false) } returns
      CommandService.CheckJobOutput(
        status = StandardCheckConnectionOutput.Status.SUCCEEDED,
        connectorConfigUpdated = false,
        message = "",
        failureReason = null,
        logs = null,
      )

    val output = controller.getCheckCommandOutput(CheckCommandOutputRequest(id = TEST_COMMAND_ID, withLogs = false))

    assertEquals(CheckCommandOutputResponse.Status.SUCCEEDED, output.status)
    assertEquals(null, output.logs)
    verify { commandService.getCheckJobOutput(TEST_COMMAND_ID, withLogs = false) }
  }

  @Test
  fun `getDiscoverCommandOutput returns structured logs when withLogs is true`() {
    val catalogId = UUID.randomUUID()
    val protocolCatalog = AirbyteCatalog().withStreams(listOf(AirbyteStream().withName("streamname")))
    val domainCatalog = ActorCatalog().withCatalog(Jsons.jsonNode(protocolCatalog))
    val logEvents =
      LogEvents(
        events =
          listOf(
            LogEvent(
              timestamp = 1234567890L,
              message = "Discover log message",
              level = "INFO",
              logSource = LogSource.PLATFORM,
            ),
          ),
        version = "1",
      )
    every { commandService.getDiscoverJobOutput(TEST_COMMAND_ID, withLogs = true) } returns
      CommandService.DiscoverJobOutput(
        catalogId = catalogId,
        catalog = domainCatalog,
        failureReason = null,
        logs = CommandService.JobLogs.createStructuredLogs(logEvents),
        destinationCatalog = null,
      )

    val apiCatalog =
      ApiAirbyteCatalog(
        streams =
          listOf(
            ApiAirbyteStreamAndConfiguration(stream = ApiAirbyteStream(name = "streamname")),
          ),
      )
    every { catalogConverter.toApi(protocolCatalog, any()) } returns apiCatalog

    val output = controller.getDiscoverCommandOutput(DiscoverCommandOutputRequest(id = TEST_COMMAND_ID, withLogs = true))

    assertEquals(DiscoverCommandOutputResponse.Status.SUCCEEDED, output.status)
    assertEquals(ApiLogFormatType.STRUCTURED, output.logs?.logType)
    assertEquals(
      1,
      output.logs
        ?.logEvents
        ?.events
        ?.size,
    )
    assertEquals("1", output.logs?.logEvents?.version)
    verify { commandService.getDiscoverJobOutput(TEST_COMMAND_ID, withLogs = true) }
  }

  @Test
  fun `getDiscoverCommandOutput returns formatted logs when withLogs is true and logs are formatted`() {
    val catalogId = UUID.randomUUID()
    val protocolCatalog = AirbyteCatalog().withStreams(listOf(AirbyteStream().withName("streamname")))
    val domainCatalog = ActorCatalog().withCatalog(Jsons.jsonNode(protocolCatalog))
    val logLines = listOf("discover line 1", "discover line 2")
    every { commandService.getDiscoverJobOutput(TEST_COMMAND_ID, withLogs = true) } returns
      CommandService.DiscoverJobOutput(
        catalogId = catalogId,
        catalog = domainCatalog,
        failureReason = null,
        logs = CommandService.JobLogs.createFormattedLogs(logLines),
        destinationCatalog = null,
      )

    val apiCatalog =
      ApiAirbyteCatalog(
        streams =
          listOf(
            ApiAirbyteStreamAndConfiguration(stream = ApiAirbyteStream(name = "streamname")),
          ),
      )
    every { catalogConverter.toApi(protocolCatalog, any()) } returns apiCatalog

    val output = controller.getDiscoverCommandOutput(DiscoverCommandOutputRequest(id = TEST_COMMAND_ID, withLogs = true))

    assertEquals(DiscoverCommandOutputResponse.Status.SUCCEEDED, output.status)
    assertEquals(ApiLogFormatType.FORMATTED, output.logs?.logType)
    assertEquals(2, output.logs?.logLines?.size)
    assertEquals("discover line 1", output.logs?.logLines?.get(0))
    verify { commandService.getDiscoverJobOutput(TEST_COMMAND_ID, withLogs = true) }
  }

  @Test
  fun `getDiscoverCommandOutput returns no logs when withLogs is false`() {
    val catalogId = UUID.randomUUID()
    val protocolCatalog = AirbyteCatalog().withStreams(listOf(AirbyteStream().withName("streamname")))
    val domainCatalog = ActorCatalog().withCatalog(Jsons.jsonNode(protocolCatalog))
    every { commandService.getDiscoverJobOutput(TEST_COMMAND_ID, withLogs = false) } returns
      CommandService.DiscoverJobOutput(
        catalogId = catalogId,
        catalog = domainCatalog,
        failureReason = null,
        logs = null,
        destinationCatalog = null,
      )

    val apiCatalog =
      ApiAirbyteCatalog(
        streams =
          listOf(
            ApiAirbyteStreamAndConfiguration(stream = ApiAirbyteStream(name = "streamname")),
          ),
      )
    every { catalogConverter.toApi(protocolCatalog, any()) } returns apiCatalog

    val output = controller.getDiscoverCommandOutput(DiscoverCommandOutputRequest(id = TEST_COMMAND_ID, withLogs = false))

    assertEquals(DiscoverCommandOutputResponse.Status.SUCCEEDED, output.status)
    assertEquals(null, output.logs)
    verify { commandService.getDiscoverJobOutput(TEST_COMMAND_ID, withLogs = false) }
  }

  @Test
  fun `run spec with docker image and tag`() {
    val dockerImage = "airbyte/source-test"
    val dockerImageTag = "1.0.0"
    val request =
      RunSpecCommandRequest(
        id = TEST_COMMAND_ID,
        workspaceId = TEST_WORKSPACE_ID,
        dockerImage = dockerImage,
        dockerImageTag = dockerImageTag,
      )

    val output = controller.runSpecCommand(request)

    assertEquals(RunSpecCommandResponse(id = TEST_COMMAND_ID), output)
    verify {
      commandService.createSpecCommand(
        commandId = TEST_COMMAND_ID,
        dockerImage = dockerImage,
        dockerImageTag = dockerImageTag,
        workspaceId = TEST_WORKSPACE_ID,
        signalInput = null,
        commandInput = Jsons.jsonNode(request),
      )
    }
  }

  @Test
  fun `run spec with docker image, tag and signal input`() {
    val dockerImage = "airbyte/source-postgres"
    val dockerImageTag = "2.0.0"
    val request =
      RunSpecCommandRequest(
        id = TEST_COMMAND_ID,
        workspaceId = TEST_WORKSPACE_ID,
        dockerImage = dockerImage,
        dockerImageTag = dockerImageTag,
        signalInput = TEST_SIGNAL_INPUT,
      )

    val output = controller.runSpecCommand(request)

    assertEquals(RunSpecCommandResponse(id = TEST_COMMAND_ID), output)
    verify {
      commandService.createSpecCommand(
        commandId = TEST_COMMAND_ID,
        dockerImage = dockerImage,
        dockerImageTag = dockerImageTag,
        workspaceId = TEST_WORKSPACE_ID,
        signalInput = TEST_SIGNAL_INPUT,
        commandInput = Jsons.jsonNode(request),
      )
    }
  }

  @Test
  fun `run spec with actor definition id`() {
    val dockerImageTag = "1.5.0"
    val request =
      RunSpecCommandRequest(
        id = TEST_COMMAND_ID,
        workspaceId = TEST_WORKSPACE_ID,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        dockerImageTag = dockerImageTag,
      )

    val output = controller.runSpecCommand(request)

    assertEquals(RunSpecCommandResponse(id = TEST_COMMAND_ID), output)
    verify {
      commandService.createSpecCommand(
        commandId = TEST_COMMAND_ID,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        dockerImageTag = dockerImageTag,
        workspaceId = TEST_WORKSPACE_ID,
        signalInput = null,
        commandInput = Jsons.jsonNode(request),
      )
    }
  }

  @Test
  fun `run spec with actor definition id and signal input`() {
    val dockerImageTag = "3.0.0"
    val request =
      RunSpecCommandRequest(
        id = TEST_COMMAND_ID,
        workspaceId = TEST_WORKSPACE_ID,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        dockerImageTag = dockerImageTag,
        signalInput = TEST_SIGNAL_INPUT,
      )

    val output = controller.runSpecCommand(request)

    assertEquals(RunSpecCommandResponse(id = TEST_COMMAND_ID), output)
    verify {
      commandService.createSpecCommand(
        commandId = TEST_COMMAND_ID,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        dockerImageTag = dockerImageTag,
        workspaceId = TEST_WORKSPACE_ID,
        signalInput = TEST_SIGNAL_INPUT,
        commandInput = Jsons.jsonNode(request),
      )
    }
  }

  @Test
  fun `run spec throws exception when neither docker image nor actor definition id provided`() {
    val request =
      RunSpecCommandRequest(
        id = TEST_COMMAND_ID,
        workspaceId = TEST_WORKSPACE_ID,
        dockerImageTag = "1.0.0",
      )

    val exception =
      assertThrows<IllegalArgumentException> {
        controller.runSpecCommand(request)
      }

    assertEquals("docker_image is required when actor_definition_id is not provided", exception.message)
    verify { commandService wasNot Called }
  }

  @Test
  fun `run spec prioritizes actor definition id when both are provided`() {
    val dockerImage = "airbyte/source-test"
    val dockerImageTag = "1.0.0"
    val request =
      RunSpecCommandRequest(
        id = TEST_COMMAND_ID,
        workspaceId = TEST_WORKSPACE_ID,
        dockerImage = dockerImage,
        dockerImageTag = dockerImageTag,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
      )

    val output = controller.runSpecCommand(request)

    assertEquals(RunSpecCommandResponse(id = TEST_COMMAND_ID), output)
    // Should use actor definition id path, not docker image path
    verify {
      commandService.createSpecCommand(
        commandId = TEST_COMMAND_ID,
        actorDefinitionId = TEST_ACTOR_DEFINITION_ID,
        dockerImageTag = dockerImageTag,
        workspaceId = TEST_WORKSPACE_ID,
        signalInput = null,
        commandInput = Jsons.jsonNode(request),
      )
    }
  }

  @Test
  fun `getSpecCommandOutput can return a successful response`() {
    val spec =
      ConnectorSpecification()
        .withConnectionSpecification(Jsons.deserialize("""{"type":"object","properties":{}}"""))
    every { commandService.getSpecJobOutput(commandId = TEST_COMMAND_ID, withLogs = false) } returns
      CommandService.SpecJobOutput(
        spec = spec,
        failureReason = null,
        logs = null,
      )

    val output = controller.getSpecCommandOutput(SpecCommandOutputRequest(id = TEST_COMMAND_ID))

    assertEquals(TEST_COMMAND_ID, output.id)
    assertEquals(SpecCommandOutputResponse.Status.SUCCEEDED, output.status)
    assertEquals(Jsons.jsonNode(spec), output.spec)
    assertEquals(null, output.failureReason)
    assertEquals(null, output.logs)
    verify { commandService.getSpecJobOutput(commandId = TEST_COMMAND_ID, withLogs = false) }
  }

  @Test
  fun `getSpecCommandOutput can return a failure response`() {
    every { commandService.getSpecJobOutput(commandId = TEST_COMMAND_ID, withLogs = false) } returns
      CommandService.SpecJobOutput(
        spec = null,
        failureReason =
          FailureReason()
            .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
            .withFailureType(FailureReason.FailureType.SYSTEM_ERROR)
            .withExternalMessage("external spec facing message")
            .withInternalMessage("internal spec facing message")
            .withStacktrace("my spec stacktrace")
            .withTimestamp(3)
            .withRetryable(false),
        logs = null,
      )

    val output = controller.getSpecCommandOutput(SpecCommandOutputRequest(id = TEST_COMMAND_ID))

    assertEquals(TEST_COMMAND_ID, output.id)
    assertEquals(SpecCommandOutputResponse.Status.FAILED, output.status)
    assertEquals(null, output.spec)
    assertEquals(
      ApiFailureReason(
        failureOrigin = FailureOrigin.SOURCE,
        failureType = FailureType.SYSTEM_ERROR,
        externalMessage = "external spec facing message",
        internalMessage = "internal spec facing message",
        stacktrace = "my spec stacktrace",
        timestamp = 3,
        retryable = false,
      ),
      output.failureReason,
    )
    verify { commandService.getSpecJobOutput(commandId = TEST_COMMAND_ID, withLogs = false) }
  }

  @Test
  fun `getSpecCommandOutput returns structured logs when withLogs is true`() {
    val spec =
      ConnectorSpecification()
        .withConnectionSpecification(Jsons.deserialize("""{"type":"object"}"""))
    val logEvents =
      LogEvents(
        events =
          listOf(
            LogEvent(
              timestamp = 1234567890L,
              message = "Spec log message",
              level = "INFO",
              logSource = LogSource.PLATFORM,
            ),
          ),
        version = "1",
      )
    every { commandService.getSpecJobOutput(commandId = TEST_COMMAND_ID, withLogs = true) } returns
      CommandService.SpecJobOutput(
        spec = spec,
        failureReason = null,
        logs = CommandService.JobLogs.createStructuredLogs(logEvents),
      )

    val output = controller.getSpecCommandOutput(SpecCommandOutputRequest(id = TEST_COMMAND_ID, withLogs = true))

    assertEquals(TEST_COMMAND_ID, output.id)
    assertEquals(SpecCommandOutputResponse.Status.SUCCEEDED, output.status)
    assertEquals(Jsons.jsonNode(spec), output.spec)
    assertEquals(ApiLogFormatType.STRUCTURED, output.logs?.logType)
    assertEquals(
      1,
      output.logs
        ?.logEvents
        ?.events
        ?.size,
    )
    assertEquals("1", output.logs?.logEvents?.version)
    verify { commandService.getSpecJobOutput(commandId = TEST_COMMAND_ID, withLogs = true) }
  }

  @Test
  fun `getSpecCommandOutput returns formatted logs when withLogs is true and logs are formatted`() {
    val spec =
      ConnectorSpecification()
        .withConnectionSpecification(Jsons.deserialize("""{"type":"object"}"""))
    val logLines = listOf("spec line 1", "spec line 2")
    every { commandService.getSpecJobOutput(commandId = TEST_COMMAND_ID, withLogs = true) } returns
      CommandService.SpecJobOutput(
        spec = spec,
        failureReason = null,
        logs = CommandService.JobLogs.createFormattedLogs(logLines),
      )

    val output = controller.getSpecCommandOutput(SpecCommandOutputRequest(id = TEST_COMMAND_ID, withLogs = true))

    assertEquals(TEST_COMMAND_ID, output.id)
    assertEquals(SpecCommandOutputResponse.Status.SUCCEEDED, output.status)
    assertEquals(Jsons.jsonNode(spec), output.spec)
    assertEquals(ApiLogFormatType.FORMATTED, output.logs?.logType)
    assertEquals(2, output.logs?.logLines?.size)
    assertEquals("spec line 1", output.logs?.logLines?.get(0))
    verify { commandService.getSpecJobOutput(commandId = TEST_COMMAND_ID, withLogs = true) }
  }

  @Test
  fun `getSpecCommandOutput returns no logs when withLogs is false`() {
    val spec =
      ConnectorSpecification()
        .withConnectionSpecification(Jsons.deserialize("""{"type":"object"}"""))
    every { commandService.getSpecJobOutput(commandId = TEST_COMMAND_ID, withLogs = false) } returns
      CommandService.SpecJobOutput(
        spec = spec,
        failureReason = null,
        logs = null,
      )

    val output = controller.getSpecCommandOutput(SpecCommandOutputRequest(id = TEST_COMMAND_ID, withLogs = false))

    assertEquals(TEST_COMMAND_ID, output.id)
    assertEquals(SpecCommandOutputResponse.Status.SUCCEEDED, output.status)
    assertEquals(Jsons.jsonNode(spec), output.spec)
    assertEquals(null, output.logs)
    verify { commandService.getSpecJobOutput(commandId = TEST_COMMAND_ID, withLogs = false) }
  }

  fun `getDiscoverCommandOutput converts source catalog to API format`() {
    val catalogId = UUID.randomUUID()
    val protocolCatalog = AirbyteCatalog().withStreams(listOf(AirbyteStream().withName("source_stream")))
    val domainCatalog =
      ActorCatalog()
        .withCatalogType(ActorCatalog.CatalogType.SOURCE_CATALOG)
        .withCatalog(Jsons.jsonNode(protocolCatalog))
    every { commandService.getDiscoverJobOutput(TEST_COMMAND_ID, any()) } returns
      CommandService.DiscoverJobOutput(
        catalogId = catalogId,
        catalog = domainCatalog,
        destinationCatalog = null,
        failureReason = null,
        logs = null,
      )
    val apiCatalog =
      ApiAirbyteCatalog(
        streams =
          listOf(
            ApiAirbyteStreamAndConfiguration(stream = ApiAirbyteStream(name = "source_stream")),
          ),
      )
    every { catalogConverter.toApi(protocolCatalog, any()) } returns apiCatalog

    val output = controller.getDiscoverCommandOutput(DiscoverCommandOutputRequest(id = TEST_COMMAND_ID))

    assertEquals(catalogId, output.catalogId)
    assertEquals(apiCatalog, output.catalog)
    assertEquals(null, output.destinationCatalog)
    assertEquals(DiscoverCommandOutputResponse.Status.SUCCEEDED, output.status)
    verify { catalogConverter.toApi(protocolCatalog, any()) }
  }

  @Test
  fun `getDiscoverCommandOutput converts destination catalog to API format`() {
    val catalogId = UUID.randomUUID()
    val jsonSchema = Jsons.deserialize("""{"type": "object", "properties": {"id": {"type": "string"}}}""")
    val protocolDestinationCatalog =
      DestinationCatalog().withOperations(
        listOf(
          DestinationOperation()
            .withObjectName("destination_table")
            .withSyncMode(DestinationSyncMode.APPEND)
            .withJsonSchema(jsonSchema),
        ),
      )
    val domainDestinationCatalog =
      ActorCatalog()
        .withCatalogType(ActorCatalog.CatalogType.DESTINATION_CATALOG)
        .withCatalog(Jsons.jsonNode(protocolDestinationCatalog))
    every { commandService.getDiscoverJobOutput(TEST_COMMAND_ID, any()) } returns
      CommandService.DiscoverJobOutput(
        catalogId = catalogId,
        catalog = null,
        destinationCatalog = domainDestinationCatalog,
        failureReason = null,
        logs = null,
      )

    val apiDestinationCatalog =
      ApiDestinationCatalog(
        operations =
          listOf(
            ApiDestinationOperation(objectName = "destination_table", syncMode = ApiDestinationSyncMode.APPEND, schema = jsonSchema),
          ),
      )

    val output = controller.getDiscoverCommandOutput(DiscoverCommandOutputRequest(id = TEST_COMMAND_ID))

    assertEquals(catalogId, output.catalogId)
    assertEquals(null, output.catalog)
    assertEquals(apiDestinationCatalog, output.destinationCatalog)
    assertEquals(DiscoverCommandOutputResponse.Status.SUCCEEDED, output.status)
  }

  @Test
  fun `getDiscoverCommandOutput handles null catalogs`() {
    every { commandService.getDiscoverJobOutput(TEST_COMMAND_ID, any()) } returns
      CommandService.DiscoverJobOutput(
        catalogId = null,
        catalog = null,
        destinationCatalog = null,
        failureReason = null,
        logs = null,
      )

    val output = controller.getDiscoverCommandOutput(DiscoverCommandOutputRequest(id = TEST_COMMAND_ID))

    assertEquals(null, output.catalogId)
    assertEquals(null, output.catalog)
    assertEquals(null, output.destinationCatalog)
    assertEquals(DiscoverCommandOutputResponse.Status.SUCCEEDED, output.status)
  }

  @Test
  fun `getCheckCommandOutput returns status and command ID`() {
    every { commandService.getCheckJobOutput(TEST_COMMAND_ID, any()) } returns
      CommandService.CheckJobOutput(
        status = StandardCheckConnectionOutput.Status.SUCCEEDED,
        connectorConfigUpdated = false,
        message = "",
        failureReason = null,
        logs = null,
      )

    val output = controller.getCheckCommandOutput(CheckCommandOutputRequest(id = TEST_COMMAND_ID))

    assertEquals(CheckCommandOutputResponse.Status.SUCCEEDED, output.status)
    assertEquals(TEST_COMMAND_ID, output.id)
    verify { commandService.getCheckJobOutput(TEST_COMMAND_ID, any()) }
  }

  @Test
  fun `runCheckCommand returns command ID`() {
    val request =
      RunCheckCommandRequest(id = TEST_COMMAND_ID, actorId = TEST_ACTOR_ID)

    val output = controller.runCheckCommand(request)

    assertEquals(RunCheckCommandResponse(id = TEST_COMMAND_ID), output)
    verify {
      commandService.createCheckCommand(
        commandId = TEST_COMMAND_ID,
        actorId = TEST_ACTOR_ID,
        jobId = null,
        attemptNumber = null,
        workloadPriority = WorkloadPriority.DEFAULT,
        signalInput = null,
        commandInput = any(),
      )
    }
  }
}
