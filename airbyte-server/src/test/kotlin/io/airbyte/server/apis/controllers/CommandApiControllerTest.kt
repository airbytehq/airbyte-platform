/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.controllers

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.api.model.generated.CancelCommandRequest
import io.airbyte.api.model.generated.CancelCommandResponse
import io.airbyte.api.model.generated.CheckCommandOutputRequest
import io.airbyte.api.model.generated.CheckCommandOutputResponse
import io.airbyte.api.model.generated.CommandStatusRequest
import io.airbyte.api.model.generated.CommandStatusResponse
import io.airbyte.api.model.generated.DiscoverCommandOutputRequest
import io.airbyte.api.model.generated.DiscoverCommandOutputResponse
import io.airbyte.api.model.generated.FailureOrigin
import io.airbyte.api.model.generated.FailureType
import io.airbyte.api.model.generated.ReplicateCommandOutputRequest
import io.airbyte.api.model.generated.ReplicateCommandOutputResponse
import io.airbyte.api.model.generated.RunCheckCommandRequest
import io.airbyte.api.model.generated.RunCheckCommandResponse
import io.airbyte.api.problems.throwable.generated.ForbiddenProblem
import io.airbyte.commons.auth.roles.AuthRoleConstants
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.authorization.RoleResolver
import io.airbyte.commons.server.handlers.helpers.CatalogConverter
import io.airbyte.commons.server.helpers.SecretSanitizer
import io.airbyte.config.ActorCatalog
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.Metadata
import io.airbyte.config.ReplicationAttemptSummary
import io.airbyte.config.ReplicationOutput
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.WorkloadPriority
import io.airbyte.data.repositories.ActorRepository
import io.airbyte.data.services.WorkspaceService
import io.airbyte.domain.models.ActorId
import io.airbyte.domain.models.CommandId
import io.airbyte.domain.models.ConnectionId
import io.airbyte.domain.models.WorkspaceId
import io.airbyte.protocol.models.v0.AirbyteCatalog
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.server.services.CommandService
import io.airbyte.server.services.CommandStatus
import io.mockk.Called
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID
import io.airbyte.api.model.generated.FailureReason as ApiFailureReason
import io.airbyte.api.model.generated.StreamDescriptor as ApiStreamDescriptor

class CommandApiControllerTest {
  private lateinit var controller: CommandApiController

  private lateinit var actorRepository: ActorRepository
  private lateinit var catalogConverter: CatalogConverter
  private lateinit var commandService: CommandService
  private lateinit var roleResolver: RoleResolver
  private lateinit var secretSanitizer: SecretSanitizer
  private lateinit var workspaceService: WorkspaceService

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
    roleResolver = mockk(relaxed = true)
    mockkConstructor(RoleResolver.Request::class)
    every { anyConstructed<RoleResolver.Request>().withCurrentUser() } returns
      mockk {
        every { withRef(any(), any<UUID>()) } returns this
        every { requireRole(any()) } returns Unit
      }

    actorRepository = mockk(relaxed = true)
    catalogConverter = mockk(relaxed = true)
    commandService = mockk(relaxed = true)
    secretSanitizer = mockk(relaxed = true)
    workspaceService = mockk(relaxed = true)

    controller =
      CommandApiController(
        roleResolver = roleResolver,
        actorRepository = actorRepository,
        catalogConverter = catalogConverter,
        commandService = commandService,
        secretSanitizer = secretSanitizer,
        workspaceService = workspaceService,
      )
  }

  @Test
  fun `cancel should call commandService cancel`() {
    val output = controller.cancelCommand(CancelCommandRequest().id(TEST_COMMAND_ID))
    assertEquals(CancelCommandResponse().id(TEST_COMMAND_ID), output)
    verify { commandService.cancel(TEST_COMMAND_ID) }
  }

  @Test
  fun `status should call commandService status`() {
    every { commandService.getStatus(TEST_COMMAND_ID) } returns CommandStatus.RUNNING

    val output = controller.getCommandStatus(CommandStatusRequest().id(TEST_COMMAND_ID))

    assertEquals(CommandStatusResponse().id(TEST_COMMAND_ID).status(CommandStatusResponse.StatusEnum.RUNNING), output)
    verify { commandService.getStatus(TEST_COMMAND_ID) }
  }

  @Test
  fun `getCheckCommandOutput can return a successful response`() {
    every { commandService.getCheckJobOutput(TEST_COMMAND_ID) } returns
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
        .withCheckConnection(
          StandardCheckConnectionOutput()
            .withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED),
        )

    val output = controller.getCheckCommandOutput(CheckCommandOutputRequest().id(TEST_COMMAND_ID))
    assertEquals(CheckCommandOutputResponse().id(TEST_COMMAND_ID).status(CheckCommandOutputResponse.StatusEnum.SUCCEEDED), output)
    verify { commandService.getCheckJobOutput(TEST_COMMAND_ID) }
  }

  @Test
  fun `getCheckCommandOutput can return a failure message`() {
    val message = "why things failed"
    every { commandService.getCheckJobOutput(TEST_COMMAND_ID) } returns
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
        .withCheckConnection(
          StandardCheckConnectionOutput()
            .withStatus(StandardCheckConnectionOutput.Status.FAILED)
            .withMessage(message),
        )

    val output = controller.getCheckCommandOutput(CheckCommandOutputRequest().id(TEST_COMMAND_ID))
    assertEquals(
      CheckCommandOutputResponse()
        .id(TEST_COMMAND_ID)
        .status(CheckCommandOutputResponse.StatusEnum.FAILED)
        .message(message),
      output,
    )
    verify { commandService.getCheckJobOutput(TEST_COMMAND_ID) }
  }

  @Test
  fun `getCheckCommandOutput can return a failure response`() {
    every { commandService.getCheckJobOutput(TEST_COMMAND_ID) } returns
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
        .withCheckConnection(
          StandardCheckConnectionOutput()
            .withStatus(StandardCheckConnectionOutput.Status.FAILED),
        ).withFailureReason(
          FailureReason()
            .withFailureOrigin(FailureReason.FailureOrigin.SOURCE)
            .withFailureType(FailureReason.FailureType.CONFIG_ERROR)
            .withExternalMessage("external facing message")
            .withInternalMessage("internal facing message")
            .withStacktrace("my stacktrace")
            .withTimestamp(2)
            .withRetryable(false),
        )

    val output = controller.getCheckCommandOutput(CheckCommandOutputRequest().id(TEST_COMMAND_ID))
    assertEquals(
      CheckCommandOutputResponse()
        .id(TEST_COMMAND_ID)
        .status(CheckCommandOutputResponse.StatusEnum.FAILED)
        .failureReason(
          ApiFailureReason()
            .failureOrigin(FailureOrigin.SOURCE)
            .failureType(FailureType.CONFIG_ERROR)
            .externalMessage("external facing message")
            .internalMessage("internal facing message")
            .stacktrace("my stacktrace")
            .timestamp(2)
            .retryable(false),
        ),
      output,
    )
    verify { commandService.getCheckJobOutput(TEST_COMMAND_ID) }
  }

  @Test
  fun `getDiscoverCommandOutput can return a successful response`() {
    val catalogId = UUID.randomUUID()
    val protocolCatalog = AirbyteCatalog().withStreams(listOf(AirbyteStream().withName("streamname")))
    val domainCatalog = ActorCatalog().withCatalog(Jsons.jsonNode(protocolCatalog))
    every { commandService.getDiscoverJobOutput(TEST_COMMAND_ID) } returns
      CommandService.DiscoverJobOutput(
        catalogId = catalogId,
        catalog = domainCatalog,
        failureReason = null,
      )

    val apiCatalog =
      io.airbyte.api.model.generated.AirbyteCatalog().streams(
        listOf(
          io.airbyte.api.model.generated
            .AirbyteStreamAndConfiguration()
            .stream(
              io.airbyte.api.model.generated
                .AirbyteStream()
                .name("streamname"),
            ),
        ),
      )
    every { catalogConverter.toApi(protocolCatalog, any()) } returns apiCatalog

    val output = controller.getDiscoverCommandOutput(DiscoverCommandOutputRequest().id(TEST_COMMAND_ID))
    assertEquals(
      DiscoverCommandOutputResponse()
        .id(TEST_COMMAND_ID)
        .catalogId(catalogId)
        .catalog(apiCatalog)
        .status(DiscoverCommandOutputResponse.StatusEnum.SUCCEEDED),
      output,
    )
    verify { commandService.getDiscoverJobOutput(TEST_COMMAND_ID) }
  }

  @Test
  fun `getDiscoverCommandOutput can return a failure response`() {
    every { commandService.getDiscoverJobOutput(TEST_COMMAND_ID) } returns
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
      )

    val output = controller.getDiscoverCommandOutput(DiscoverCommandOutputRequest().id(TEST_COMMAND_ID))
    assertEquals(
      DiscoverCommandOutputResponse()
        .id(TEST_COMMAND_ID)
        .status(DiscoverCommandOutputResponse.StatusEnum.FAILED)
        .failureReason(
          ApiFailureReason()
            .failureOrigin(FailureOrigin.SOURCE)
            .failureType(FailureType.CONFIG_ERROR)
            .externalMessage("external discover facing message")
            .internalMessage("internal discover facing message")
            .stacktrace("my discover stacktrace")
            .timestamp(2)
            .retryable(false),
        ),
      output,
    )
    verify { commandService.getDiscoverJobOutput(TEST_COMMAND_ID) }
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
            FailureReason().withFailureOrigin(FailureReason.FailureOrigin.SOURCE).withExternalMessage("Something to validate"),
          ),
        )
    every { commandService.getReplicationOutput(TEST_COMMAND_ID) } returns persistedReplicationOutput

    val output = controller.getReplicateCommandOutput(ReplicateCommandOutputRequest().id(TEST_COMMAND_ID))
    assertEquals(
      ReplicateCommandOutputResponse()
        .id(TEST_COMMAND_ID)
        .attemptSummary(persistedReplicationOutput.replicationAttemptSummary)
        .failures(
          listOf(
            ApiFailureReason()
              .failureOrigin(FailureOrigin.SOURCE)
              .externalMessage("Something to validate"),
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

    val output = controller.toApi(input)

    assertEquals(
      ApiFailureReason()
        .failureOrigin(FailureOrigin.SOURCE)
        .failureType(FailureType.CONFIG_ERROR)
        .internalMessage("example internal message")
        .externalMessage("example external message")
        .fromTraceMessage(true)
        .stacktrace("example stacktrace")
        .retryable(true)
        .timestamp(42)
        .streamDescriptor(ApiStreamDescriptor().name("example name").namespace("example namespace")),
      output,
    )
  }

  @Test
  fun `FailureReason toApi handles null fields`() {
    assertEquals(ApiFailureReason(), controller.toApi(FailureReason()))
  }

  @Test
  fun `run check with an actor id`() {
    val request =
      RunCheckCommandRequest()
        .id(TEST_COMMAND_ID)
        .actorId(TEST_ACTOR_ID)
    val output = controller.runCheckCommand(request)
    assertEquals(RunCheckCommandResponse().id(TEST_COMMAND_ID), output)
    verify {
      commandService.createCheckCommand(
        TEST_COMMAND_ID,
        TEST_ACTOR_ID,
        null,
        null,
        WorkloadPriority.DEFAULT,
        null,
        any(),
      )
    }
  }

  @Test
  fun `run check with an actor id returns 200 if the command already existed`() {
    val request =
      RunCheckCommandRequest()
        .id(TEST_COMMAND_ID)
        .actorId(TEST_ACTOR_ID)
    val output = controller.runCheckCommand(request)
    assertEquals(RunCheckCommandResponse().id(TEST_COMMAND_ID), output)
    verify {
      commandService.createCheckCommand(
        TEST_COMMAND_ID,
        TEST_ACTOR_ID,
        null,
        null,
        WorkloadPriority.DEFAULT,
        null,
        any(),
      )
    }
  }

  @Test
  fun `run check with an actor id all relevant optional fields`() {
    val request =
      RunCheckCommandRequest()
        .id(TEST_COMMAND_ID)
        .actorId(TEST_ACTOR_ID)
        .jobId(TEST_JOB_ID)
        .attemptNumber(TEST_ATTEMPT_NUMBER.toInt())
        .priority("high")
        .signalInput(TEST_SIGNAL_INPUT)
    val output = controller.runCheckCommand(request)
    assertEquals(RunCheckCommandResponse().id(TEST_COMMAND_ID), output)
    verify {
      commandService.createCheckCommand(
        TEST_COMMAND_ID,
        TEST_ACTOR_ID,
        TEST_JOB_ID,
        TEST_ATTEMPT_NUMBER,
        WorkloadPriority.HIGH,
        TEST_SIGNAL_INPUT,
        Jsons.jsonNode(request),
      )
    }
  }

  @Test
  fun `run check with an actor definition id and a config`() {
    val sanitizedConfig = Jsons.deserialize("""{"sanitized":"config"}""")
    every { secretSanitizer.sanitizePartialConfig(TEST_ACTOR_DEFINITION_ID, TEST_WORKSPACE_ID, TEST_CONFIG) } returns sanitizedConfig
    val request =
      RunCheckCommandRequest()
        .id(TEST_COMMAND_ID)
        .actorDefinitionId(TEST_ACTOR_DEFINITION_ID)
        .workspaceId(TEST_WORKSPACE_ID)
        .config(TEST_CONFIG)
    val output = controller.runCheckCommand(request)
    assertEquals(RunCheckCommandResponse().id(TEST_COMMAND_ID), output)

    val expectedCommandInput =
      RunCheckCommandRequest()
        .id(TEST_COMMAND_ID)
        .actorDefinitionId(TEST_ACTOR_DEFINITION_ID)
        .workspaceId(TEST_WORKSPACE_ID)
        .config(sanitizedConfig)
    verify {
      commandService.createCheckCommand(
        TEST_COMMAND_ID,
        TEST_ACTOR_DEFINITION_ID,
        TEST_WORKSPACE_ID,
        sanitizedConfig,
        WorkloadPriority.DEFAULT,
        null,
        Jsons.jsonNode(expectedCommandInput),
      )
    }
  }

  @Test
  fun `run check with an actor definition id and a config and relevant optional fields`() {
    val sanitizedConfig = Jsons.deserialize("""{"sanitized":"config"}""")
    every { secretSanitizer.sanitizePartialConfig(TEST_ACTOR_DEFINITION_ID, TEST_WORKSPACE_ID, TEST_CONFIG) } returns sanitizedConfig
    val request =
      RunCheckCommandRequest()
        .id(TEST_COMMAND_ID)
        .actorDefinitionId(TEST_ACTOR_DEFINITION_ID)
        .workspaceId(TEST_WORKSPACE_ID)
        .config(TEST_CONFIG)
        .priority("HIGH")
        .signalInput(TEST_SIGNAL_INPUT)
    val output = controller.runCheckCommand(request)

    assertEquals(RunCheckCommandResponse().id(TEST_COMMAND_ID), output)
    val expectedCommandInput =
      RunCheckCommandRequest()
        .id(TEST_COMMAND_ID)
        .actorDefinitionId(TEST_ACTOR_DEFINITION_ID)
        .workspaceId(TEST_WORKSPACE_ID)
        .config(sanitizedConfig)
        .priority("HIGH")
        .signalInput(TEST_SIGNAL_INPUT)
    verify {
      commandService.createCheckCommand(
        TEST_COMMAND_ID,
        TEST_ACTOR_DEFINITION_ID,
        TEST_WORKSPACE_ID,
        sanitizedConfig,
        WorkloadPriority.HIGH,
        TEST_SIGNAL_INPUT,
        Jsons.jsonNode(expectedCommandInput),
      )
    }
  }

  @Test
  fun `role validation from an ActorId should look up actors`() {
    val actorId = UUID.randomUUID()
    val f = { "looked up actor" }
    val result = controller.withRoleValidation(ActorId(actorId), AuthRoleConstants.WORKSPACE_RUNNER, f)

    verify { actorRepository.findByActorId(actorId) }
    assertEquals("looked up actor", result)
  }

  @Test
  fun `role validation from a CommandId should look up commands`() {
    val commandId = "my-command"
    val f = { "looked up command" }
    val result = controller.withRoleValidation(CommandId(commandId), AuthRoleConstants.WORKSPACE_RUNNER, f)

    verify { commandService.get(commandId = commandId) }
    assertEquals("looked up command", result)
  }

  @Test
  fun `role validation from a ConnectionId should look up connection`() {
    val connectionId = UUID.randomUUID()
    val f = { "looked up connection" }
    val result = controller.withRoleValidation(ConnectionId(connectionId), AuthRoleConstants.WORKSPACE_RUNNER, f)

    verify { workspaceService.getStandardWorkspaceFromConnection(connectionId, false) }
    assertEquals("looked up connection", result)
  }

  @Test
  fun `role validation from a WorkspaceId should check the id directly`() {
    val workspaceId = UUID.randomUUID()
    val f = { "didn't look up workspace" }
    val result = controller.withRoleValidation(WorkspaceId(workspaceId), AuthRoleConstants.WORKSPACE_RUNNER, f)

    verify { workspaceService wasNot Called }
    assertEquals("didn't look up workspace", result)
  }

  @Test
  fun `role validation for an unsupported type throws an IllegalStateException`() {
    val untypedUUID = UUID.randomUUID()
    assertThrows<IllegalStateException> {
      controller.withRoleValidation(untypedUUID, AuthRoleConstants.WORKSPACE_RUNNER) {
        TODO("I should not be executed")
      }
    }
  }

  @Test
  fun `role validation for an unknown object throws a problem`() {
    val actorId = UUID.randomUUID()
    every { actorRepository.findByActorId(actorId) } returns null
    assertThrows<ForbiddenProblem> {
      controller.withRoleValidation(ActorId(actorId), AuthRoleConstants.WORKSPACE_RUNNER) {
        TODO("I should not be executed")
      }
    }
  }
}
