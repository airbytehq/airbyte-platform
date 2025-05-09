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
import io.airbyte.api.model.generated.FailureOrigin
import io.airbyte.api.model.generated.FailureType
import io.airbyte.api.model.generated.RunCheckCommandRequest
import io.airbyte.api.model.generated.RunCheckCommandResponse
import io.airbyte.commons.json.Jsons
import io.airbyte.commons.server.helpers.SecretSanitizer
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionOutput
import io.airbyte.config.WorkloadPriority
import io.airbyte.server.services.CommandService
import io.airbyte.server.services.CommandStatus
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

class CommandApiControllerTest {
  private lateinit var controller: CommandApiController
  private lateinit var commandService: CommandService
  private lateinit var secretSanitizer: SecretSanitizer

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
    commandService = mockk(relaxed = true)
    secretSanitizer = mockk(relaxed = true)
    controller = CommandApiController(commandService, secretSanitizer)
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
    every { commandService.getConnectorJobOutput(TEST_COMMAND_ID) } returns
      ConnectorJobOutput()
        .withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)
        .withCheckConnection(
          StandardCheckConnectionOutput()
            .withStatus(StandardCheckConnectionOutput.Status.SUCCEEDED),
        )

    val output = controller.getCheckCommandOutput(CheckCommandOutputRequest().id(TEST_COMMAND_ID))
    assertEquals(CheckCommandOutputResponse().id(TEST_COMMAND_ID).status(CheckCommandOutputResponse.StatusEnum.SUCCEEDED), output)
    verify { commandService.getConnectorJobOutput(TEST_COMMAND_ID) }
  }

  @Test
  fun `getCheckCommandOutput can return a failure response`() {
    every { commandService.getConnectorJobOutput(TEST_COMMAND_ID) } returns
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
          io.airbyte.api.model.generated
            .FailureReason()
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
    verify { commandService.getConnectorJobOutput(TEST_COMMAND_ID) }
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
  fun `run check with an actor id all relevant optional fields`() {
    val request =
      RunCheckCommandRequest()
        .id(TEST_COMMAND_ID)
        .actorId(TEST_ACTOR_ID)
        .jobId(TEST_JOB_ID)
        .attemptNumber(TEST_ATTEMPT_NUMBER.toBigDecimal())
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
        request,
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
        expectedCommandInput,
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
        expectedCommandInput,
      )
    }
  }
}
