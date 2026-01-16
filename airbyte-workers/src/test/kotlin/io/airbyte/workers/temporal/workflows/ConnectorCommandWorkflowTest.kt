/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.workflows

import io.airbyte.commons.temporal.TemporalConstants
import io.airbyte.commons.temporal.WorkflowClientWrapped
import io.airbyte.commons.temporal.scheduling.CheckCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardDiscoverCatalogInput
import io.airbyte.micronaut.temporal.TemporalProxyHelper
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.commands.CheckCommand
import io.airbyte.workers.commands.CheckCommandV2
import io.airbyte.workers.commands.DiscoverCommand
import io.airbyte.workers.commands.DiscoverCommandV2
import io.airbyte.workers.commands.ReplicationCommand
import io.airbyte.workers.commands.SpecCommand
import io.airbyte.workers.commands.SpecCommandV2
import io.airbyte.workers.workload.WorkspaceNotFoundException
import io.micronaut.context.BeanRegistration
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.temporal.activity.ActivityCancellationType
import io.temporal.activity.ActivityOptions
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowFailedException
import io.temporal.client.WorkflowOptions
import io.temporal.common.RetryOptions
import io.temporal.failure.ActivityFailure
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.Worker
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.UUID
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class ConnectorCommandWorkflowTest {
  companion object {
    const val QUEUE_NAME = "connector_command_queue"

    lateinit var checkCommand: CheckCommand
    lateinit var checkCommandThroughApi: CheckCommandV2
    lateinit var discoverCommand: DiscoverCommand
    lateinit var discoverCommandV2: DiscoverCommandV2
    lateinit var specCommand: SpecCommand
    lateinit var specCommandV2: SpecCommandV2
    lateinit var replicationCommand: ReplicationCommand
    lateinit var activityExecutionContextProvider: ActivityExecutionContextProvider
    lateinit var connectorCommandActivity: ConnectorCommandActivity

    lateinit var testEnv: TestWorkflowEnvironment
    lateinit var worker: Worker
    lateinit var client: WorkflowClient
    lateinit var temporalProxyHelper: TemporalProxyHelper

    @JvmStatic
    @BeforeAll
    fun setup() {
      val shortActivityOptions =
        ActivityOptions
          .newBuilder()
          .setStartToCloseTimeout(10.seconds.toJavaDuration())
          .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
          .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(1).build())
          .setHeartbeatTimeout(TemporalConstants.HEARTBEAT_TIMEOUT)
          .build()
      val activityOptionsBeanRegistration: BeanRegistration<ActivityOptions> =
        mockk(relaxed = true) {
          every { identifier } returns
            mockk {
              every { name } returns "shortActivityOptions"
            }
          every { bean } returns shortActivityOptions
        }
      temporalProxyHelper = TemporalProxyHelper(listOf(activityOptionsBeanRegistration))

      testEnv = TestWorkflowEnvironment.newInstance()
      worker = testEnv.newWorker(QUEUE_NAME)
      worker.registerWorkflowImplementationTypes(temporalProxyHelper.proxyWorkflowClass(ConnectorCommandWorkflowImpl::class.java))
      client = testEnv.workflowClient

      checkCommand = mockk()
      checkCommandThroughApi = mockk()
      discoverCommand = mockk()
      discoverCommandV2 = mockk()
      specCommand = mockk()
      specCommandV2 = mockk()
      replicationCommand = mockk()
      activityExecutionContextProvider = ActivityExecutionContextProvider()
      connectorCommandActivity =
        ConnectorCommandActivityImpl(
          checkCommand,
          checkCommandThroughApi,
          discoverCommand,
          discoverCommandV2,
          specCommand,
          specCommandV2,
          replicationCommand,
          activityExecutionContextProvider,
          mockk(relaxed = true),
        )
      worker.registerActivitiesImplementations(connectorCommandActivity)
      testEnv.start()
    }

    @JvmStatic
    @AfterAll
    fun teardown() {
      testEnv.close()
    }
  }

  @Test
  fun `testing check`() {
    val workflowClient = WorkflowClientWrapped(client, mockk(relaxed = true))
    val workloadId = "test workload"
    val output = ConnectorJobOutput().withOutputType(ConnectorJobOutput.OutputType.CHECK_CONNECTION)

    every { checkCommand.start(any(), any()) } returns workloadId
    every { checkCommand.isTerminal(workloadId) } returns false andThen false andThen true
    every { checkCommand.getOutput(workloadId) } returns output
    every { checkCommand.getAwaitDuration() } returns 1.minutes

    val input =
      CheckCommandInput(
        input =
          CheckCommandInput.CheckConnectionInput(
            jobRunConfig = JobRunConfig(),
            integrationLauncherConfig = IntegrationLauncherConfig(),
            checkConnectionInput = StandardCheckConnectionInput(),
          ),
      )

    val result =
      workflowClient
        .newWorkflowStub(ConnectorCommandWorkflow::class.java, WorkflowOptions.newBuilder().setTaskQueue(QUEUE_NAME).build())
        .run(input)
    assertEquals(output, result)
  }

  @Test
  fun `workflow returns structured output when workspace not found during discover`() {
    val workflowClient = WorkflowClientWrapped(client, mockk(relaxed = true))
    val workspaceId = UUID.randomUUID()

    // Mock the discover command to throw WorkspaceNotFoundException
    every { discoverCommand.start(any(), any()) } throws
      WorkspaceNotFoundException(workspaceId, "Workspace $workspaceId not found")
    every { discoverCommand.getAwaitDuration() } returns 1.minutes

    val input =
      DiscoverCommandInput(
        input =
          DiscoverCommandInput.DiscoverCatalogInput(
            jobRunConfig = JobRunConfig(),
            integrationLauncherConfig = IntegrationLauncherConfig(),
            discoverCatalogInput = StandardDiscoverCatalogInput(),
          ),
      )

    val result =
      workflowClient
        .newWorkflowStub(ConnectorCommandWorkflow::class.java, WorkflowOptions.newBuilder().setTaskQueue(QUEUE_NAME).build())
        .run(input)

    // Verify workflow completes successfully with structured failure
    assertNotNull(result)
    assertNotNull(result.failureReason)
    assertEquals(FailureReason.FailureOrigin.AIRBYTE_PLATFORM, result.failureReason.failureOrigin)
    assertEquals(FailureReason.FailureType.SYSTEM_ERROR, result.failureReason.failureType)
    assertFalse(result.failureReason.retryable)

    // Verify metadata contains error code and workflow type
    assertEquals("WORKSPACE_NOT_FOUND", result.failureReason.metadata.additionalProperties["errorCode"])
    assertEquals("discover", result.failureReason.metadata.additionalProperties["workflowType"])

    // Verify external message is user-friendly
    assertTrue(result.failureReason.externalMessage.contains("workspace"))
    assertTrue(result.failureReason.externalMessage.contains("deleted"))
  }

  @Test
  fun `workflow returns structured output when workspace not found during check`() {
    val workflowClient = WorkflowClientWrapped(client, mockk(relaxed = true))
    val workspaceId = UUID.randomUUID()

    every { checkCommand.start(any(), any()) } throws
      WorkspaceNotFoundException(workspaceId, "Workspace $workspaceId not found")
    every { checkCommand.getAwaitDuration() } returns 1.minutes

    val input =
      CheckCommandInput(
        input =
          CheckCommandInput.CheckConnectionInput(
            jobRunConfig = JobRunConfig(),
            integrationLauncherConfig = IntegrationLauncherConfig(),
            checkConnectionInput = StandardCheckConnectionInput(),
          ),
      )

    val result =
      workflowClient
        .newWorkflowStub(ConnectorCommandWorkflow::class.java, WorkflowOptions.newBuilder().setTaskQueue(QUEUE_NAME).build())
        .run(input)

    assertNotNull(result.failureReason)
    assertEquals("WORKSPACE_NOT_FOUND", result.failureReason.metadata.additionalProperties["errorCode"])
    assertEquals("check", result.failureReason.metadata.additionalProperties["workflowType"])
  }

  @Test
  fun `workflow propagates other ActivityFailures normally`() {
    val workflowClient = WorkflowClientWrapped(client, mockk(relaxed = true))

    // Mock a different type of failure
    every { checkCommand.start(any(), any()) } throws RuntimeException("Network error")
    every { checkCommand.getAwaitDuration() } returns 1.minutes

    val input =
      CheckCommandInput(
        input =
          CheckCommandInput.CheckConnectionInput(
            jobRunConfig = JobRunConfig(),
            integrationLauncherConfig = IntegrationLauncherConfig(),
            checkConnectionInput = StandardCheckConnectionInput(),
          ),
      )

    // Should throw, not return graceful output
    // Temporal wraps activity failures in WorkflowFailedException
    val exception =
      assertThrows<WorkflowFailedException> {
        workflowClient
          .newWorkflowStub(ConnectorCommandWorkflow::class.java, WorkflowOptions.newBuilder().setTaskQueue(QUEUE_NAME).build())
          .run(input)
      }

    // Verify it's an ActivityFailure wrapped in WorkflowFailedException
    assertTrue(exception.cause is ActivityFailure)
  }

  @Test
  fun `workflow calls cancel on command when cancelled during wait`() {
    val workloadId = "test workload"

    every { checkCommand.start(any(), any()) } returns workloadId
    every { checkCommand.isTerminal(workloadId) } returns false // Keep workflow waiting
    every { checkCommand.getAwaitDuration() } returns 10.seconds
    every { checkCommand.cancel(workloadId) } returns Unit

    val input =
      CheckCommandInput(
        input =
          CheckCommandInput.CheckConnectionInput(
            jobRunConfig = JobRunConfig(),
            integrationLauncherConfig = IntegrationLauncherConfig(),
            checkConnectionInput = StandardCheckConnectionInput(),
          ),
      )

    val workflowStub = client.newWorkflowStub(ConnectorCommandWorkflow::class.java, WorkflowOptions.newBuilder().setTaskQueue(QUEUE_NAME).build())

    // Start workflow asynchronously
    val execution = WorkflowClient.start(workflowStub::run, input)

    // Give the workflow time to start and begin waiting
    Thread.sleep(1000)

    // Cancel the workflow
    client.newUntypedWorkflowStub(execution.workflowId).cancel()

    // Verify that cancel was called on the command
    verify(timeout = 5000) { checkCommand.cancel(workloadId) }
  }
}
