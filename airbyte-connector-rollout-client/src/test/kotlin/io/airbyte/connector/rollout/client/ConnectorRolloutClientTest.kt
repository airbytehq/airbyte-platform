/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.client

import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPause
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.airbyte.connector.rollout.worker.ConnectorRolloutWorkflow
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.slot
import io.mockk.unmockkStatic
import io.mockk.verify
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.workflow.v1.WorkflowExecutionInfo
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse
import io.temporal.api.workflowservice.v1.TerminateWorkflowExecutionRequest
import io.temporal.api.workflowservice.v1.WorkflowServiceGrpc.WorkflowServiceBlockingStub
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.client.WorkflowUpdateHandle
import io.temporal.client.WorkflowUpdateStage
import io.temporal.serviceclient.WorkflowServiceStubs
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.time.Instant
import java.util.UUID

class ConnectorRolloutClientTest {
  private val workflowClientWrapper: WorkflowClientWrapper = mockk<WorkflowClientWrapper>()
  private val workflowClient: WorkflowClient = mockk<WorkflowClient>()
  private val serviceStubs: WorkflowServiceStubs = mockk<WorkflowServiceStubs>()
  private val blockingStub: WorkflowServiceBlockingStub = mockk<WorkflowServiceBlockingStub>()
  private val workflowStub: WorkflowStub = mockk<WorkflowStub>()
  private val rolloutWorkflow: ConnectorRolloutWorkflow = mockk<ConnectorRolloutWorkflow>()

  private lateinit var client: ConnectorRolloutClient

  private val connectorRollout =
    ConnectorRollout(
      id = UUID.randomUUID(),
      workflowRunId = UUID.randomUUID().toString(),
      actorDefinitionId = UUID.randomUUID(),
      releaseCandidateVersionId = UUID.randomUUID(),
      state = ConnectorEnumRolloutState.INITIALIZED,
      createdAt = Instant.now().toEpochMilli(),
      updatedAt = Instant.now().toEpochMilli(),
      hasBreakingChanges = false,
      tag = null,
    )

  private val dockerRepository = "repo"
  private val dockerImageTag = "tag"
  private val actorDefinitionId = connectorRollout.actorDefinitionId
  private val updatedBy = UUID.randomUUID()
  private val rolloutStrategy = ConnectorEnumRolloutStrategy.AUTOMATED
  private val workflowId = "repo:tag:${actorDefinitionId.toString().substring(0, 8)}"

  companion object {
    @JvmStatic
    fun workflowExecutionStatusRunning() =
      listOf(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING,
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
      )

    @JvmStatic
    fun workflowExecutionStatusStopped() =
      listOf(
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED,
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED,
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CANCELED,
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TERMINATED,
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
        WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED,
      )
  }

  @BeforeEach
  fun setup() {
    client = ConnectorRolloutClient(workflowClientWrapper)

    mockkStatic(WorkflowStub::class)
    mockkStatic(WorkflowClient::class)
    every { workflowClientWrapper.getClient() } returns workflowClient
    every { workflowClient.workflowServiceStubs } returns serviceStubs
    every { serviceStubs.blockingStub() } returns blockingStub
    every { workflowClient.options } returns WorkflowClientOptions.newBuilder().setNamespace("test").build()
    every { rolloutWorkflow.run(any()) } returns mockk<ConnectorEnumRolloutState>()
    every { workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, any<WorkflowOptions>()) } returns rolloutWorkflow
    every { WorkflowStub.fromTyped(any<ConnectorRolloutWorkflow>()) } returns workflowStub
    every { workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, workflowId) } returns rolloutWorkflow
  }

  fun buildMockDescribeWorkflowExecutionResponse(workflowExecutionStatus: WorkflowExecutionStatus) {
    val response =
      DescribeWorkflowExecutionResponse
        .newBuilder()
        .apply {
          workflowExecutionInfoBuilder.status = workflowExecutionStatus
        }.build()

    every { blockingStub.describeWorkflowExecution(any()) } returns response
  }

  @AfterEach
  fun tearDown() {
    unmockkStatic(WorkflowStub::class)
    unmockkStatic(WorkflowClient::class)
  }

  @ParameterizedTest
  @MethodSource("workflowExecutionStatusRunning")
  fun `doRollout should skip workflow start when status is RUNNING`(workflowExecutionStatus: WorkflowExecutionStatus) {
    buildMockDescribeWorkflowExecutionResponse(workflowExecutionStatus)
    every { workflowStub.startUpdate(any<String>(), any<WorkflowUpdateStage>(), ConnectorRolloutOutput::class.java, any()) } returns
      mockk<WorkflowUpdateHandle<ConnectorRolloutOutput>>()

    client.doRollout(connectorRollout, dockerRepository, dockerImageTag, actorDefinitionId, listOf(UUID.randomUUID()), 10, updatedBy, rolloutStrategy)

    verify(exactly = 0) {
      workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, any<WorkflowOptions>())
    }
    verify {
      workflowStub.startUpdate("progressRollout", WorkflowUpdateStage.ACCEPTED, ConnectorRolloutOutput::class.java, any())
    }
  }

  @ParameterizedTest
  @MethodSource("workflowExecutionStatusRunning")
  fun `pauseRollout should skip workflow start when status is RUNNING`(workflowExecutionStatus: WorkflowExecutionStatus) {
    buildMockDescribeWorkflowExecutionResponse(workflowExecutionStatus)
    every { rolloutWorkflow.pauseRollout(any<ConnectorRolloutActivityInputPause>()) } returns mockk<ConnectorRolloutOutput>()

    client.pauseRollout(connectorRollout, dockerRepository, dockerImageTag, actorDefinitionId, "paused", updatedBy, rolloutStrategy)

    verify(exactly = 0) {
      workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, any<WorkflowOptions>())
    }
    verify {
      rolloutWorkflow.pauseRollout(match { it.rolloutId == connectorRollout.id })
    }
  }

  @ParameterizedTest
  @MethodSource("workflowExecutionStatusRunning")
  fun `finalizeRollout should skip workflow start when status is RUNNING`(workflowExecutionStatus: WorkflowExecutionStatus) {
    buildMockDescribeWorkflowExecutionResponse(workflowExecutionStatus)

    every { workflowStub.startUpdate(any<String>(), any<WorkflowUpdateStage>(), ConnectorRolloutOutput::class.java, any()) } returns
      mockk<WorkflowUpdateHandle<ConnectorRolloutOutput>>()

    client.finalizeRollout(
      connectorRollout,
      dockerRepository,
      dockerImageTag,
      actorDefinitionId,
      defaultDockerImageTag = "default-tag",
      finalState = ConnectorRolloutFinalState.CANCELED,
      errorMsg = null,
      failedReason = null,
      updatedBy = updatedBy,
      rolloutStrategy = rolloutStrategy,
    )

    verify(exactly = 0) {
      workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, any<WorkflowOptions>())
    }
    verify {
      workflowStub.startUpdate("finalizeRollout", WorkflowUpdateStage.ACCEPTED, ConnectorRolloutOutput::class.java, any())
    }
  }

  @Test
  fun `should start workflow for doRollout if NOT_FOUND error is thrown`() {
    every { blockingStub.describeWorkflowExecution(any()) } throws StatusRuntimeException(Status.NOT_FOUND)
    every { workflowStub.startUpdate(any<String>(), any<WorkflowUpdateStage>(), ConnectorRolloutOutput::class.java, any()) } returns
      mockk<WorkflowUpdateHandle<ConnectorRolloutOutput>>()

    every { WorkflowStub.fromTyped(rolloutWorkflow) } returns workflowStub
    every {
      workflowStub.startUpdate(
        eq("progressRollout"),
        eq(WorkflowUpdateStage.ACCEPTED),
        eq(ConnectorRolloutOutput::class.java),
        any<ConnectorRolloutActivityInputRollout>(),
      )
    } returns mockk()

    client.doRollout(connectorRollout, dockerRepository, dockerImageTag, actorDefinitionId, listOf(UUID.randomUUID()), 20, updatedBy, rolloutStrategy)

    verify {
      workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, any<WorkflowOptions>())
      workflowStub.startUpdate("progressRollout", WorkflowUpdateStage.ACCEPTED, ConnectorRolloutOutput::class.java, any())
    }
  }

  @Test
  fun `should start workflow for pauseRollout if NOT_FOUND error is thrown`() {
    every { blockingStub.describeWorkflowExecution(any()) } throws StatusRuntimeException(Status.NOT_FOUND)
    every { rolloutWorkflow.pauseRollout(any<ConnectorRolloutActivityInputPause>()) } returns mockk<ConnectorRolloutOutput>()

    every { workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, workflowId) } returns rolloutWorkflow

    val input =
      ConnectorRolloutActivityInputPause(
        dockerRepository,
        dockerImageTag,
        actorDefinitionId,
        connectorRollout.id,
        "paused",
        updatedBy,
        rolloutStrategy,
      )
    every { rolloutWorkflow.pauseRollout(input) } returns mockk()

    client.pauseRollout(connectorRollout, dockerRepository, dockerImageTag, actorDefinitionId, "paused", updatedBy, rolloutStrategy)

    verify {
      workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, any<WorkflowOptions>())
      rolloutWorkflow.pauseRollout(match { it.rolloutId == input.rolloutId })
    }
  }

  @Test
  fun `should start workflow for finalizeRollout if NOT_FOUND error is thrown`() {
    every { blockingStub.describeWorkflowExecution(any()) } throws StatusRuntimeException(Status.NOT_FOUND)
    every { workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, workflowId) } returns rolloutWorkflow

    every { WorkflowStub.fromTyped(rolloutWorkflow) } returns workflowStub
    every {
      workflowStub.startUpdate(
        eq("finalizeRollout"),
        eq(WorkflowUpdateStage.ACCEPTED),
        eq(ConnectorRolloutOutput::class.java),
        any<ConnectorRolloutActivityInputFinalize>(),
      )
    } returns mockk()

    client.finalizeRollout(
      connectorRollout,
      dockerRepository,
      dockerImageTag,
      actorDefinitionId,
      defaultDockerImageTag = "default-tag",
      finalState = ConnectorRolloutFinalState.CANCELED,
      errorMsg = null,
      failedReason = null,
      updatedBy = updatedBy,
      rolloutStrategy = rolloutStrategy,
    )

    verify {
      workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, any<WorkflowOptions>())
      workflowStub.startUpdate("finalizeRollout", WorkflowUpdateStage.ACCEPTED, ConnectorRolloutOutput::class.java, any())
    }
  }

  @ParameterizedTest
  @MethodSource("workflowExecutionStatusStopped")
  fun `should start workflow for doRollout if workflow is not running`(workflowExecutionStatus: WorkflowExecutionStatus) {
    buildMockDescribeWorkflowExecutionResponse(workflowExecutionStatus)
    every { workflowStub.startUpdate(any<String>(), any<WorkflowUpdateStage>(), ConnectorRolloutOutput::class.java, any()) } returns
      mockk<WorkflowUpdateHandle<ConnectorRolloutOutput>>()

    every { WorkflowStub.fromTyped(rolloutWorkflow) } returns workflowStub
    every {
      workflowStub.startUpdate(
        eq("progressRollout"),
        eq(WorkflowUpdateStage.ACCEPTED),
        eq(ConnectorRolloutOutput::class.java),
        any<ConnectorRolloutActivityInputRollout>(),
      )
    } returns mockk()

    client.doRollout(connectorRollout, dockerRepository, dockerImageTag, actorDefinitionId, listOf(UUID.randomUUID()), 20, updatedBy, rolloutStrategy)

    verify {
      workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, any<WorkflowOptions>())
      workflowStub.startUpdate("progressRollout", WorkflowUpdateStage.ACCEPTED, ConnectorRolloutOutput::class.java, any())
    }
  }

  @ParameterizedTest
  @MethodSource("workflowExecutionStatusStopped")
  fun `should start workflow for pauseRollout if workflow is not running`(workflowExecutionStatus: WorkflowExecutionStatus) {
    buildMockDescribeWorkflowExecutionResponse(workflowExecutionStatus)
    every { rolloutWorkflow.pauseRollout(any<ConnectorRolloutActivityInputPause>()) } returns mockk<ConnectorRolloutOutput>()

    every { workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, workflowId) } returns rolloutWorkflow

    val input =
      ConnectorRolloutActivityInputPause(
        dockerRepository,
        dockerImageTag,
        actorDefinitionId,
        connectorRollout.id,
        "paused",
        updatedBy,
        rolloutStrategy,
      )
    every { rolloutWorkflow.pauseRollout(input) } returns mockk()

    client.pauseRollout(connectorRollout, dockerRepository, dockerImageTag, actorDefinitionId, "paused", updatedBy, rolloutStrategy)

    verify {
      workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, any<WorkflowOptions>())
      rolloutWorkflow.pauseRollout(match { it.rolloutId == input.rolloutId })
    }
  }

  @ParameterizedTest
  @MethodSource("workflowExecutionStatusStopped")
  fun `should start workflow for finalizeRollout if workflow is not running`(workflowExecutionStatus: WorkflowExecutionStatus) {
    buildMockDescribeWorkflowExecutionResponse(workflowExecutionStatus)

    every { workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, workflowId) } returns rolloutWorkflow

    every { WorkflowStub.fromTyped(rolloutWorkflow) } returns workflowStub
    every {
      workflowStub.startUpdate(
        eq("finalizeRollout"),
        eq(WorkflowUpdateStage.ACCEPTED),
        eq(ConnectorRolloutOutput::class.java),
        any<ConnectorRolloutActivityInputFinalize>(),
      )
    } returns mockk()

    client.finalizeRollout(
      connectorRollout,
      dockerRepository,
      dockerImageTag,
      actorDefinitionId,
      defaultDockerImageTag = "default-tag",
      finalState = ConnectorRolloutFinalState.CANCELED,
      errorMsg = null,
      failedReason = null,
      updatedBy = updatedBy,
      rolloutStrategy = rolloutStrategy,
    )

    verify {
      workflowClient.newWorkflowStub(ConnectorRolloutWorkflow::class.java, any<WorkflowOptions>())
      workflowStub.startUpdate("finalizeRollout", WorkflowUpdateStage.ACCEPTED, ConnectorRolloutOutput::class.java, any())
    }
  }

  @Test
  fun `should throw if status is UNRECOGNIZED`() {
    val info =
      mockk<WorkflowExecutionInfo> {
        every { status } returns WorkflowExecutionStatus.UNRECOGNIZED
      }

    val response =
      mockk<DescribeWorkflowExecutionResponse> {
        every { workflowExecutionInfo } returns info
      }

    every { blockingStub.describeWorkflowExecution(any()) } returns response

    assertThrows<RuntimeException> {
      client.doRollout(
        connectorRollout,
        dockerRepository,
        dockerImageTag,
        actorDefinitionId,
        listOf(UUID.randomUUID()),
        10,
        updatedBy,
        rolloutStrategy,
      )
    }
  }

  @Test
  fun `cancelRollout should terminate the workflow successfully`() {
    val terminationSlot = slot<TerminateWorkflowExecutionRequest>()
    every {
      blockingStub.terminateWorkflowExecution(capture(terminationSlot))
    } returns mockk()

    client.cancelRollout(
      connectorRollout,
      dockerRepository,
      dockerImageTag,
      actorDefinitionId,
      errorMsg = "some error",
      failedReason = "user cancelled",
    )

    val request = terminationSlot.captured
    assertEquals(workflowId, request.workflowExecution.workflowId)
    assertTrue(request.reason.contains("Manual cancellation"))
    assertTrue(request.reason.contains("some error"))
    assertTrue(request.reason.contains("user cancelled"))

    verify {
      blockingStub.terminateWorkflowExecution(any())
    }
  }

  @Test
  fun `cancelRollout should log error if termination fails`() {
    val exception = StatusRuntimeException(Status.INTERNAL.withDescription("Temporal failed"))
    every {
      blockingStub.terminateWorkflowExecution(any())
    } throws exception

    client.cancelRollout(
      connectorRollout,
      dockerRepository,
      dockerImageTag,
      actorDefinitionId,
      errorMsg = "some error",
      failedReason = "failed to terminate",
    )

    verify {
      blockingStub.terminateWorkflowExecution(any())
    }
  }
}
