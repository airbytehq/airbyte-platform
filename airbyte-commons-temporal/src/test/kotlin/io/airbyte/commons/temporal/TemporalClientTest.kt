/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.logging.DEFAULT_LOG_FILENAME
import io.airbyte.commons.temporal.config.TemporalQueueConfiguration
import io.airbyte.commons.temporal.scheduling.CheckCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow.JobInformation
import io.airbyte.commons.temporal.scheduling.ConnectorCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.commons.temporal.scheduling.DiscoverCommandInput
import io.airbyte.commons.temporal.scheduling.SpecCommandInput
import io.airbyte.commons.temporal.scheduling.state.WorkflowState
import io.airbyte.config.ActorContext
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.JobCheckConnectionConfig
import io.airbyte.config.JobDiscoverCatalogConfig
import io.airbyte.config.JobGetSpecConfig
import io.airbyte.config.RefreshStream
import io.airbyte.config.StreamDescriptor
import io.airbyte.config.WorkloadPriority
import io.airbyte.config.persistence.StreamRefreshesRepository
import io.airbyte.config.persistence.StreamResetPersistence
import io.airbyte.config.persistence.domain.StreamRefresh
import io.airbyte.config.secrets.InlinedConfigWithSecretRefs
import io.airbyte.config.secrets.toConfigWithRefs
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.models.JobRunConfig
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.workflow.v1.WorkflowExecutionInfo
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse
import io.temporal.api.workflowservice.v1.WorkflowServiceGrpc.WorkflowServiceBlockingStub
import io.temporal.client.BatchRequest
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowClientOptions
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.serviceclient.WorkflowServiceStubs
import io.temporal.workflow.Functions
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import java.util.function.Supplier

/**
 * Setup for the Temporal Client test suite.
 */
internal class TemporalClientTest {
  private lateinit var workflowClient: WorkflowClient
  private lateinit var temporalClient: TemporalClient
  private lateinit var logPath: Path
  private lateinit var workflowServiceStubs: WorkflowServiceStubs
  private lateinit var workflowServiceBlockingStub: WorkflowServiceBlockingStub
  private lateinit var streamResetPersistence: StreamResetPersistence
  private lateinit var streamRefreshesRepository: StreamRefreshesRepository
  private lateinit var connectionManagerUtils: ConnectionManagerUtils
  private lateinit var streamResetRecordsHelper: StreamResetRecordsHelper
  private lateinit var workspaceRoot: Path

  @BeforeEach
  fun setup() {
    workspaceRoot = Files.createTempDirectory(Path.of("/tmp"), "temporal_client_test")
    logPath = workspaceRoot.resolve(JOB_ID.toString()).resolve(ATTEMPT_ID.toString()).resolve(DEFAULT_LOG_FILENAME)
    workflowClient = mockk(relaxed = true)
    every { workflowClient.options } returns WorkflowClientOptions.newBuilder().setNamespace(NAMESPACE).build()
    workflowServiceStubs = mockk(relaxed = true)
    every { workflowClient.workflowServiceStubs } returns workflowServiceStubs
    every { workflowClient.signalWithStart(any<BatchRequest>()) } returns WorkflowExecution.newBuilder().build()
    workflowServiceBlockingStub = mockk(relaxed = true)
    every { workflowServiceStubs.blockingStub() } returns workflowServiceBlockingStub
    streamResetPersistence = mockk(relaxed = true)
    streamRefreshesRepository = mockk(relaxed = true)
    mockWorkflowStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING)
    val metricClient = mockk<MetricClient>(relaxed = true)
    val workflowClientWrapped = WorkflowClientWrapped(workflowClient, metricClient)
    val workflowServiceStubsWrapped = WorkflowServiceStubsWrapped(workflowServiceStubs, metricClient)
    val scopedConfigurationService = mockk<ScopedConfigurationService>(relaxed = true)
    connectionManagerUtils = spyk(ConnectionManagerUtils(workflowClientWrapped, metricClient))
    streamResetRecordsHelper = mockk(relaxed = true)
    temporalClient =
      spyk(
        TemporalClient(
          workspaceRoot,
          TemporalQueueConfiguration(),
          workflowClientWrapped,
          workflowServiceStubsWrapped,
          streamResetPersistence,
          streamRefreshesRepository,
          connectionManagerUtils,
          streamResetRecordsHelper,
          metricClient,
          scopedConfigurationService,
        ),
      )
  }

  @Nested
  internal inner class RestartPerStatus {
    private lateinit var mConnectionManagerUtils: ConnectionManagerUtils

    @BeforeEach
    fun init() {
      mConnectionManagerUtils = mockk()

      val metricClient = mockk<MetricClient>(relaxed = true)
      val scopedConfigurationService = mockk<ScopedConfigurationService>(relaxed = true)
      temporalClient =
        spyk(
          TemporalClient(
            workspaceRoot,
            TemporalQueueConfiguration(),
            WorkflowClientWrapped(workflowClient, metricClient),
            WorkflowServiceStubsWrapped(workflowServiceStubs, metricClient),
            streamResetPersistence,
            streamRefreshesRepository,
            mConnectionManagerUtils,
            streamResetRecordsHelper,
            metricClient,
            scopedConfigurationService,
          ),
        )
    }

    @Test
    fun testRestartFailed() {
      val mConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()

      every { workflowClient.newWorkflowStub(any<Class<ConnectionManagerWorkflow>>(), any<String>()) } returns mConnectionManagerWorkflow
      val connectionId = UUID.fromString("ebbfdc4c-295b-48a0-844f-88551dfad3db")
      val workflowIds = mutableSetOf<UUID>(connectionId)

      every { temporalClient.fetchClosedWorkflowsByStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED) } returns workflowIds
      every { temporalClient.filterOutRunningWorkspaceId(workflowIds) } returns workflowIds
      every { mConnectionManagerUtils.safeTerminateWorkflow(eq(connectionId), any()) } returns mockk()
      every { mConnectionManagerUtils.startConnectionManagerNoSignal(eq(connectionId)) } returns mockk()
      mockWorkflowStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED)
      temporalClient.restartClosedWorkflowByStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED)
      verify { mConnectionManagerUtils.safeTerminateWorkflow(eq(connectionId), any()) }
      verify { mConnectionManagerUtils.startConnectionManagerNoSignal(eq(connectionId)) }
    }
  }

  @Nested
  @DisplayName("Test execute method.")
  internal inner class ExecuteJob {
    @Test
    fun testExecute() {
      val supplier = mockk<Supplier<String>>()
      every { supplier.get() } returns "hello"

      val response = temporalClient.execute(JOB_RUN_CONFIG, supplier)

      Assertions.assertNotNull(response)
      Assertions.assertTrue(response.getOutput().isPresent)
      Assertions.assertEquals("hello", response.getOutput().get())
      Assertions.assertTrue(response.metadata.succeeded)
      Assertions.assertEquals(logPath, response.metadata.logPath)
    }

    @Test
    fun testExecuteWithException() {
      val supplier = mockk<Supplier<String>>()
      every { supplier.get() } throws IllegalStateException()

      val response = temporalClient.execute(JOB_RUN_CONFIG, supplier)

      Assertions.assertNotNull(response)
      Assertions.assertFalse(response.getOutput().isPresent)
      Assertions.assertFalse(response.metadata.succeeded)
      Assertions.assertEquals(logPath, response.metadata.logPath)
    }

    @Test
    fun testExecuteWithConnectorJobFailure() {
      val supplier = mockk<Supplier<ConnectorJobOutput>>()
      val mockFailureReason = mockk<FailureReason>()
      val connectorJobOutput =
        ConnectorJobOutput()
          .withFailureReason(mockFailureReason)
      every { supplier.get() } returns connectorJobOutput

      val response = temporalClient.execute(JOB_RUN_CONFIG, supplier)

      Assertions.assertNotNull(response)
      Assertions.assertTrue(response.getOutput().isPresent)
      Assertions.assertEquals(connectorJobOutput, response.getOutput().get())
      Assertions.assertFalse(response.metadata.succeeded)
      Assertions.assertEquals(logPath, response.metadata.logPath)
    }
  }

  @Nested
  @DisplayName("Test job creation for each configuration type.")
  internal inner class TestJobSubmission {
    @Test
    fun testSubmitGetSpec() {
      val specWorkflow = mockk<ConnectorCommandWorkflow>(relaxed = true)
      val workflowClassSlot = slot<Class<ConnectorCommandWorkflow>>()
      val workflowOptionsSlot = slot<WorkflowOptions>()
      val connectorCommandInputSlot = slot<ConnectorCommandInput>()

      every {
        workflowClient.newWorkflowStub(
          capture(workflowClassSlot),
          capture(workflowOptionsSlot),
        )
      } returns specWorkflow

      val getSpecConfig = JobGetSpecConfig().withDockerImage(IMAGE_NAME1)

      temporalClient.submitGetSpec(JOB_UUID, ATTEMPT_ID, WORKSPACE_ID, getSpecConfig)

      verify { workflowClient.newWorkflowStub(any<Class<ConnectorCommandWorkflow>>(), any<WorkflowOptions>()) }
      verify { specWorkflow.run(capture(connectorCommandInputSlot)) }

      Assertions.assertEquals(ConnectorCommandWorkflow::class.java, workflowClassSlot.captured)
      Assertions.assertEquals(UI_COMMANDS_TASK_QUEUE, workflowOptionsSlot.captured.taskQueue)
      Assertions.assertInstanceOf(SpecCommandInput::class.java, connectorCommandInputSlot.captured)
    }

    @Test
    fun testSubmitCheckConnection() {
      val checkConnectionWorkflow = mockk<ConnectorCommandWorkflow>(relaxed = true)
      val workflowClassSlot = slot<Class<ConnectorCommandWorkflow>>()
      val workflowOptionsSlot = slot<WorkflowOptions>()
      val connectorCommandInputSlot = slot<ConnectorCommandInput>()

      every {
        workflowClient.newWorkflowStub(
          capture(workflowClassSlot),
          capture(workflowOptionsSlot),
        )
      } returns checkConnectionWorkflow

      val checkConnectionConfig =
        JobCheckConnectionConfig()
          .withDockerImage(IMAGE_NAME1)
          .withConnectionConfiguration(InlinedConfigWithSecretRefs(emptyObject()).toConfigWithRefs())

      temporalClient.submitCheckConnection(JOB_UUID, ATTEMPT_ID, WORKSPACE_ID, checkConnectionConfig, ActorContext())

      verify { workflowClient.newWorkflowStub(any<Class<ConnectorCommandWorkflow>>(), any<WorkflowOptions>()) }
      verify { checkConnectionWorkflow.run(capture(connectorCommandInputSlot)) }

      Assertions.assertEquals(ConnectorCommandWorkflow::class.java, workflowClassSlot.captured)
      Assertions.assertEquals(UI_COMMANDS_TASK_QUEUE, workflowOptionsSlot.captured.taskQueue)
      Assertions.assertInstanceOf(CheckCommandInput::class.java, connectorCommandInputSlot.captured)
    }

    @Test
    fun testSubmitDiscoverSchema() {
      val discoverCatalogWorkflow = mockk<ConnectorCommandWorkflow>(relaxed = true)
      val workflowClassSlot = slot<Class<ConnectorCommandWorkflow>>()
      val workflowOptionsSlot = slot<WorkflowOptions>()
      val connectorCommandInputSlot = slot<ConnectorCommandInput>()

      every {
        workflowClient.newWorkflowStub(
          capture(workflowClassSlot),
          capture(workflowOptionsSlot),
        )
      } returns discoverCatalogWorkflow

      val checkConnectionConfig =
        JobDiscoverCatalogConfig()
          .withDockerImage(IMAGE_NAME1)
          .withConnectionConfiguration(InlinedConfigWithSecretRefs(emptyObject()).toConfigWithRefs())

      temporalClient.submitDiscoverSchema(
        JOB_UUID,
        ATTEMPT_ID,
        WORKSPACE_ID,
        checkConnectionConfig,
        ActorContext(),
        WorkloadPriority.DEFAULT,
      )

      verify { workflowClient.newWorkflowStub(any<Class<ConnectorCommandWorkflow>>(), any<WorkflowOptions>()) }
      verify { discoverCatalogWorkflow.run(capture(connectorCommandInputSlot)) }

      Assertions.assertEquals(ConnectorCommandWorkflow::class.java, workflowClassSlot.captured)
      Assertions.assertEquals(UI_COMMANDS_TASK_QUEUE, workflowOptionsSlot.captured.taskQueue)
      Assertions.assertInstanceOf(DiscoverCommandInput::class.java, connectorCommandInputSlot.captured)
    }
  }

  @Nested
  @DisplayName("Test related to the migration to the new scheduler")
  internal inner class TestMigration {
    @DisplayName("Test that the migration is properly done if needed")
    @Test
    fun migrateCalled() {
      val nonMigratedId = UUID.randomUUID()
      val migratedId = UUID.randomUUID()

      every { temporalClient.isInRunningWorkflowCache(connectionManagerUtils.getConnectionManagerName(nonMigratedId)) } returns false
      every { temporalClient.isInRunningWorkflowCache(connectionManagerUtils.getConnectionManagerName(migratedId)) } returns true

      every { temporalClient.refreshRunningWorkflow() } just Runs
      val mConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>(relaxed = true)
      every { temporalClient.submitConnectionUpdaterAsync(nonMigratedId) } returns mConnectionManagerWorkflow
      every { connectionManagerUtils.getConnectionManagerWorkflow(nonMigratedId) } returns mConnectionManagerWorkflow
      every { connectionManagerUtils.getConnectionManagerName(any<UUID>()) } answers { "connection_manager_${firstArg<UUID>()}" }
      every {
        workflowClient.newWorkflowStub(
          any<Class<ConnectionManagerWorkflow>>(),
          any<WorkflowOptions>(),
        )
      } returns mConnectionManagerWorkflow

      every { temporalClient.isInRunningWorkflowCache("connection_manager_$nonMigratedId") } returns false
      every { temporalClient.isInRunningWorkflowCache("connection_manager_$migratedId") } returns true

      temporalClient.migrateSyncIfNeeded(setOf(nonMigratedId, migratedId))

      verify(exactly = 1) { temporalClient.submitConnectionUpdaterAsync(nonMigratedId) }
      verify(exactly = 0) { temporalClient.submitConnectionUpdaterAsync(migratedId) }
    }
  }

  @Nested
  @DisplayName("Test delete connection method.")
  internal inner class ForceCancelConnection {
    @Test
    @DisplayName("Forcing a workflow deletion delegates to the connection manager.")
    fun testForceCancelConnection() {
      temporalClient.forceDeleteWorkflow(CONNECTION_ID)

      verify { connectionManagerUtils.deleteWorkflowIfItExist(CONNECTION_ID) }
    }
  }

  @Nested
  @DisplayName("Test update connection behavior")
  internal inner class UpdateConnection {
    @Test
    @DisplayName("Test update connection when workflow is running")
    fun testUpdateConnection() {
      val mConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
      val mWorkflowState = mockk<WorkflowState>(relaxed = true)

      every { mWorkflowState.isRunning } returns true
      every { mWorkflowState.isDeleted } returns false
      every { mConnectionManagerWorkflow.getState() } returns mWorkflowState
      every { mConnectionManagerWorkflow.connectionUpdated() } just Runs
      every {
        workflowClient.newWorkflowStub(
          any<Class<ConnectionManagerWorkflow>>(),
          any<String>(),
        )
      } returns mConnectionManagerWorkflow

      temporalClient.update(CONNECTION_ID)

      verify(exactly = 1) { mConnectionManagerWorkflow.connectionUpdated() }
    }

    @Test
    @DisplayName("Test update connection method starts a new workflow when workflow is in an unexpected state")
    fun testUpdateConnectionInUnexpectedState() {
      val mConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()

      every { mConnectionManagerWorkflow.getState() } throws IllegalStateException(EXCEPTION_MESSAGE)
      every {
        workflowClient.newWorkflowStub(
          any<Class<ConnectionManagerWorkflow>>(),
          any<String>(),
        )
      } returns mConnectionManagerWorkflow
      every { temporalClient.submitConnectionUpdaterAsync(CONNECTION_ID) } returns mConnectionManagerWorkflow

      val untypedWorkflowStub = mockk<WorkflowStub>()
      every { workflowClient.newUntypedWorkflowStub(any<String>()) } returns untypedWorkflowStub
      every { untypedWorkflowStub.terminate(any<String>()) } just Runs

      temporalClient.update(CONNECTION_ID)

      // this is only called when updating an existing workflow
      verify(exactly = 0) { mConnectionManagerWorkflow.connectionUpdated() }

      verify(exactly = 1) { untypedWorkflowStub.terminate(any<String>()) }
      verify(exactly = 1) { temporalClient.submitConnectionUpdaterAsync(CONNECTION_ID) }
    }

    @Test
    @DisplayName("Test update connection method does nothing when connection is deleted")
    fun testUpdateConnectionDeletedWorkflow() {
      val mConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
      val mWorkflowState = mockk<WorkflowState>(relaxed = true)
      every { mConnectionManagerWorkflow.getState() } returns mWorkflowState
      every { mWorkflowState.isDeleted } returns true
      every {
        workflowClient.newWorkflowStub(any<Class<ConnectionManagerWorkflow>>(), any<String>())
      } returns mConnectionManagerWorkflow
      mockWorkflowStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)

      temporalClient.update(CONNECTION_ID)

      // this is only called when updating an existing workflow
      verify(exactly = 0) { mConnectionManagerWorkflow.connectionUpdated() }
      verify { temporalClient.update(CONNECTION_ID) }
    }
  }

  @Nested
  @DisplayName("Test manual sync behavior")
  internal inner class ManualSync {
    @Test
    @DisplayName("Test startNewManualSync successful")
    fun testStartNewManualSyncSuccess() {
      val mConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
      val mWorkflowState = mockk<WorkflowState>(relaxed = true)
      every { mConnectionManagerWorkflow.getState() } returns mWorkflowState
      every { mWorkflowState.isDeleted } returns false
      every { mWorkflowState.isRunning } returns false andThen true
      every { mConnectionManagerWorkflow.getJobInformation() } returns JobInformation(JOB_ID, ATTEMPT_ID)
      every { mConnectionManagerWorkflow.submitManualSync() } just Runs
      every {
        workflowClient.newWorkflowStub(any<Class<ConnectionManagerWorkflow>>(), any<String>())
      } returns mConnectionManagerWorkflow

      val result = temporalClient.startNewManualSync(CONNECTION_ID)

      Assertions.assertNotNull(result.jobId)
      Assertions.assertEquals(JOB_ID, result.jobId)
      Assertions.assertNull(result.failingReason)
      verify { mConnectionManagerWorkflow.submitManualSync() }
    }

    @Test
    @DisplayName("Test startNewManualSync fails if job is already running")
    fun testStartNewManualSyncAlreadyRunning() {
      val mConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
      val mWorkflowState = mockk<WorkflowState>(relaxed = true)
      every { mConnectionManagerWorkflow.getState() } returns mWorkflowState
      every { mWorkflowState.isDeleted } returns false
      every { mWorkflowState.isRunning } returns true
      every { mConnectionManagerWorkflow.getJobInformation() } returns JobInformation(JOB_ID, ATTEMPT_ID)
      every {
        workflowClient.newWorkflowStub(any<Class<ConnectionManagerWorkflow>>(), any<String>())
      } returns mConnectionManagerWorkflow

      val result = temporalClient.startNewManualSync(CONNECTION_ID)

      Assertions.assertNull(result.jobId)
      Assertions.assertNotNull(result.failingReason)
      verify(exactly = 0) { mConnectionManagerWorkflow.submitManualSync() }
    }

    @Test
    @DisplayName("Test startNewManualSync repairs the workflow if it is in a bad state")
    fun testStartNewManualSyncRepairsBadWorkflowState() {
      val mTerminatedConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()

      // This simulates a workflow in a bad state.
      every { mTerminatedConnectionManagerWorkflow.getState() } throws IllegalStateException(EXCEPTION_MESSAGE)
      every { mTerminatedConnectionManagerWorkflow.getJobInformation() } returns JobInformation(JOB_ID, ATTEMPT_ID)

      val mNewConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
      val mWorkflowState = mockk<WorkflowState>(relaxed = true)
      every { mNewConnectionManagerWorkflow.getState() } returns mWorkflowState
      every { mWorkflowState.isDeleted } returns false
      every { mWorkflowState.isRunning } returns false andThen true
      every { mNewConnectionManagerWorkflow.getJobInformation() } returns JobInformation(JOB_ID, ATTEMPT_ID)
      every { mNewConnectionManagerWorkflow.submitManualSync() } just Runs

      val mBatchRequest = mockk<BatchRequest>(relaxed = true)
      val batchRequestAddSlot = slot<Functions.Proc>()
      every { mBatchRequest.add(capture(batchRequestAddSlot)) } just Runs
      every { workflowClient.newSignalWithStartRequest() } returns mBatchRequest
      every { workflowClient.signalWithStart(mBatchRequest) } returns WorkflowExecution.newBuilder().build()

      // First two calls return terminated workflow, third returns new workflow
      every {
        workflowClient.newWorkflowStub(any<Class<ConnectionManagerWorkflow>>(), any<String>())
      } returnsMany
        listOf(
          mTerminatedConnectionManagerWorkflow,
          mTerminatedConnectionManagerWorkflow,
          mNewConnectionManagerWorkflow,
        )

      every {
        workflowClient.newWorkflowStub(
          any<Class<ConnectionManagerWorkflow>>(),
          any<WorkflowOptions>(),
        )
      } returns mNewConnectionManagerWorkflow

      val result = temporalClient.startNewManualSync(CONNECTION_ID)

      Assertions.assertNotNull(result.jobId)
      Assertions.assertEquals(JOB_ID, result.jobId)
      Assertions.assertNull(result.failingReason)
      verify { workflowClient.signalWithStart(mBatchRequest) }

      // Verify that the submitManualSync signal was passed to the batch request by capturing the
      // argument, executing the signal, and verifying that the desired signal was executed
      val signal = batchRequestAddSlot.captured
      signal.apply()
      verify { mNewConnectionManagerWorkflow.submitManualSync() }
    }

    @Test
    @DisplayName("Test startNewManualSync returns a failure reason when connection is deleted")
    fun testStartNewManualSyncDeletedWorkflow() {
      val mConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
      val mWorkflowState = mockk<WorkflowState>(relaxed = true)
      every { mConnectionManagerWorkflow.getState() } returns mWorkflowState
      every { mWorkflowState.isDeleted } returns true
      every {
        workflowClient.newWorkflowStub(any<Class<ConnectionManagerWorkflow>>(), any<String>())
      } returns mConnectionManagerWorkflow
      mockWorkflowStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)

      val result = temporalClient.startNewManualSync(CONNECTION_ID)

      // this is only called when updating an existing workflow
      Assertions.assertNull(result.jobId)
      Assertions.assertNotNull(result.failingReason)
      verify(exactly = 0) { mConnectionManagerWorkflow.submitManualSync() }
    }
  }

  @Nested
  @DisplayName("Test cancellation behavior")
  internal inner class Cancellation {
    @Test
    @DisplayName("Test startNewCancellation successful")
    fun testStartNewCancellationSuccess() {
      val mConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
      val mWorkflowState = mockk<WorkflowState>(relaxed = true)
      every { mConnectionManagerWorkflow.getState() } returns mWorkflowState
      every { mWorkflowState.isDeleted } returns false
      every { mWorkflowState.isRunning } returns true andThen false
      every { mConnectionManagerWorkflow.getJobInformation() } returns JobInformation(JOB_ID, ATTEMPT_ID)
      every { mConnectionManagerWorkflow.cancelJob() } just Runs
      every { streamResetRecordsHelper.deleteStreamResetRecordsForJob(JOB_ID, CONNECTION_ID) } just Runs
      every {
        workflowClient.newWorkflowStub(any<Class<ConnectionManagerWorkflow>>(), any<String>())
      } returns mConnectionManagerWorkflow

      val result = temporalClient.startNewCancellation(CONNECTION_ID)

      Assertions.assertNotNull(result.jobId)
      Assertions.assertEquals(JOB_ID, result.jobId)
      Assertions.assertNull(result.failingReason)
      verify { mConnectionManagerWorkflow.cancelJob() }
      verify { streamResetRecordsHelper.deleteStreamResetRecordsForJob(JOB_ID, CONNECTION_ID) }
    }

    @Test
    @DisplayName("Test startNewCancellation repairs the workflow if it is in a bad state")
    fun testStartNewCancellationRepairsBadWorkflowState() {
      val mTerminatedConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
      every {
        mTerminatedConnectionManagerWorkflow.getState()
      } throws IllegalStateException(EXCEPTION_MESSAGE)
      every { mTerminatedConnectionManagerWorkflow.getJobInformation() } returns JobInformation(JOB_ID, ATTEMPT_ID)

      val mNewConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
      val mWorkflowState = mockk<WorkflowState>(relaxed = true)
      every { mNewConnectionManagerWorkflow.getState() } returns mWorkflowState
      every { mWorkflowState.isDeleted } returns false
      every { mWorkflowState.isRunning } returns true andThen false
      every { mNewConnectionManagerWorkflow.getJobInformation() } returns JobInformation(JOB_ID, ATTEMPT_ID)
      every { mNewConnectionManagerWorkflow.cancelJob() } just Runs

      val mBatchRequest = mockk<BatchRequest>(relaxed = true)
      val batchRequestAddSlot = slot<Functions.Proc>()
      every { mBatchRequest.add(capture(batchRequestAddSlot)) } just Runs
      every { workflowClient.newSignalWithStartRequest() } returns mBatchRequest
      every { workflowClient.signalWithStart(mBatchRequest) } returns WorkflowExecution.newBuilder().build()

      every {
        workflowClient.newWorkflowStub(any<Class<ConnectionManagerWorkflow>>(), any<String>())
      } returnsMany
        listOf(
          mTerminatedConnectionManagerWorkflow,
          mTerminatedConnectionManagerWorkflow,
          mNewConnectionManagerWorkflow,
        )

      every {
        workflowClient.newWorkflowStub(
          any<Class<ConnectionManagerWorkflow>>(),
          any<WorkflowOptions>(),
        )
      } returns mNewConnectionManagerWorkflow

      val result = temporalClient.startNewCancellation(CONNECTION_ID)

      Assertions.assertNotNull(result.jobId)
      Assertions.assertEquals(ConnectionManagerWorkflow.NON_RUNNING_JOB_ID, result.jobId)
      Assertions.assertNull(result.failingReason)
      verify { workflowClient.signalWithStart(mBatchRequest) }

      // Verify that the cancelJob signal was passed to the batch request by capturing the argument,
      // executing the signal, and verifying that the desired signal was executed
      val signal = batchRequestAddSlot.captured
      signal.apply()
      verify { mNewConnectionManagerWorkflow.cancelJob() }
    }

    @Test
    @DisplayName("Test startNewCancellation returns a failure reason when connection is deleted")
    fun testStartNewCancellationDeletedWorkflow() {
      val mConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
      val mWorkflowState = mockk<WorkflowState>(relaxed = true)
      every { mConnectionManagerWorkflow.getState() } returns mWorkflowState
      every { mWorkflowState.isDeleted } returns true
      every {
        workflowClient.newWorkflowStub(any<Class<ConnectionManagerWorkflow>>(), any<String>())
      } returns mConnectionManagerWorkflow
      mockWorkflowStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)

      val result = temporalClient.startNewCancellation(CONNECTION_ID)

      // this is only called when updating an existing workflow
      Assertions.assertNull(result.jobId)
      Assertions.assertNotNull(result.failingReason)
      verify(exactly = 0) { mConnectionManagerWorkflow.cancelJob() }
    }
  }

  @Nested
  @DisplayName("Test refresh connection behavior")
  internal inner class RefreshConnection {
    @Test
    @DisplayName("Test refreshConnectionAsync saves the stream to refresh and signals workflow")
    fun testRefreshConnectionAsyncHappyPath() {
      val mConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
      val mWorkflowState = mockk<WorkflowState>(relaxed = true)
      every { mConnectionManagerWorkflow.getState() } returns mWorkflowState
      every { mWorkflowState.isDeleted } returns false
      every { mWorkflowState.isRunning } returns false
      val jobId1: Long = 1
      val jobId2: Long = 2
      every { mConnectionManagerWorkflow.getJobInformation() } returnsMany
        listOf(
          JobInformation(jobId1, 0),
          JobInformation(jobId1, 0),
          JobInformation(ConnectionManagerWorkflow.NON_RUNNING_JOB_ID, 0),
          JobInformation(ConnectionManagerWorkflow.NON_RUNNING_JOB_ID, 0),
          JobInformation(jobId2, 0),
          JobInformation(jobId2, 0),
        )
      every {
        workflowClient.newWorkflowStub(
          any<Class<ConnectionManagerWorkflow>>(),
          any<String>(),
        )
      } returns mConnectionManagerWorkflow
      every { streamRefreshesRepository.saveAll(any<Iterable<StreamRefresh>>()) } returns listOf()
      every { connectionManagerUtils.signalWorkflowAndRepairIfNecessary(eq(CONNECTION_ID), any()) } returns mockk()

      val streamsToReset = listOf(STREAM_DESCRIPTOR)
      val refreshType = RefreshStream.RefreshType.TRUNCATE
      temporalClient.refreshConnectionAsync(CONNECTION_ID, streamsToReset, refreshType)

      verify { streamRefreshesRepository.saveAll(any<Iterable<StreamRefresh>>()) }
      verify { connectionManagerUtils.signalWorkflowAndRepairIfNecessary(eq(CONNECTION_ID), any()) }
    }
  }

  @Nested
  @DisplayName("Test reset connection behavior")
  internal inner class ResetConnection {
    @Test
    @DisplayName("Test resetConnection successful")
    fun testResetConnectionSuccess() {
      val mConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
      val mWorkflowState = mockk<WorkflowState>(relaxed = true)
      every { mConnectionManagerWorkflow.getState() } returns mWorkflowState
      every { mWorkflowState.isDeleted } returns false
      every { mWorkflowState.isRunning } returns false
      val jobId1: Long = 1
      val jobId2: Long = 2
      every { mConnectionManagerWorkflow.getJobInformation() } returnsMany
        listOf(
          JobInformation(jobId1, 0),
          JobInformation(jobId1, 0),
          JobInformation(ConnectionManagerWorkflow.NON_RUNNING_JOB_ID, 0),
          JobInformation(ConnectionManagerWorkflow.NON_RUNNING_JOB_ID, 0),
          JobInformation(jobId2, 0),
          JobInformation(jobId2, 0),
        )
      every { mConnectionManagerWorkflow.resetConnection() } just Runs
      every { streamResetPersistence.createStreamResets(CONNECTION_ID, any()) } just Runs
      every {
        workflowClient.newWorkflowStub(any<Class<ConnectionManagerWorkflow>>(), any<String>())
      } returns mConnectionManagerWorkflow

      val streamsToReset = listOf(STREAM_DESCRIPTOR)
      val result = temporalClient.resetConnection(CONNECTION_ID, streamsToReset)

      verify { streamResetPersistence.createStreamResets(CONNECTION_ID, streamsToReset) }

      Assertions.assertNotNull(result.jobId)
      Assertions.assertEquals(jobId2, result.jobId)
      Assertions.assertNull(result.failingReason)
      verify { mConnectionManagerWorkflow.resetConnection() }
    }

    @Test
    @DisplayName("Test resetConnection repairs the workflow if it is in a bad state")
    fun testResetConnectionRepairsBadWorkflowState() {
      val mTerminatedConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()

      // This simulates a workflow in a bad state.
      every { mTerminatedConnectionManagerWorkflow.getState() } throws IllegalStateException(EXCEPTION_MESSAGE)
      every { mTerminatedConnectionManagerWorkflow.getJobInformation() } returns JobInformation(JOB_ID, ATTEMPT_ID)

      val mNewConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
      val mWorkflowState = mockk<WorkflowState>(relaxed = true)
      every { mNewConnectionManagerWorkflow.getState() } returns mWorkflowState
      every { mWorkflowState.isDeleted } returns false
      every { mWorkflowState.isRunning } returns false
      every { mNewConnectionManagerWorkflow.getJobInformation() } returnsMany
        listOf(
          JobInformation(ConnectionManagerWorkflow.NON_RUNNING_JOB_ID, 0),
          JobInformation(ConnectionManagerWorkflow.NON_RUNNING_JOB_ID, 0),
          JobInformation(JOB_ID, 0),
          JobInformation(JOB_ID, 0),
        )
      every { mNewConnectionManagerWorkflow.resetConnection() } just Runs
      every { streamResetPersistence.createStreamResets(CONNECTION_ID, any()) } just Runs

      val mBatchRequest = mockk<BatchRequest>(relaxed = true)
      val batchRequestAddSlot = slot<Functions.Proc>()
      every { mBatchRequest.add(capture(batchRequestAddSlot)) } just Runs
      every { workflowClient.newSignalWithStartRequest() } returns mBatchRequest
      every { workflowClient.signalWithStart(mBatchRequest) } returns WorkflowExecution.newBuilder().build()

      every {
        workflowClient.newWorkflowStub(any<Class<ConnectionManagerWorkflow>>(), any<String>())
      } returnsMany
        listOf(
          mTerminatedConnectionManagerWorkflow,
          mTerminatedConnectionManagerWorkflow,
          mNewConnectionManagerWorkflow,
        )

      every {
        workflowClient.newWorkflowStub(
          any<Class<ConnectionManagerWorkflow>>(),
          any<WorkflowOptions>(),
        )
      } returns mNewConnectionManagerWorkflow

      val streamsToReset = listOf(STREAM_DESCRIPTOR)
      val result = temporalClient.resetConnection(CONNECTION_ID, streamsToReset)

      verify { streamResetPersistence.createStreamResets(CONNECTION_ID, streamsToReset) }

      Assertions.assertNotNull(result.jobId)
      Assertions.assertEquals(JOB_ID, result.jobId)
      Assertions.assertNull(result.failingReason)
      verify { workflowClient.signalWithStart(mBatchRequest) }

      // Verify that the resetConnection signal was passed to the batch request by capturing the argument,
      // executing the signal, and verifying that the desired signal was executed
      val signal = batchRequestAddSlot.captured
      signal.apply()
      verify { mNewConnectionManagerWorkflow.resetConnection() }
    }

    @Test
    @DisplayName("Test resetConnection returns a failure reason when connection is deleted")
    fun testResetConnectionDeletedWorkflow() {
      val mConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
      val mWorkflowState = mockk<WorkflowState>(relaxed = true)
      every { mConnectionManagerWorkflow.getState() } returns mWorkflowState
      every { mWorkflowState.isDeleted } returns true
      every { streamResetPersistence.createStreamResets(CONNECTION_ID, any()) } just Runs
      every {
        workflowClient.newWorkflowStub(any<Class<ConnectionManagerWorkflow>>(), any<String>())
      } returns mConnectionManagerWorkflow
      mockWorkflowStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)

      val streamsToReset = listOf(STREAM_DESCRIPTOR)
      val result = temporalClient.resetConnection(CONNECTION_ID, streamsToReset)

      verify { streamResetPersistence.createStreamResets(CONNECTION_ID, streamsToReset) }

      // this is only called when updating an existing workflow
      Assertions.assertNull(result.jobId)
      Assertions.assertNotNull(result.failingReason)
      verify(exactly = 0) { mConnectionManagerWorkflow.resetConnection() }
    }
  }

  @Test
  @DisplayName("Test manual operation on completed workflow causes a restart")
  fun testManualOperationOnCompletedWorkflow() {
    val mConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
    val mWorkflowState = mockk<WorkflowState>(relaxed = true)
    every { mConnectionManagerWorkflow.getState() } returns mWorkflowState
    every { mWorkflowState.isDeleted } returns false
    every {
      workflowServiceBlockingStub.describeWorkflowExecution(
        any<DescribeWorkflowExecutionRequest>(),
      )
    } returnsMany
      listOf(
        DescribeWorkflowExecutionResponse
          .newBuilder()
          .setWorkflowExecutionInfo(
            WorkflowExecutionInfo.newBuilder().setStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED).buildPartial(),
          ).build(),
        DescribeWorkflowExecutionResponse
          .newBuilder()
          .setWorkflowExecutionInfo(
            WorkflowExecutionInfo.newBuilder().setStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING).buildPartial(),
          ).build(),
      )

    val mNewConnectionManagerWorkflow = mockk<ConnectionManagerWorkflow>()
    val mNewWorkflowState = mockk<WorkflowState>(relaxed = true)
    every { mNewConnectionManagerWorkflow.getState() } returns mNewWorkflowState
    every { mNewWorkflowState.isRunning } returns false andThen true
    every {
      mNewConnectionManagerWorkflow.getJobInformation()
    } returns JobInformation(JOB_ID, ATTEMPT_ID)
    every { mNewConnectionManagerWorkflow.submitManualSync() } just Runs

    val mBatchRequest = mockk<BatchRequest>(relaxed = true)
    val batchRequestAddSlot = slot<Functions.Proc>()
    every { mBatchRequest.add(capture(batchRequestAddSlot)) } just Runs
    every { workflowClient.newSignalWithStartRequest() } returns mBatchRequest
    every { workflowClient.signalWithStart(mBatchRequest) } returns WorkflowExecution.newBuilder().build()

    every {
      workflowClient.newWorkflowStub(
        any<Class<ConnectionManagerWorkflow>>(),
        any<String>(),
      )
    } returnsMany
      listOf(
        mConnectionManagerWorkflow,
        mConnectionManagerWorkflow,
        mNewConnectionManagerWorkflow,
      )

    every {
      workflowClient.newWorkflowStub(any<Class<ConnectionManagerWorkflow>>(), any<WorkflowOptions>())
    } returns mNewConnectionManagerWorkflow

    val mWorkflowStub = mockk<WorkflowStub>()
    every { workflowClient.newUntypedWorkflowStub(any<String>()) } returns mWorkflowStub
    every { mWorkflowStub.terminate(any<String>()) } just Runs

    val result = temporalClient.startNewManualSync(CONNECTION_ID)

    Assertions.assertNotNull(result.jobId)
    Assertions.assertEquals(JOB_ID, result.jobId)
    Assertions.assertNull(result.failingReason)
    verify { workflowClient.signalWithStart(mBatchRequest) }
    verify { mWorkflowStub.terminate(any<String>()) }

    // Verify that the submitManualSync signal was passed to the batch request by capturing the
    // argument, executing the signal, and verifying that the desired signal was executed
    val signal = batchRequestAddSlot.captured
    signal.apply()
    verify { mNewConnectionManagerWorkflow.submitManualSync() }
  }

  private fun mockWorkflowStatus(status: WorkflowExecutionStatus) {
    every {
      workflowServiceBlockingStub.describeWorkflowExecution(
        any<DescribeWorkflowExecutionRequest>(),
      )
    } returns
      DescribeWorkflowExecutionResponse
        .newBuilder()
        .setWorkflowExecutionInfo(
          WorkflowExecutionInfo.newBuilder().setStatus(status).buildPartial(),
        ).build()
  }

  companion object {
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private val JOB_UUID: UUID = UUID.randomUUID()
    private const val JOB_ID = 11L
    private const val ATTEMPT_ID = 21
    private const val UI_COMMANDS_TASK_QUEUE = "ui_commands"
    private val JOB_RUN_CONFIG: JobRunConfig =
      JobRunConfig()
        .withJobId(JOB_ID.toString())
        .withAttemptId(ATTEMPT_ID.toLong())
    private const val IMAGE_NAME1 = "hms invincible"
    private const val NAMESPACE = "namespace"
    private val STREAM_DESCRIPTOR: StreamDescriptor = StreamDescriptor().withName("name")
    private const val EXCEPTION_MESSAGE = "Force state exception to simulate workflow not running"
  }
}
