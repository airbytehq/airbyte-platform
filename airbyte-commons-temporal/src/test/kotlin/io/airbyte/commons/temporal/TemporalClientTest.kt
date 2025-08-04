/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import com.google.common.collect.Sets
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.logging.DEFAULT_LOG_FILENAME
import io.airbyte.commons.temporal.config.TemporalQueueConfiguration
import io.airbyte.commons.temporal.exception.DeletedWorkflowException
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
import io.airbyte.config.secrets.buildConfigWithSecretRefsJava
import io.airbyte.data.services.ScopedConfigurationService
import io.airbyte.featureflag.TestClient
import io.airbyte.metrics.MetricClient
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
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
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.capture
import org.mockito.kotlin.eq
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import java.util.function.Supplier

/**
 * Setup for Temporal Client test suite.
 */
class TemporalClientTest {
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
  @Throws(IOException::class)
  fun setup() {
    workspaceRoot = Files.createTempDirectory(Path.of("/tmp"), "temporal_client_test")
    logPath = workspaceRoot.resolve(JOB_ID.toString()).resolve(ATTEMPT_ID.toString()).resolve(DEFAULT_LOG_FILENAME)
    workflowClient = Mockito.mock(WorkflowClient::class.java)
    Mockito
      .`when`(workflowClient.getOptions())
      .thenReturn(WorkflowClientOptions.newBuilder().setNamespace(NAMESPACE).build())
    workflowServiceStubs = Mockito.mock(WorkflowServiceStubs::class.java)
    Mockito.`when`(workflowClient.getWorkflowServiceStubs()).thenReturn(workflowServiceStubs)
    Mockito
      .`when`(workflowClient.signalWithStart(any<BatchRequest>()))
      .thenReturn(WorkflowExecution.newBuilder().build())
    workflowServiceBlockingStub = Mockito.mock(WorkflowServiceBlockingStub::class.java)
    Mockito.`when`(workflowServiceStubs.blockingStub()).thenReturn(workflowServiceBlockingStub)
    streamResetPersistence = Mockito.mock(StreamResetPersistence::class.java)
    streamRefreshesRepository = Mockito.mock(StreamRefreshesRepository::class.java)
    mockWorkflowStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING)
    val metricClient = Mockito.mock(MetricClient::class.java)
    val workflowClientWrapped = WorkflowClientWrapped(workflowClient, metricClient)
    val workflowServiceStubsWrapped = WorkflowServiceStubsWrapped(workflowServiceStubs, metricClient)
    val scopedConfigurationService = Mockito.mock(ScopedConfigurationService::class.java)
    connectionManagerUtils = Mockito.spy(ConnectionManagerUtils(workflowClientWrapped, metricClient))
    streamResetRecordsHelper = Mockito.mock(StreamResetRecordsHelper::class.java)
    temporalClient =
      Mockito.spy(
        TemporalClient(
          workspaceRoot,
          TemporalQueueConfiguration(),
          workflowClientWrapped,
          workflowServiceStubsWrapped,
          streamResetPersistence,
          streamRefreshesRepository,
          connectionManagerUtils,
          streamResetRecordsHelper,
          Mockito.mock(MetricClient::class.java),
          TestClient(),
          scopedConfigurationService,
        ),
      )
  }

  @Nested
  internal inner class RestartPerStatus {
    private lateinit var mConnectionManagerUtils: ConnectionManagerUtils

    @BeforeEach
    fun init() {
      mConnectionManagerUtils = Mockito.mock(ConnectionManagerUtils::class.java)

      val metricClient = Mockito.mock(MetricClient::class.java)
      val scopedConfigurationService = Mockito.mock(ScopedConfigurationService::class.java)
      temporalClient =
        Mockito.spy(
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
            TestClient(),
            scopedConfigurationService,
          ),
        )
    }

    @Test
    fun testRestartFailed() {
      val mConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)

      Mockito
        .`when`(workflowClient.newWorkflowStub(ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(), ArgumentMatchers.anyString()))
        .thenReturn(mConnectionManagerWorkflow)
      val connectionId = UUID.fromString("ebbfdc4c-295b-48a0-844f-88551dfad3db")
      val workflowIds = mutableSetOf<UUID>(connectionId)

      Mockito
        .doReturn(workflowIds)
        .`when`<TemporalClient?>(temporalClient)
        .fetchClosedWorkflowsByStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED)
      Mockito
        .doReturn(workflowIds)
        .`when`<TemporalClient?>(temporalClient)
        .filterOutRunningWorkspaceId(workflowIds)
      mockWorkflowStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED)
      temporalClient.restartClosedWorkflowByStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED)
      Mockito
        .verify(mConnectionManagerUtils)
        .safeTerminateWorkflow(eq(connectionId), any())
      Mockito.verify(mConnectionManagerUtils).startConnectionManagerNoSignal(eq(connectionId))
    }
  }

  @Nested
  @DisplayName("Test execute method.")
  internal inner class ExecuteJob {
    @Test
    fun testExecute() {
      val supplier = Mockito.mock(Supplier::class.java) as Supplier<String>
      Mockito.`when`(supplier.get()).thenReturn("hello")

      val response = temporalClient.execute<String>(JOB_RUN_CONFIG, supplier)

      Assertions.assertNotNull(response)
      Assertions.assertTrue(response.getOutput().isPresent())
      Assertions.assertEquals("hello", response.getOutput().get())
      Assertions.assertTrue(response.metadata.succeeded)
      Assertions.assertEquals(logPath, response.metadata.logPath)
    }

    @Test
    fun testExecuteWithException() {
      val supplier = Mockito.mock(Supplier::class.java) as Supplier<String>
      Mockito.`when`(supplier.get()).thenThrow(IllegalStateException::class.java)

      val response = temporalClient.execute<String>(JOB_RUN_CONFIG, supplier)

      Assertions.assertNotNull(response)
      Assertions.assertFalse(response.getOutput().isPresent())
      Assertions.assertFalse(response.metadata.succeeded)
      Assertions.assertEquals(logPath, response.metadata.logPath)
    }

    @Test
    fun testExecuteWithConnectorJobFailure() {
      val supplier = Mockito.mock(Supplier::class.java) as Supplier<ConnectorJobOutput>
      val mockFailureReason = Mockito.mock(FailureReason::class.java)
      val connectorJobOutput =
        ConnectorJobOutput()
          .withFailureReason(mockFailureReason)
      Mockito.`when`(supplier.get()).thenReturn(connectorJobOutput)

      val response = temporalClient.execute<ConnectorJobOutput>(JOB_RUN_CONFIG, supplier)

      Assertions.assertNotNull(response)
      Assertions.assertTrue(response.getOutput().isPresent())
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
      val specWorkflow = Mockito.mock(ConnectorCommandWorkflow::class.java)
      Mockito
        .`when`(
          workflowClient.newWorkflowStub(
            eq(ConnectorCommandWorkflow::class.java),
            any<WorkflowOptions>(),
          ),
        ).thenReturn(specWorkflow)
      val getSpecConfig = JobGetSpecConfig().withDockerImage(IMAGE_NAME1)

      temporalClient.submitGetSpec(JOB_UUID, ATTEMPT_ID, WORKSPACE_ID, getSpecConfig)

      val workflowClassCaptor = ArgumentCaptor.forClass(Class::class.java)
      val workflowOptionsCaptor = ArgumentCaptor.forClass(WorkflowOptions::class.java)
      Mockito.verify(workflowClient).newWorkflowStub(
        capture(workflowClassCaptor),
        capture(workflowOptionsCaptor),
      )
      Assertions.assertEquals(ConnectorCommandWorkflow::class.java, workflowClassCaptor.getValue())
      Assertions.assertEquals(UI_COMMANDS_TASK_QUEUE, workflowOptionsCaptor.getValue()!!.getTaskQueue())

      val connectorCommandInputCaptor = argumentCaptor<ConnectorCommandInput>()
      Mockito.verify(specWorkflow).run(connectorCommandInputCaptor.capture())
      Assertions.assertInstanceOf(SpecCommandInput::class.java, connectorCommandInputCaptor.firstValue)
    }

    @Test
    fun testSubmitCheckConnection() {
      val checkConnectionWorkflow = Mockito.mock(ConnectorCommandWorkflow::class.java)
      Mockito
        .`when`(
          workflowClient.newWorkflowStub(
            eq(ConnectorCommandWorkflow::class.java),
            any<WorkflowOptions>(),
          ),
        ).thenReturn(checkConnectionWorkflow)
      val checkConnectionConfig =
        JobCheckConnectionConfig()
          .withDockerImage(IMAGE_NAME1)
          .withConnectionConfiguration(buildConfigWithSecretRefsJava(emptyObject()))

      temporalClient.submitCheckConnection(JOB_UUID, ATTEMPT_ID, WORKSPACE_ID, CHECK_TASK_QUEUE, checkConnectionConfig, ActorContext())

      val workflowClassCaptor = ArgumentCaptor.forClass(Class::class.java)
      val workflowOptionsCaptor = ArgumentCaptor.forClass(WorkflowOptions::class.java)
      Mockito.verify(workflowClient).newWorkflowStub(
        capture(workflowClassCaptor),
        capture(workflowOptionsCaptor),
      )
      Assertions.assertEquals(ConnectorCommandWorkflow::class.java, workflowClassCaptor.getValue())
      Assertions.assertEquals(UI_COMMANDS_TASK_QUEUE, workflowOptionsCaptor.getValue()!!.getTaskQueue())

      val connectorCommandInputCaptor = argumentCaptor<ConnectorCommandInput>()
      Mockito.verify(checkConnectionWorkflow).run(connectorCommandInputCaptor.capture())
      Assertions.assertInstanceOf(CheckCommandInput::class.java, connectorCommandInputCaptor.firstValue)
    }

    @Test
    fun testSubmitDiscoverSchema() {
      val discoverCatalogWorkflow = Mockito.mock(ConnectorCommandWorkflow::class.java)
      Mockito
        .`when`(
          workflowClient.newWorkflowStub(
            eq(ConnectorCommandWorkflow::class.java),
            any<WorkflowOptions>(),
          ),
        ).thenReturn(discoverCatalogWorkflow)
      val checkConnectionConfig =
        JobDiscoverCatalogConfig()
          .withDockerImage(IMAGE_NAME1)
          .withConnectionConfiguration(buildConfigWithSecretRefsJava(emptyObject()))

      temporalClient.submitDiscoverSchema(
        JOB_UUID,
        ATTEMPT_ID,
        WORKSPACE_ID,
        DISCOVER_TASK_QUEUE,
        checkConnectionConfig,
        ActorContext(),
        WorkloadPriority.DEFAULT,
      )

      val workflowClassCaptor = ArgumentCaptor.forClass(Class::class.java)
      val workflowOptionsCaptor = ArgumentCaptor.forClass(WorkflowOptions::class.java)
      Mockito.verify(workflowClient).newWorkflowStub(
        capture(workflowClassCaptor),
        capture(workflowOptionsCaptor),
      )
      Assertions.assertEquals(ConnectorCommandWorkflow::class.java, workflowClassCaptor.getValue())
      Assertions.assertEquals(UI_COMMANDS_TASK_QUEUE, workflowOptionsCaptor.getValue()!!.getTaskQueue())

      val connectorCommandInputCaptor = argumentCaptor<ConnectorCommandInput>()
      Mockito.verify(discoverCatalogWorkflow).run(connectorCommandInputCaptor.capture())
      Assertions.assertInstanceOf(DiscoverCommandInput::class.java, connectorCommandInputCaptor.firstValue)
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

      Mockito
        .`when`(temporalClient.isInRunningWorkflowCache(connectionManagerUtils.getConnectionManagerName(nonMigratedId)))
        .thenReturn(false)
      Mockito
        .`when`(temporalClient.isInRunningWorkflowCache(connectionManagerUtils.getConnectionManagerName(migratedId)))
        .thenReturn(true)

      Mockito
        .doNothing()
        .`when`(temporalClient)
        .refreshRunningWorkflow()
      val mConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
      Mockito
        .doReturn(mConnectionManagerWorkflow)
        .`when`(temporalClient)
        .submitConnectionUpdaterAsync(nonMigratedId)

      temporalClient.migrateSyncIfNeeded(Sets.newHashSet(nonMigratedId, migratedId))

      Mockito.verify(temporalClient, Mockito.times(1)).submitConnectionUpdaterAsync(nonMigratedId)
      Mockito.verify(temporalClient, Mockito.times(0)).submitConnectionUpdaterAsync(migratedId)
    }
  }

  @Nested
  @DisplayName("Test delete connection method.")
  internal inner class ForceCancelConnection {
    @Test
    @DisplayName("Forcing a workflow deletion delegates to the connection manager.")
    fun testForceCancelConnection() {
      temporalClient.forceDeleteWorkflow(CONNECTION_ID)

      Mockito.verify(connectionManagerUtils).deleteWorkflowIfItExist(CONNECTION_ID)
    }
  }

  @Nested
  @DisplayName("Test update connection behavior")
  internal inner class UpdateConnection {
    @Test
    @DisplayName("Test update connection when workflow is running")
    fun testUpdateConnection() {
      val mConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
      val mWorkflowState = Mockito.mock(WorkflowState::class.java)

      Mockito.`when`(mWorkflowState.isRunning).thenReturn(true)
      Mockito.`when`(mWorkflowState.isDeleted).thenReturn(false)
      Mockito.`when`(mConnectionManagerWorkflow.getState()).thenReturn(mWorkflowState)
      Mockito
        .`when`(
          workflowClient.newWorkflowStub(
            ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(),
            ArgumentMatchers.anyString(),
          ),
        ).thenReturn(mConnectionManagerWorkflow)

      temporalClient.update(CONNECTION_ID)

      Mockito.verify(mConnectionManagerWorkflow, Mockito.times(1)).connectionUpdated()
    }

    @Test
    @DisplayName("Test update connection method starts a new workflow when workflow is in an unexpected state")
    fun testUpdateConnectionInUnexpectedState() {
      val mConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)

      Mockito.`when`(mConnectionManagerWorkflow.getState()).thenThrow(IllegalStateException(EXCEPTION_MESSAGE))
      Mockito
        .`when`(
          workflowClient.newWorkflowStub(
            ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(),
            ArgumentMatchers.anyString(),
          ),
        ).thenReturn(mConnectionManagerWorkflow)
      Mockito.doReturn(mConnectionManagerWorkflow).`when`(temporalClient).submitConnectionUpdaterAsync(CONNECTION_ID)

      val untypedWorkflowStub = Mockito.mock(WorkflowStub::class.java)
      Mockito.`when`(workflowClient.newUntypedWorkflowStub(ArgumentMatchers.anyString())).thenReturn(untypedWorkflowStub)

      temporalClient.update(CONNECTION_ID)

      // this is only called when updating an existing workflow
      Mockito.verify(mConnectionManagerWorkflow, Mockito.never()).connectionUpdated()

      Mockito.verify(untypedWorkflowStub, Mockito.times(1)).terminate(ArgumentMatchers.anyString())
      Mockito.verify(temporalClient, Mockito.times(1)).submitConnectionUpdaterAsync(CONNECTION_ID)
    }

    @Test
    @DisplayName("Test update connection method does nothing when connection is deleted")
    fun testUpdateConnectionDeletedWorkflow() {
      val mConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
      val mWorkflowState = Mockito.mock(WorkflowState::class.java)
      Mockito.`when`(mConnectionManagerWorkflow.getState()).thenReturn(mWorkflowState)
      Mockito.`when`(mWorkflowState.isDeleted).thenReturn(true)
      Mockito
        .`when`(workflowClient.newWorkflowStub(ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(), ArgumentMatchers.anyString()))
        .thenReturn(mConnectionManagerWorkflow)
      mockWorkflowStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)

      temporalClient.update(CONNECTION_ID)

      // this is only called when updating an existing workflow
      Mockito.verify(mConnectionManagerWorkflow, Mockito.never()).connectionUpdated()
      Mockito.verify(temporalClient).update(CONNECTION_ID)
      Mockito.verifyNoMoreInteractions(temporalClient)
    }
  }

  @Nested
  @DisplayName("Test manual sync behavior")
  internal inner class ManualSync {
    @Test
    @DisplayName("Test startNewManualSync successful")
    fun testStartNewManualSyncSuccess() {
      val mConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
      val mWorkflowState = Mockito.mock(WorkflowState::class.java)
      Mockito.`when`(mConnectionManagerWorkflow.getState()).thenReturn(mWorkflowState)
      Mockito.`when`(mWorkflowState.isDeleted).thenReturn(false)
      Mockito.`when`(mWorkflowState.isRunning).thenReturn(false).thenReturn(true)
      Mockito.`when`(mConnectionManagerWorkflow.getJobInformation()).thenReturn(JobInformation(JOB_ID, ATTEMPT_ID))
      Mockito
        .`when`(workflowClient.newWorkflowStub(ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(), ArgumentMatchers.anyString()))
        .thenReturn(mConnectionManagerWorkflow)

      val result = temporalClient.startNewManualSync(CONNECTION_ID)

      Assertions.assertNotNull(result.jobId)
      Assertions.assertEquals(JOB_ID, result.jobId)
      Assertions.assertNull(result.failingReason)
      Mockito.verify(mConnectionManagerWorkflow).submitManualSync()
    }

    @Test
    @DisplayName("Test startNewManualSync fails if job is already running")
    fun testStartNewManualSyncAlreadyRunning() {
      val mConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
      val mWorkflowState = Mockito.mock(WorkflowState::class.java)
      Mockito.`when`(mConnectionManagerWorkflow.getState()).thenReturn(mWorkflowState)
      Mockito.`when`(mWorkflowState.isDeleted).thenReturn(false)
      Mockito.`when`(mWorkflowState.isRunning).thenReturn(true)
      Mockito.`when`(mConnectionManagerWorkflow.getJobInformation()).thenReturn(JobInformation(JOB_ID, ATTEMPT_ID))
      Mockito
        .`when`(workflowClient.newWorkflowStub(ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(), ArgumentMatchers.anyString()))
        .thenReturn(mConnectionManagerWorkflow)

      val result = temporalClient.startNewManualSync(CONNECTION_ID)

      Assertions.assertNull(result.jobId)
      Assertions.assertNotNull(result.failingReason)
      Mockito.verify(mConnectionManagerWorkflow, Mockito.times(0)).submitManualSync()
    }

    @Test
    @DisplayName("Test startNewManualSync repairs the workflow if it is in a bad state")
    fun testStartNewManualSyncRepairsBadWorkflowState() {
      val mTerminatedConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)

      // This simulates a workflow that is in a bad state.
      Mockito.`when`(mTerminatedConnectionManagerWorkflow.getState()).thenThrow(IllegalStateException(EXCEPTION_MESSAGE))
      Mockito.`when`(mTerminatedConnectionManagerWorkflow.getJobInformation()).thenReturn(JobInformation(JOB_ID, ATTEMPT_ID))

      val mNewConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
      val mWorkflowState = Mockito.mock(WorkflowState::class.java)
      Mockito.`when`(mNewConnectionManagerWorkflow.getState()).thenReturn(mWorkflowState)
      Mockito.`when`(mWorkflowState.isDeleted).thenReturn(false)
      Mockito.`when`(mWorkflowState.isRunning).thenReturn(false).thenReturn(true)
      Mockito.`when`(mNewConnectionManagerWorkflow.getJobInformation()).thenReturn(JobInformation(JOB_ID, ATTEMPT_ID))
      Mockito
        .`when`(
          workflowClient.newWorkflowStub(
            ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(),
            any<WorkflowOptions>(),
          ),
        ).thenReturn(mNewConnectionManagerWorkflow)
      val mBatchRequest = Mockito.mock(BatchRequest::class.java)
      Mockito.`when`(workflowClient.newSignalWithStartRequest()).thenReturn(mBatchRequest)

      Mockito
        .`when`(workflowClient.newWorkflowStub(ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(), ArgumentMatchers.anyString()))
        .thenReturn(
          mTerminatedConnectionManagerWorkflow,
          mTerminatedConnectionManagerWorkflow,
          mNewConnectionManagerWorkflow,
        )

      val result = temporalClient.startNewManualSync(CONNECTION_ID)

      Assertions.assertNotNull(result.jobId)
      Assertions.assertEquals(JOB_ID, result.jobId)
      Assertions.assertNull(result.failingReason)
      Mockito.verify(workflowClient).signalWithStart(mBatchRequest)

      // Verify that the submitManualSync signal was passed to the batch request by capturing the
      // argument,
      // executing the signal, and verifying that the desired signal was executed
      val batchRequestAddArgCaptor = ArgumentCaptor.forClass(Functions.Proc::class.java)
      Mockito.verify(mBatchRequest).add(capture(batchRequestAddArgCaptor))
      val signal = batchRequestAddArgCaptor.getValue()
      signal.apply()
      Mockito.verify(mNewConnectionManagerWorkflow).submitManualSync()
    }

    @Test
    @DisplayName("Test startNewManualSync returns a failure reason when connection is deleted")
    fun testStartNewManualSyncDeletedWorkflow() {
      val mConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
      val mWorkflowState = Mockito.mock(WorkflowState::class.java)
      Mockito.`when`(mConnectionManagerWorkflow.getState()).thenReturn(mWorkflowState)
      Mockito.`when`(mWorkflowState.isDeleted).thenReturn(true)
      Mockito
        .`when`(workflowClient.newWorkflowStub(ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(), ArgumentMatchers.anyString()))
        .thenReturn(mConnectionManagerWorkflow)
      mockWorkflowStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)

      val result = temporalClient.startNewManualSync(CONNECTION_ID)

      // this is only called when updating an existing workflow
      Assertions.assertNull(result.jobId)
      Assertions.assertNotNull(result.failingReason)
      Mockito.verify(mConnectionManagerWorkflow, Mockito.times(0)).submitManualSync()
    }
  }

  @Nested
  @DisplayName("Test cancellation behavior")
  internal inner class Cancellation {
    @Test
    @DisplayName("Test startNewCancellation successful")
    fun testStartNewCancellationSuccess() {
      val mConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
      val mWorkflowState = Mockito.mock(WorkflowState::class.java)
      Mockito.`when`(mConnectionManagerWorkflow.getState()).thenReturn(mWorkflowState)
      Mockito.`when`(mWorkflowState.isDeleted).thenReturn(false)
      Mockito.`when`(mWorkflowState.isRunning).thenReturn(true).thenReturn(false)
      Mockito.`when`(mConnectionManagerWorkflow.getJobInformation()).thenReturn(JobInformation(JOB_ID, ATTEMPT_ID))
      Mockito
        .`when`(workflowClient.newWorkflowStub(ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(), ArgumentMatchers.anyString()))
        .thenReturn(mConnectionManagerWorkflow)

      val result = temporalClient.startNewCancellation(CONNECTION_ID)

      Assertions.assertNotNull(result.jobId)
      Assertions.assertEquals(JOB_ID, result.jobId)
      Assertions.assertNull(result.failingReason)
      Mockito.verify(mConnectionManagerWorkflow).cancelJob()
      Mockito.verify(streamResetRecordsHelper).deleteStreamResetRecordsForJob(JOB_ID, CONNECTION_ID)
    }

    @Test
    @DisplayName("Test startNewCancellation repairs the workflow if it is in a bad state")
    fun testStartNewCancellationRepairsBadWorkflowState() {
      val mTerminatedConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
      Mockito
        .`when`(mTerminatedConnectionManagerWorkflow.getState())
        .thenThrow(IllegalStateException(EXCEPTION_MESSAGE))
      Mockito.`when`(mTerminatedConnectionManagerWorkflow.getJobInformation()).thenReturn(JobInformation(JOB_ID, ATTEMPT_ID))

      val mNewConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
      val mWorkflowState = Mockito.mock(WorkflowState::class.java)
      Mockito.`when`(mNewConnectionManagerWorkflow.getState()).thenReturn(mWorkflowState)
      Mockito.`when`(mWorkflowState.isDeleted).thenReturn(false)
      Mockito.`when`(mWorkflowState.isRunning).thenReturn(true).thenReturn(false)
      Mockito.`when`(mNewConnectionManagerWorkflow.getJobInformation()).thenReturn(JobInformation(JOB_ID, ATTEMPT_ID))
      Mockito
        .`when`(
          workflowClient.newWorkflowStub(
            ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(),
            any<WorkflowOptions>(),
          ),
        ).thenReturn(mNewConnectionManagerWorkflow)
      val mBatchRequest = Mockito.mock(BatchRequest::class.java)
      Mockito.`when`(workflowClient.newSignalWithStartRequest()).thenReturn(mBatchRequest)

      Mockito
        .`when`(workflowClient.newWorkflowStub(ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(), ArgumentMatchers.anyString()))
        .thenReturn(
          mTerminatedConnectionManagerWorkflow,
          mTerminatedConnectionManagerWorkflow,
          mNewConnectionManagerWorkflow,
        )

      val result = temporalClient.startNewCancellation(CONNECTION_ID)

      Assertions.assertNotNull(result.jobId)
      Assertions.assertEquals(ConnectionManagerWorkflow.Companion.NON_RUNNING_JOB_ID, result.jobId)
      Assertions.assertNull(result.failingReason)
      Mockito.verify(workflowClient).signalWithStart(mBatchRequest)

      // Verify that the cancelJob signal was passed to the batch request by capturing the argument,
      // executing the signal, and verifying that the desired signal was executed
      val batchRequestAddArgCaptor = ArgumentCaptor.forClass(Functions.Proc::class.java)
      Mockito.verify(mBatchRequest).add(capture(batchRequestAddArgCaptor))
      val signal = batchRequestAddArgCaptor.getValue()
      signal.apply()
      Mockito.verify(mNewConnectionManagerWorkflow).cancelJob()
    }

    @Test
    @DisplayName("Test startNewCancellation returns a failure reason when connection is deleted")
    fun testStartNewCancellationDeletedWorkflow() {
      val mConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
      val mWorkflowState = Mockito.mock(WorkflowState::class.java)
      Mockito.`when`(mConnectionManagerWorkflow.getState()).thenReturn(mWorkflowState)
      Mockito.`when`(mWorkflowState.isDeleted).thenReturn(true)
      Mockito
        .`when`(workflowClient.newWorkflowStub(ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(), ArgumentMatchers.anyString()))
        .thenReturn(mConnectionManagerWorkflow)
      mockWorkflowStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)

      val result = temporalClient.startNewCancellation(CONNECTION_ID)

      // this is only called when updating an existing workflow
      Assertions.assertNull(result.jobId)
      Assertions.assertNotNull(result.failingReason)
      Mockito.verify(mConnectionManagerWorkflow, Mockito.times(0)).cancelJob()
    }
  }

  @Nested
  @DisplayName("Test refresh connection behavior")
  internal inner class RefreshConnection {
    @Test
    @DisplayName("Test refreshConnectionAsync saves the stream to refresh and signals workflow")
    @Throws(DeletedWorkflowException::class)
    fun testRefreshConnectionAsyncHappyPath() {
      val mConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
      val mWorkflowState = Mockito.mock(WorkflowState::class.java)
      Mockito.`when`(mConnectionManagerWorkflow.getState()).thenReturn(mWorkflowState)
      Mockito.`when`(mWorkflowState.isDeleted).thenReturn(false)
      Mockito.`when`(mWorkflowState.isRunning).thenReturn(false)
      val jobId1: Long = 1
      val jobId2: Long = 2
      Mockito.`when`(mConnectionManagerWorkflow.getJobInformation()).thenReturn(
        JobInformation(jobId1, 0),
        JobInformation(jobId1, 0),
        JobInformation(ConnectionManagerWorkflow.Companion.NON_RUNNING_JOB_ID, 0),
        JobInformation(ConnectionManagerWorkflow.Companion.NON_RUNNING_JOB_ID, 0),
        JobInformation(jobId2, 0),
        JobInformation(jobId2, 0),
      )
      Mockito
        .`when`(
          workflowClient.newWorkflowStub(
            ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(),
            ArgumentMatchers.anyString(),
          ),
        ).thenReturn(mConnectionManagerWorkflow)

      val streamsToReset = listOf(STREAM_DESCRIPTOR)
      val refreshType = RefreshStream.RefreshType.TRUNCATE
      temporalClient.refreshConnectionAsync(CONNECTION_ID, streamsToReset, refreshType)

      Mockito.verify(streamRefreshesRepository).saveAll(
        ArgumentMatchers.any<Iterable<StreamRefresh>>(),
      )

      Mockito.verify(connectionManagerUtils).signalWorkflowAndRepairIfNecessary(
        eq(CONNECTION_ID),
        any(),
      )
    }
  }

  @Nested
  @DisplayName("Test reset connection behavior")
  internal inner class ResetConnection {
    @Test
    @DisplayName("Test resetConnection successful")
    @Throws(IOException::class)
    fun testResetConnectionSuccess() {
      val mConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
      val mWorkflowState = Mockito.mock(WorkflowState::class.java)
      Mockito.`when`(mConnectionManagerWorkflow.getState()).thenReturn(mWorkflowState)
      Mockito.`when`(mWorkflowState.isDeleted).thenReturn(false)
      Mockito.`when`(mWorkflowState.isRunning).thenReturn(false)
      val jobId1: Long = 1
      val jobId2: Long = 2
      Mockito.`when`(mConnectionManagerWorkflow.getJobInformation()).thenReturn(
        JobInformation(jobId1, 0),
        JobInformation(jobId1, 0),
        JobInformation(ConnectionManagerWorkflow.Companion.NON_RUNNING_JOB_ID, 0),
        JobInformation(ConnectionManagerWorkflow.Companion.NON_RUNNING_JOB_ID, 0),
        JobInformation(jobId2, 0),
        JobInformation(jobId2, 0),
      )
      Mockito
        .`when`(workflowClient.newWorkflowStub(ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(), ArgumentMatchers.anyString()))
        .thenReturn(mConnectionManagerWorkflow)

      val streamsToReset = listOf(STREAM_DESCRIPTOR)
      val result = temporalClient.resetConnection(CONNECTION_ID, streamsToReset)

      Mockito.verify(streamResetPersistence).createStreamResets(CONNECTION_ID, streamsToReset)

      Assertions.assertNotNull(result.jobId)
      Assertions.assertEquals(jobId2, result.jobId)
      Assertions.assertNull(result.failingReason)
      Mockito.verify(mConnectionManagerWorkflow).resetConnection()
    }

    @Test
    @DisplayName("Test resetConnection repairs the workflow if it is in a bad state")
    @Throws(IOException::class)
    fun testResetConnectionRepairsBadWorkflowState() {
      val mTerminatedConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)

      // This simulates a workflow that is in a bad state.
      Mockito.`when`(mTerminatedConnectionManagerWorkflow.getState()).thenThrow(IllegalStateException(EXCEPTION_MESSAGE))
      Mockito.`when`(mTerminatedConnectionManagerWorkflow.getJobInformation()).thenReturn(JobInformation(JOB_ID, ATTEMPT_ID))

      val mNewConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
      val mWorkflowState = Mockito.mock(WorkflowState::class.java)
      Mockito.`when`(mNewConnectionManagerWorkflow.getState()).thenReturn(mWorkflowState)
      Mockito.`when`(mWorkflowState.isDeleted).thenReturn(false)
      Mockito.`when`(mWorkflowState.isRunning).thenReturn(false)
      Mockito.`when`(mNewConnectionManagerWorkflow.getJobInformation()).thenReturn(
        JobInformation(ConnectionManagerWorkflow.Companion.NON_RUNNING_JOB_ID, 0),
        JobInformation(ConnectionManagerWorkflow.Companion.NON_RUNNING_JOB_ID, 0),
        JobInformation(JOB_ID, 0),
        JobInformation(JOB_ID, 0),
      )
      Mockito
        .`when`(
          workflowClient.newWorkflowStub(
            ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(),
            any<WorkflowOptions>(),
          ),
        ).thenReturn(mNewConnectionManagerWorkflow)
      val mBatchRequest = Mockito.mock(BatchRequest::class.java)
      Mockito.`when`(workflowClient.newSignalWithStartRequest()).thenReturn(mBatchRequest)

      Mockito
        .`when`(workflowClient.newWorkflowStub(ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(), ArgumentMatchers.anyString()))
        .thenReturn(
          mTerminatedConnectionManagerWorkflow,
          mTerminatedConnectionManagerWorkflow,
          mNewConnectionManagerWorkflow,
        )

      val streamsToReset = listOf(STREAM_DESCRIPTOR)
      val result = temporalClient.resetConnection(CONNECTION_ID, streamsToReset)

      Mockito.verify(streamResetPersistence).createStreamResets(CONNECTION_ID, streamsToReset)

      Assertions.assertNotNull(result.jobId)
      Assertions.assertEquals(JOB_ID, result.jobId)
      Assertions.assertNull(result.failingReason)
      Mockito.verify(workflowClient).signalWithStart(mBatchRequest)

      // Verify that the resetConnection signal was passed to the batch request by capturing the argument,
      // executing the signal, and verifying that the desired signal was executed
      val batchRequestAddArgCaptor = ArgumentCaptor.forClass(Functions.Proc::class.java)
      Mockito.verify(mBatchRequest).add(capture(batchRequestAddArgCaptor))
      val signal = batchRequestAddArgCaptor.getValue()
      signal.apply()
      Mockito.verify(mNewConnectionManagerWorkflow).resetConnection()
    }

    @Test
    @DisplayName("Test resetConnection returns a failure reason when connection is deleted")
    @Throws(IOException::class)
    fun testResetConnectionDeletedWorkflow() {
      val mConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
      val mWorkflowState = Mockito.mock(WorkflowState::class.java)
      Mockito.`when`(mConnectionManagerWorkflow.getState()).thenReturn(mWorkflowState)
      Mockito.`when`(mWorkflowState.isDeleted).thenReturn(true)
      Mockito
        .`when`(workflowClient.newWorkflowStub(ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(), ArgumentMatchers.anyString()))
        .thenReturn(mConnectionManagerWorkflow)
      mockWorkflowStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED)

      val streamsToReset = listOf(STREAM_DESCRIPTOR)
      val result = temporalClient.resetConnection(CONNECTION_ID, streamsToReset)

      Mockito.verify(streamResetPersistence).createStreamResets(CONNECTION_ID, streamsToReset)

      // this is only called when updating an existing workflow
      Assertions.assertNull(result.jobId)
      Assertions.assertNotNull(result.failingReason)
      Mockito.verify(mConnectionManagerWorkflow, Mockito.times(0)).resetConnection()
    }
  }

  @Test
  @DisplayName("Test manual operation on completed workflow causes a restart")
  fun testManualOperationOnCompletedWorkflow() {
    val mConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
    val mWorkflowState = Mockito.mock(WorkflowState::class.java)
    Mockito.`when`(mConnectionManagerWorkflow.getState()).thenReturn(mWorkflowState)
    Mockito.`when`(mWorkflowState.isDeleted).thenReturn(false)
    Mockito
      .`when`(
        workflowServiceBlockingStub.describeWorkflowExecution(
          ArgumentMatchers.any<DescribeWorkflowExecutionRequest>(),
        ),
      ).thenReturn(
        DescribeWorkflowExecutionResponse
          .newBuilder()
          .setWorkflowExecutionInfo(
            WorkflowExecutionInfo.newBuilder().setStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED).buildPartial(),
          ).build(),
      ).thenReturn(
        DescribeWorkflowExecutionResponse
          .newBuilder()
          .setWorkflowExecutionInfo(
            WorkflowExecutionInfo.newBuilder().setStatus(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING).buildPartial(),
          ).build(),
      )

    val mNewConnectionManagerWorkflow = Mockito.mock(ConnectionManagerWorkflow::class.java)
    val mNewWorkflowState = Mockito.mock(WorkflowState::class.java)
    Mockito.`when`(mNewConnectionManagerWorkflow.getState()).thenReturn(mNewWorkflowState)
    Mockito.`when`(mNewWorkflowState.isRunning).thenReturn(false).thenReturn(true)
    Mockito
      .`when`(mNewConnectionManagerWorkflow.getJobInformation())
      .thenReturn(JobInformation(JOB_ID, ATTEMPT_ID))
    Mockito
      .`when`(
        workflowClient.newWorkflowStub(any<Class<ConnectionManagerWorkflow>>(), any<WorkflowOptions>()),
      ).thenReturn(mNewConnectionManagerWorkflow)
    val mBatchRequest = Mockito.mock(BatchRequest::class.java)
    Mockito.`when`(workflowClient.newSignalWithStartRequest()).thenReturn(mBatchRequest)

    Mockito
      .`when`(
        workflowClient.newWorkflowStub(
          ArgumentMatchers.any<Class<ConnectionManagerWorkflow>>(),
          ArgumentMatchers.anyString(),
        ),
      ).thenReturn(
        mConnectionManagerWorkflow,
        mConnectionManagerWorkflow,
        mNewConnectionManagerWorkflow,
      )

    val mWorkflowStub = Mockito.mock(WorkflowStub::class.java)
    Mockito.`when`(workflowClient.newUntypedWorkflowStub(ArgumentMatchers.anyString())).thenReturn(mWorkflowStub)

    val result = temporalClient.startNewManualSync(CONNECTION_ID)

    Assertions.assertNotNull(result.jobId)
    Assertions.assertEquals(JOB_ID, result.jobId)
    Assertions.assertNull(result.failingReason)
    Mockito.verify(workflowClient).signalWithStart(mBatchRequest)
    Mockito.verify(mWorkflowStub).terminate(ArgumentMatchers.anyString())

    // Verify that the submitManualSync signal was passed to the batch request by capturing the
    // argument,
    // executing the signal, and verifying that the desired signal was executed
    val batchRequestAddArgCaptor = ArgumentCaptor.forClass(Functions.Proc::class.java)
    Mockito.verify(mBatchRequest).add(batchRequestAddArgCaptor.capture())
    val signal = batchRequestAddArgCaptor.getValue()
    signal.apply()
    Mockito.verify(mNewConnectionManagerWorkflow).submitManualSync()
  }

  private fun mockWorkflowStatus(status: WorkflowExecutionStatus) {
    Mockito
      .`when`(
        workflowServiceBlockingStub.describeWorkflowExecution(
          ArgumentMatchers.any<DescribeWorkflowExecutionRequest>(),
        ),
      ).thenReturn(
        DescribeWorkflowExecutionResponse
          .newBuilder()
          .setWorkflowExecutionInfo(
            WorkflowExecutionInfo.newBuilder().setStatus(status).buildPartial(),
          ).build(),
      )
  }

  companion object {
    private val WORKSPACE_ID: UUID = UUID.randomUUID()
    private val CONNECTION_ID: UUID = UUID.randomUUID()
    private val JOB_UUID: UUID = UUID.randomUUID()
    private const val JOB_ID = 11L
    private const val ATTEMPT_ID = 21

    private const val CHECK_TASK_QUEUE = "CHECK_CONNECTION"
    private const val DISCOVER_TASK_QUEUE = "DISCOVER_SCHEMA"
    private const val UI_COMMANDS_TASK_QUEUE = "ui_commands"
    private val JOB_RUN_CONFIG: JobRunConfig =
      JobRunConfig()
        .withJobId(JOB_ID.toString())
        .withAttemptId(ATTEMPT_ID.toLong())
    private const val IMAGE_NAME1 = "hms invincible"
    private val UUID_LAUNCHER_CONFIG: IntegrationLauncherConfig? =
      IntegrationLauncherConfig()
        .withJobId(JOB_UUID.toString())
        .withAttemptId(ATTEMPT_ID.toLong())
        .withDockerImage(IMAGE_NAME1)
        .withPriority(WorkloadPriority.DEFAULT)
    private const val NAMESPACE = "namespace"
    private val STREAM_DESCRIPTOR: StreamDescriptor = StreamDescriptor().withName("name")
    private const val UNCHECKED = "unchecked"
    private const val EXCEPTION_MESSAGE = "Force state exception to simulate workflow not running"
  }
}
