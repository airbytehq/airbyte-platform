/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync

import io.airbyte.api.client.model.generated.ConnectionStatus
import io.airbyte.commons.json.Jsons.emptyObject
import io.airbyte.commons.temporal.TemporalConstants
import io.airbyte.commons.temporal.TemporalJobType
import io.airbyte.commons.temporal.scheduling.SyncWorkflow
import io.airbyte.config.FailureReason
import io.airbyte.config.OperatorWebhook
import io.airbyte.config.StandardSync
import io.airbyte.config.StandardSyncInput
import io.airbyte.config.StandardSyncOperation
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.SyncStats
import io.airbyte.config.WebhookOperationSummary
import io.airbyte.config.WorkloadPriority
import io.airbyte.micronaut.temporal.TemporalProxyHelper
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.models.PostprocessCatalogOutput.Companion.success
import io.airbyte.workers.models.ReplicationActivityInput
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogHelperActivity
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogHelperActivityImpl
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivityImpl
import io.airbyte.workers.temporal.workflows.MockConnectorCommandWorkflow
import io.airbyte.workers.testutils.TestConfigHelpers.createSyncConfig
import io.micronaut.context.BeanRegistration
import io.micronaut.inject.BeanIdentifier
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.temporal.activity.ActivityCancellationType
import io.temporal.activity.ActivityOptions
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.workflowservice.v1.RequestCancelWorkflowExecutionRequest
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowFailedException
import io.temporal.client.WorkflowOptions
import io.temporal.common.RetryOptions
import io.temporal.testing.TestWorkflowEnvironment
import io.temporal.worker.Worker
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.Optional
import java.util.UUID

internal class SyncWorkflowTest {
  // TEMPORAL
  private lateinit var testEnv: TestWorkflowEnvironment
  private lateinit var syncWorker: Worker
  private lateinit var client: WorkflowClient
  private lateinit var asyncReplicationActivity: AsyncReplicationActivity
  private lateinit var discoverCatalogHelperActivity: DiscoverCatalogHelperActivity
  private lateinit var generateReplicationActivityInputActivity: GenerateReplicationActivityInputActivity
  private lateinit var workloadStatusCheckActivity: WorkloadStatusCheckActivity
  private lateinit var invokeOperationsActivity: InvokeOperationsActivity
  private lateinit var configFetchActivity: ConfigFetchActivity
  private lateinit var reportRunTimeActivity: ReportRunTimeActivity

  private lateinit var sync: StandardSync
  private lateinit var syncInput: StandardSyncInput
  private lateinit var replicationSuccessOutput: StandardSyncOutput
  private lateinit var replicationFailOutput: StandardSyncOutput
  private lateinit var standardSyncSummary: StandardSyncSummary
  private lateinit var failedSyncSummary: StandardSyncSummary
  private lateinit var syncStats: SyncStats
  private lateinit var longActivityOptions: ActivityOptions
  private lateinit var shortActivityOptions: ActivityOptions
  private lateinit var refreshSchemaActivityOptions: ActivityOptions
  private lateinit var asyncReplicationActivityOptions: ActivityOptions
  private lateinit var workloadStatusCheckActivityOptions: ActivityOptions
  private lateinit var temporalProxyHelper: TemporalProxyHelper

  @BeforeEach
  fun setUp() {
    testEnv = TestWorkflowEnvironment.newInstance()

    val connectorCommandWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
    connectorCommandWorker.registerWorkflowImplementationTypes(MockConnectorCommandWorkflow::class.java)

    syncWorker = testEnv.newWorker(SYNC_QUEUE)
    client = testEnv.getWorkflowClient()

    val syncPair: Pair<StandardSync, StandardSyncInput?> = createSyncConfig(ORGANIZATION_ID, SOURCE_DEFINITION_ID)
    sync = syncPair.first
    syncInput = syncPair.second!!.withSourceId(SOURCE_ID)

    syncStats = SyncStats().withRecordsCommitted(10L)
    standardSyncSummary = StandardSyncSummary().withTotalStats(syncStats)
    failedSyncSummary =
      StandardSyncSummary().withStatus(StandardSyncSummary.ReplicationStatus.FAILED).withTotalStats(SyncStats().withRecordsEmitted(0L))
    replicationSuccessOutput = StandardSyncOutput().withStandardSyncSummary(standardSyncSummary)
    replicationFailOutput = StandardSyncOutput().withStandardSyncSummary(failedSyncSummary)
    asyncReplicationActivity = mockk<AsyncReplicationActivityImpl>()
    discoverCatalogHelperActivity = mockk<DiscoverCatalogHelperActivityImpl>()
    generateReplicationActivityInputActivity = mockk<GenerateReplicationActivityInputActivityImpl>()
    workloadStatusCheckActivity = mockk<WorkloadStatusCheckActivityImpl>()
    invokeOperationsActivity = mockk<InvokeOperationsActivityImpl>(relaxed = true)
    configFetchActivity = mockk<ConfigFetchActivityImpl>()
    reportRunTimeActivity = mockk<ReportRunTimeActivityImpl>(relaxed = true)

    every { discoverCatalogHelperActivity.postprocess(any()) } returns success(null)

    every { configFetchActivity.getSourceId(sync.getConnectionId()) } returns Optional.of(SOURCE_ID)
    every { configFetchActivity.getStatus(sync.getConnectionId()) } returns Optional.of(ConnectionStatus.ACTIVE)

    every { configFetchActivity.getSourceConfig(SOURCE_ID) } returns emptyObject()

    every {
      generateReplicationActivityInputActivity.generate(any(), any(), any(), any(), any(), any(), any())
    } returns
      ReplicationActivityInput(
        sourceId = SOURCE_ID,
        destinationId = UUID.randomUUID(),
        sourceConfiguration = emptyObject(),
        destinationConfiguration = emptyObject(),
        jobRunConfig = JOB_RUN_CONFIG,
        sourceLauncherConfig = SOURCE_LAUNCHER_CONFIG,
        destinationLauncherConfig = DESTINATION_LAUNCHER_CONFIG,
        connectionId = sync.connectionId,
        workspaceId = UUID.randomUUID(),
      )

    longActivityOptions =
      ActivityOptions
        .newBuilder()
        .setScheduleToCloseTimeout(Duration.ofDays(3))
        .setStartToCloseTimeout(Duration.ofDays(3))
        .setScheduleToStartTimeout(Duration.ofDays(3))
        .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
        .setRetryOptions(TemporalConstants.NO_RETRY)
        .setHeartbeatTimeout(TemporalConstants.HEARTBEAT_TIMEOUT)
        .build()
    shortActivityOptions =
      ActivityOptions
        .newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(120))
        .setRetryOptions(
          RetryOptions
            .newBuilder()
            .setMaximumAttempts(5)
            .setInitialInterval(Duration.ofSeconds(30))
            .setMaximumInterval(Duration.ofSeconds(600))
            .build(),
        ).build()
    refreshSchemaActivityOptions =
      ActivityOptions
        .newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(360))
        .setRetryOptions(TemporalConstants.NO_RETRY)
        .build()
    asyncReplicationActivityOptions =
      ActivityOptions
        .newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(60))
        .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
        .setRetryOptions(TemporalConstants.NO_RETRY)
        .build()
    workloadStatusCheckActivityOptions =
      ActivityOptions
        .newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(60))
        .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(5).build())
        .build()

    val longActivitiesBeanIdentifier = mockk<BeanIdentifier>()
    val longActivityOptionsBeanRegistration = mockk<BeanRegistration<ActivityOptions>>()
    every { longActivitiesBeanIdentifier.getName() } returns "longRunActivityOptions"
    every { longActivityOptionsBeanRegistration.getIdentifier() } returns longActivitiesBeanIdentifier
    every { longActivityOptionsBeanRegistration.getBean() } returns longActivityOptions
    val shortActivitiesBeanIdentifier = mockk<BeanIdentifier>()
    val shortActivityOptionsBeanRegistration = mockk<BeanRegistration<ActivityOptions>>()
    every { shortActivitiesBeanIdentifier.getName() } returns "shortActivityOptions"
    every { shortActivityOptionsBeanRegistration.getIdentifier() } returns shortActivitiesBeanIdentifier
    every { shortActivityOptionsBeanRegistration.getBean() } returns shortActivityOptions
    val refreshSchemaActivityBeanIdentifier = mockk<BeanIdentifier>()
    val refreshSchemaActivityOptionsBeanRegistration = mockk<BeanRegistration<ActivityOptions>>()
    every { refreshSchemaActivityBeanIdentifier.getName() } returns "refreshSchemaActivityOptions"
    every { refreshSchemaActivityOptionsBeanRegistration.getIdentifier() } returns refreshSchemaActivityBeanIdentifier
    every { refreshSchemaActivityOptionsBeanRegistration.getBean() } returns refreshSchemaActivityOptions
    val asyncActivitiesBeanIdentifier = mockk<BeanIdentifier>()
    val asyncActivityOptionsBeanRegistration = mockk<BeanRegistration<ActivityOptions>>()
    every { asyncActivitiesBeanIdentifier.getName() } returns "asyncActivityOptions"
    every { asyncActivityOptionsBeanRegistration.getIdentifier() } returns asyncActivitiesBeanIdentifier
    every { asyncActivityOptionsBeanRegistration.getBean() } returns asyncReplicationActivityOptions
    val workloadStatusCheckActivitiesBeanIdentifier = mockk<BeanIdentifier>()
    val workloadStatusCheckActivityOptionsBeanRegistration = mockk<BeanRegistration<ActivityOptions>>()
    every { workloadStatusCheckActivitiesBeanIdentifier.getName() } returns "workloadStatusCheckActivityOptions"
    every { workloadStatusCheckActivityOptionsBeanRegistration.getIdentifier() } returns workloadStatusCheckActivitiesBeanIdentifier
    every { workloadStatusCheckActivityOptionsBeanRegistration.getBean() } returns workloadStatusCheckActivityOptions
    temporalProxyHelper =
      TemporalProxyHelper(
        listOf(
          longActivityOptionsBeanRegistration,
          shortActivityOptionsBeanRegistration,
          refreshSchemaActivityOptionsBeanRegistration,
          asyncActivityOptionsBeanRegistration,
          workloadStatusCheckActivityOptionsBeanRegistration,
        ),
      )

    syncWorker.registerWorkflowImplementationTypes(temporalProxyHelper.proxyWorkflowClass(SyncWorkflowImpl::class.java))
  }

  @AfterEach
  fun tearDown() {
    testEnv.close()
  }

  // bundle up all the temporal worker setup / execution into one method.
  private fun execute(isReset: Boolean = false): StandardSyncOutput {
    syncWorker.registerActivitiesImplementations(
      asyncReplicationActivity,
      discoverCatalogHelperActivity,
      generateReplicationActivityInputActivity,
      workloadStatusCheckActivity,
      invokeOperationsActivity,
      configFetchActivity,
      reportRunTimeActivity,
    )
    testEnv.start()
    val workflow = client.newWorkflowStub(SyncWorkflow::class.java, WorkflowOptions.newBuilder().setTaskQueue(SYNC_QUEUE).build())

    return workflow.run(
      JOB_RUN_CONFIG,
      SOURCE_LAUNCHER_CONFIG,
      DESTINATION_LAUNCHER_CONFIG,
      syncInput.withIsReset(isReset),
      sync.getConnectionId(),
    )
  }

  @Test
  @Throws(Exception::class)
  fun testSuccess() {
    val workloadId = "my-successful-workload"
    every { asyncReplicationActivity.startReplication(any()) } returns workloadId

    every { workloadStatusCheckActivity.isTerminal(workloadId) } returns true

    every {
      asyncReplicationActivity.getReplicationOutput(any(), workloadId)
    } returns replicationSuccessOutput

    val actualOutput = execute()

    Assertions.assertEquals(
      replicationSuccessOutput.getStandardSyncSummary(),
      removeRefreshTime(actualOutput.getStandardSyncSummary()),
    )
  }

  @Test
  @Throws(Exception::class)
  fun testReplicationFailure() {
    every { asyncReplicationActivity.startReplication(any()) } throws IllegalArgumentException("induced exception")

    Assertions.assertThrows(
      WorkflowFailedException::class.java,
    ) { this.execute() }
  }

  @Test
  @Throws(Exception::class)
  fun testReplicationFailedGracefully() {
    val workloadId = "my-failed-workload"
    every { asyncReplicationActivity.startReplication(any()) } returns workloadId

    every { workloadStatusCheckActivity.isTerminal(workloadId) } returns true

    every { asyncReplicationActivity.getReplicationOutput(any(), workloadId) } returns replicationFailOutput

    val actualOutput = execute()

    Assertions.assertEquals(
      replicationFailOutput.getStandardSyncSummary(),
      removeRefreshTime(actualOutput.getStandardSyncSummary()),
    )
  }

  private fun removeRefreshTime(`in`: StandardSyncSummary): StandardSyncSummary {
    `in`.getTotalStats().setDiscoverSchemaEndTime(null)
    `in`.getTotalStats().setDiscoverSchemaStartTime(null)

    return `in`
  }

  @Test
  @Throws(Exception::class)
  fun testCancelDuringReplication() {
    val workloadId = "my-cancelled-workload"

    every { asyncReplicationActivity.startReplication(any()) } returns workloadId

    every { workloadStatusCheckActivity.isTerminal(workloadId) } answers {
      cancelWorkflow()
      replicationSuccessOutput // This will be ignored, see note below
      true
    }

    Assertions.assertThrows(WorkflowFailedException::class.java) {
      this.execute()
    }
  }

  @Test
  fun testWebhookOperation() {
    val workloadId = "my-successful-workload"
    every { asyncReplicationActivity.startReplication(any()) } returns workloadId

    every { workloadStatusCheckActivity.isTerminal(workloadId) } returns true

    every { asyncReplicationActivity.getReplicationOutput(any(), workloadId) } returns StandardSyncOutput()

    val webhookOperation =
      StandardSyncOperation()
        .withOperationId(UUID.randomUUID())
        .withOperatorType(StandardSyncOperation.OperatorType.WEBHOOK)
        .withOperatorWebhook(
          OperatorWebhook()
            .withExecutionUrl(WEBHOOK_URL)
            .withExecutionBody(WEBHOOK_BODY)
            .withWebhookConfigId(WEBHOOK_CONFIG_ID),
        )
    val workspaceWebhookConfigs = emptyObject()
    val webhookOperationSummary = WebhookOperationSummary()
    webhookOperationSummary.setSuccesses(java.util.List.of(WEBHOOK_CONFIG_ID))
    syncInput
      .withOperationSequence(java.util.List.of(webhookOperation))
      .withWebhookOperationConfigs(workspaceWebhookConfigs)
    every {
      invokeOperationsActivity.invokeOperations(
        any<List<StandardSyncOperation>>(),
        any<StandardSyncInput>(),
        any<JobRunConfig>(),
      )
    } returns webhookOperationSummary
    val actualOutput = execute()
    Assertions.assertEquals(actualOutput.getWebhookOperationSummary().getSuccesses().first(), WEBHOOK_CONFIG_ID)
  }

  @Test
  @Throws(Exception::class)
  fun testSkipReplicationIfConnectionDisabledBySchemaRefresh() {
    every {
      configFetchActivity.getStatus(
        any<UUID>(),
      )
    } returns Optional.of(ConnectionStatus.INACTIVE)
    val output = execute()
    confirmVerified(asyncReplicationActivity)
    Assertions.assertEquals(output.getStandardSyncSummary().getStatus(), StandardSyncSummary.ReplicationStatus.CANCELLED)
  }

  @Test
  @Throws(Exception::class)
  fun testGetProperFailureIfRefreshFails() {
    every { discoverCatalogHelperActivity.postprocess(any()) } throws RuntimeException()

    val output = execute()
    Assertions.assertEquals(StandardSyncSummary.ReplicationStatus.FAILED, output.getStandardSyncSummary().getStatus())
    Assertions.assertEquals(1, output.getFailures().size.toLong())
    Assertions.assertEquals(FailureReason.FailureOrigin.AIRBYTE_PLATFORM, output.getFailures().get(0).getFailureOrigin())
    Assertions.assertEquals(FailureReason.FailureType.REFRESH_SCHEMA, output.getFailures().get(0).getFailureType())
  }

  private fun cancelWorkflow() {
    val temporalService = testEnv.getWorkflowService().blockingStub()
    // there should only be one execution running.
    val workflowId =
      temporalService
        .listOpenWorkflowExecutions(null)
        .getExecutionsList()
        .get(0)
        .getExecution()
        .getWorkflowId()

    val workflowExecution =
      WorkflowExecution
        .newBuilder()
        .setWorkflowId(workflowId)
        .build()

    val cancelRequest =
      RequestCancelWorkflowExecutionRequest
        .newBuilder()
        .setWorkflowExecution(workflowExecution)
        .build()

    testEnv.getWorkflowService().blockingStub().requestCancelWorkflowExecution(cancelRequest)
  }

  companion object {
    private const val WEBHOOK_URL = "http://example.com"
    private const val WEBHOOK_BODY = "webhook-body"
    private val WEBHOOK_CONFIG_ID: UUID = UUID.randomUUID()

    // AIRBYTE CONFIGURATION
    private const val JOB_ID = 11L
    private const val ATTEMPT_ID = 21
    private val SOURCE_ID: UUID = UUID.randomUUID()
    private val JOB_RUN_CONFIG: JobRunConfig =
      JobRunConfig()
        .withJobId(JOB_ID.toString())
        .withAttemptId(ATTEMPT_ID.toLong())
    private const val IMAGE_NAME1 = "hms:invincible"
    private const val IMAGE_NAME2 = "hms:defiant"
    private val SOURCE_LAUNCHER_CONFIG: IntegrationLauncherConfig =
      IntegrationLauncherConfig()
        .withJobId(JOB_ID.toString())
        .withAttemptId(ATTEMPT_ID.toLong())
        .withDockerImage(IMAGE_NAME1)
        .withPriority(WorkloadPriority.DEFAULT)
    private val DESTINATION_LAUNCHER_CONFIG: IntegrationLauncherConfig =
      IntegrationLauncherConfig()
        .withJobId(JOB_ID.toString())
        .withAttemptId(ATTEMPT_ID.toLong())
        .withDockerImage(IMAGE_NAME2)

    private const val SYNC_QUEUE = "SYNC"

    private val ORGANIZATION_ID: UUID = UUID.randomUUID()
    private val SOURCE_DEFINITION_ID: UUID = UUID.randomUUID()
  }
}
