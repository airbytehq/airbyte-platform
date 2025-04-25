/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.sync;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.api.client.model.generated.ConnectionStatus;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.temporal.TemporalConstants;
import io.airbyte.commons.temporal.TemporalJobType;
import io.airbyte.commons.temporal.scheduling.SyncWorkflow;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.OperatorWebhook;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.StandardSyncOperation.OperatorType;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.config.SyncStats;
import io.airbyte.config.WebhookOperationSummary;
import io.airbyte.config.WorkloadPriority;
import io.airbyte.micronaut.temporal.TemporalProxyHelper;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.models.PostprocessCatalogOutput;
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogHelperActivity;
import io.airbyte.workers.temporal.discover.catalog.DiscoverCatalogHelperActivityImpl;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivityImpl;
import io.airbyte.workers.temporal.workflows.MockConnectorCommandWorkflow;
import io.airbyte.workers.testutils.TestConfigHelpers;
import io.micronaut.context.BeanRegistration;
import io.micronaut.inject.BeanIdentifier;
import io.temporal.activity.ActivityCancellationType;
import io.temporal.activity.ActivityOptions;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.workflowservice.v1.RequestCancelWorkflowExecutionRequest;
import io.temporal.api.workflowservice.v1.WorkflowServiceGrpc.WorkflowServiceBlockingStub;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowFailedException;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.RetryOptions;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"PMD.UnusedPrivateField", "PMD.UnusedPrivateMethod"})
class SyncWorkflowTest {

  private static final String WEBHOOK_URL = "http://example.com";
  private static final String WEBHOOK_BODY = "webhook-body";
  private static final UUID WEBHOOK_CONFIG_ID = UUID.randomUUID();

  // TEMPORAL

  private TestWorkflowEnvironment testEnv;
  private Worker syncWorker;
  private WorkflowClient client;
  private AsyncReplicationActivity asyncReplicationActivity;
  private DiscoverCatalogHelperActivity discoverCatalogHelperActivity;
  private GenerateReplicationActivityInputActivity generateReplicationActivityInputActivity;
  private WorkloadStatusCheckActivity workloadStatusCheckActivity;
  private InvokeOperationsActivity invokeOperationsActivity;
  private ConfigFetchActivityImpl configFetchActivity;
  private ReportRunTimeActivity reportRunTimeActivity;

  // AIRBYTE CONFIGURATION
  private static final long JOB_ID = 11L;
  private static final int ATTEMPT_ID = 21;
  private static final UUID SOURCE_ID = UUID.randomUUID();
  private static final JobRunConfig JOB_RUN_CONFIG = new JobRunConfig()
      .withJobId(String.valueOf(JOB_ID))
      .withAttemptId((long) ATTEMPT_ID);
  private static final String IMAGE_NAME1 = "hms:invincible";
  private static final String IMAGE_NAME2 = "hms:defiant";
  private static final IntegrationLauncherConfig SOURCE_LAUNCHER_CONFIG = new IntegrationLauncherConfig()
      .withJobId(String.valueOf(JOB_ID))
      .withAttemptId((long) ATTEMPT_ID)
      .withDockerImage(IMAGE_NAME1)
      .withPriority(WorkloadPriority.DEFAULT);
  private static final IntegrationLauncherConfig DESTINATION_LAUNCHER_CONFIG = new IntegrationLauncherConfig()
      .withJobId(String.valueOf(JOB_ID))
      .withAttemptId((long) ATTEMPT_ID)
      .withDockerImage(IMAGE_NAME2);

  private static final String SYNC_QUEUE = "SYNC";

  private static final UUID ORGANIZATION_ID = UUID.randomUUID();
  private static final UUID SOURCE_DEFINITION_ID = UUID.randomUUID();

  private StandardSync sync;
  private StandardSyncInput syncInput;
  private StandardSyncOutput replicationSuccessOutput;
  private StandardSyncOutput replicationFailOutput;
  private StandardSyncSummary standardSyncSummary;
  private StandardSyncSummary failedSyncSummary;
  private SyncStats syncStats;
  private ActivityOptions longActivityOptions;
  private ActivityOptions shortActivityOptions;
  private ActivityOptions refreshSchemaActivityOptions;
  private ActivityOptions asyncReplicationActivityOptions;
  private ActivityOptions workloadStatusCheckActivityOptions;
  private TemporalProxyHelper temporalProxyHelper;

  @BeforeEach
  void setUp() {
    testEnv = TestWorkflowEnvironment.newInstance();

    final Worker connectorCommandWorker = testEnv.newWorker(TemporalJobType.SYNC.name());
    connectorCommandWorker.registerWorkflowImplementationTypes(MockConnectorCommandWorkflow.class);

    syncWorker = testEnv.newWorker(SYNC_QUEUE);
    client = testEnv.getWorkflowClient();

    final var syncPair = TestConfigHelpers.createSyncConfig(ORGANIZATION_ID, SOURCE_DEFINITION_ID);
    sync = syncPair.getFirst();
    syncInput = syncPair.getSecond().withSourceId(SOURCE_ID);

    syncStats = new SyncStats().withRecordsCommitted(10L);
    standardSyncSummary = new StandardSyncSummary().withTotalStats(syncStats);
    failedSyncSummary = new StandardSyncSummary().withStatus(ReplicationStatus.FAILED).withTotalStats(new SyncStats().withRecordsEmitted(0L));
    replicationSuccessOutput = new StandardSyncOutput().withStandardSyncSummary(standardSyncSummary);
    replicationFailOutput = new StandardSyncOutput().withStandardSyncSummary(failedSyncSummary);
    asyncReplicationActivity = mock(AsyncReplicationActivityImpl.class);
    discoverCatalogHelperActivity = mock(DiscoverCatalogHelperActivityImpl.class);
    generateReplicationActivityInputActivity = mock(GenerateReplicationActivityInputActivityImpl.class);
    workloadStatusCheckActivity = mock(WorkloadStatusCheckActivityImpl.class);
    invokeOperationsActivity = mock(InvokeOperationsActivityImpl.class);
    configFetchActivity = mock(ConfigFetchActivityImpl.class);
    reportRunTimeActivity = mock(ReportRunTimeActivityImpl.class);

    when(discoverCatalogHelperActivity.postprocess(any())).thenReturn(PostprocessCatalogOutput.Companion.success(null));

    when(configFetchActivity.getSourceId(sync.getConnectionId())).thenReturn(Optional.of(SOURCE_ID));
    when(configFetchActivity.getStatus(sync.getConnectionId())).thenReturn(Optional.of(ConnectionStatus.ACTIVE));
    when(configFetchActivity.getSourceConfig(SOURCE_ID)).thenReturn(Jsons.emptyObject());

    longActivityOptions = ActivityOptions.newBuilder()
        .setScheduleToCloseTimeout(Duration.ofDays(3))
        .setStartToCloseTimeout(Duration.ofDays(3))
        .setScheduleToStartTimeout(Duration.ofDays(3))
        .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
        .setRetryOptions(TemporalConstants.NO_RETRY)
        .setHeartbeatTimeout(TemporalConstants.HEARTBEAT_TIMEOUT)
        .build();
    shortActivityOptions = ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(120))
        .setRetryOptions(RetryOptions.newBuilder()
            .setMaximumAttempts(5)
            .setInitialInterval(Duration.ofSeconds(30))
            .setMaximumInterval(Duration.ofSeconds(600))
            .build())
        .build();
    refreshSchemaActivityOptions = ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(360))
        .setRetryOptions(TemporalConstants.NO_RETRY)
        .build();
    asyncReplicationActivityOptions = ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(60))
        .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
        .setRetryOptions(TemporalConstants.NO_RETRY)
        .build();
    workloadStatusCheckActivityOptions = ActivityOptions.newBuilder()
        .setStartToCloseTimeout(Duration.ofSeconds(60))
        .setCancellationType(ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
        .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(5).build())
        .build();

    final BeanIdentifier longActivitiesBeanIdentifier = mock(BeanIdentifier.class);
    final BeanRegistration longActivityOptionsBeanRegistration = mock(BeanRegistration.class);
    when(longActivitiesBeanIdentifier.getName()).thenReturn("longRunActivityOptions");
    when(longActivityOptionsBeanRegistration.getIdentifier()).thenReturn(longActivitiesBeanIdentifier);
    when(longActivityOptionsBeanRegistration.getBean()).thenReturn(longActivityOptions);
    final BeanIdentifier shortActivitiesBeanIdentifier = mock(BeanIdentifier.class);
    final BeanRegistration shortActivityOptionsBeanRegistration = mock(BeanRegistration.class);
    when(shortActivitiesBeanIdentifier.getName()).thenReturn("shortActivityOptions");
    when(shortActivityOptionsBeanRegistration.getIdentifier()).thenReturn(shortActivitiesBeanIdentifier);
    when(shortActivityOptionsBeanRegistration.getBean()).thenReturn(shortActivityOptions);
    final BeanIdentifier refreshSchemaActivityBeanIdentifier = mock(BeanIdentifier.class);
    final BeanRegistration refreshSchemaActivityOptionsBeanRegistration = mock(BeanRegistration.class);
    when(refreshSchemaActivityBeanIdentifier.getName()).thenReturn("refreshSchemaActivityOptions");
    when(refreshSchemaActivityOptionsBeanRegistration.getIdentifier()).thenReturn(refreshSchemaActivityBeanIdentifier);
    when(refreshSchemaActivityOptionsBeanRegistration.getBean()).thenReturn(refreshSchemaActivityOptions);
    final BeanIdentifier asyncActivitiesBeanIdentifier = mock(BeanIdentifier.class);
    final BeanRegistration asyncActivityOptionsBeanRegistration = mock(BeanRegistration.class);
    when(asyncActivitiesBeanIdentifier.getName()).thenReturn("asyncActivityOptions");
    when(asyncActivityOptionsBeanRegistration.getIdentifier()).thenReturn(asyncActivitiesBeanIdentifier);
    when(asyncActivityOptionsBeanRegistration.getBean()).thenReturn(asyncReplicationActivityOptions);
    final BeanIdentifier workloadStatusCheckActivitiesBeanIdentifier = mock(BeanIdentifier.class);
    final BeanRegistration workloadStatusCheckActivityOptionsBeanRegistration = mock(BeanRegistration.class);
    when(workloadStatusCheckActivitiesBeanIdentifier.getName()).thenReturn("workloadStatusCheckActivityOptions");
    when(workloadStatusCheckActivityOptionsBeanRegistration.getIdentifier()).thenReturn(workloadStatusCheckActivitiesBeanIdentifier);
    when(workloadStatusCheckActivityOptionsBeanRegistration.getBean()).thenReturn(workloadStatusCheckActivityOptions);
    temporalProxyHelper = new TemporalProxyHelper(
        List.of(longActivityOptionsBeanRegistration, shortActivityOptionsBeanRegistration,
            refreshSchemaActivityOptionsBeanRegistration, asyncActivityOptionsBeanRegistration, workloadStatusCheckActivityOptionsBeanRegistration));

    syncWorker.registerWorkflowImplementationTypes(temporalProxyHelper.proxyWorkflowClass(SyncWorkflowImpl.class));
  }

  @AfterEach
  public void tearDown() {
    testEnv.close();
  }

  private StandardSyncOutput execute() {
    return execute(false);
  }

  // bundle up all the temporal worker setup / execution into one method.
  private StandardSyncOutput execute(final boolean isReset) {
    syncWorker.registerActivitiesImplementations(
        asyncReplicationActivity,
        discoverCatalogHelperActivity,
        generateReplicationActivityInputActivity,
        workloadStatusCheckActivity,
        invokeOperationsActivity,
        configFetchActivity,
        reportRunTimeActivity);
    testEnv.start();
    final SyncWorkflow workflow =
        client.newWorkflowStub(SyncWorkflow.class, WorkflowOptions.newBuilder().setTaskQueue(SYNC_QUEUE).build());

    return workflow.run(JOB_RUN_CONFIG, SOURCE_LAUNCHER_CONFIG, DESTINATION_LAUNCHER_CONFIG, syncInput.withIsReset(isReset), sync.getConnectionId());
  }

  @Test
  void testSuccess() throws Exception {
    final String workloadId = "my-successful-workload";
    doReturn(workloadId).when(asyncReplicationActivity).startReplication(any());
    doReturn(true).when(workloadStatusCheckActivity).isTerminal(workloadId);
    doReturn(replicationSuccessOutput).when(asyncReplicationActivity).getReplicationOutput(any(), eq(workloadId));

    final StandardSyncOutput actualOutput = execute();

    assertEquals(
        replicationSuccessOutput.getStandardSyncSummary(),
        removeRefreshTime(actualOutput.getStandardSyncSummary()));
  }

  @Test
  void testReplicationFailure() throws Exception {
    doThrow(new IllegalArgumentException("induced exception")).when(asyncReplicationActivity).startReplication(any());

    assertThrows(WorkflowFailedException.class, this::execute);
  }

  @Test
  void testReplicationFailedGracefully() throws Exception {
    final String workloadId = "my-failed-workload";
    doReturn(workloadId).when(asyncReplicationActivity).startReplication(any());
    doReturn(true).when(workloadStatusCheckActivity).isTerminal(workloadId);
    doReturn(replicationFailOutput).when(asyncReplicationActivity).getReplicationOutput(any(), eq(workloadId));

    final StandardSyncOutput actualOutput = execute();

    assertEquals(
        replicationFailOutput.getStandardSyncSummary(),
        removeRefreshTime(actualOutput.getStandardSyncSummary()));
  }

  private StandardSyncSummary removeRefreshTime(final StandardSyncSummary in) {
    in.getTotalStats().setDiscoverSchemaEndTime(null);
    in.getTotalStats().setDiscoverSchemaStartTime(null);

    return in;
  }

  @Test
  void testCancelDuringReplication() throws Exception {
    final String workloadId = "my-cancelled-workload";
    doReturn(workloadId).when(asyncReplicationActivity).startReplication(any());
    doAnswer(ignored -> {
      cancelWorkflow();
      return replicationSuccessOutput;
    }).when(workloadStatusCheckActivity).isTerminal(eq(workloadId));

    assertThrows(WorkflowFailedException.class, this::execute);
  }

  @Test
  void testWebhookOperation() {
    final String workloadId = "my-successful-workload";
    doReturn(workloadId).when(asyncReplicationActivity).startReplication(any());
    doReturn(true).when(workloadStatusCheckActivity).isTerminal(workloadId);
    doReturn(new StandardSyncOutput()).when(asyncReplicationActivity).getReplicationOutput(any(), eq(workloadId));
    final StandardSyncOperation webhookOperation = new StandardSyncOperation()
        .withOperationId(UUID.randomUUID())
        .withOperatorType(OperatorType.WEBHOOK)
        .withOperatorWebhook(new OperatorWebhook()
            .withExecutionUrl(WEBHOOK_URL)
            .withExecutionBody(WEBHOOK_BODY)
            .withWebhookConfigId(WEBHOOK_CONFIG_ID));
    final JsonNode workspaceWebhookConfigs = Jsons.emptyObject();
    final WebhookOperationSummary webhookOperationSummary = new WebhookOperationSummary();
    webhookOperationSummary.setSuccesses(List.of(WEBHOOK_CONFIG_ID));
    syncInput.withOperationSequence(List.of(webhookOperation)).withWebhookOperationConfigs(workspaceWebhookConfigs);
    when(invokeOperationsActivity.invokeOperations(any(), any(), any())).thenReturn(webhookOperationSummary);
    final StandardSyncOutput actualOutput = execute();
    assertEquals(actualOutput.getWebhookOperationSummary().getSuccesses().getFirst(), WEBHOOK_CONFIG_ID);
  }

  @Test
  void testSkipReplicationIfConnectionDisabledBySchemaRefresh() throws Exception {
    when(configFetchActivity.getStatus(any())).thenReturn(Optional.of(ConnectionStatus.INACTIVE));
    final StandardSyncOutput output = execute();
    verifyNoInteractions(asyncReplicationActivity);
    assertEquals(output.getStandardSyncSummary().getStatus(), ReplicationStatus.CANCELLED);
  }

  @Test
  void testGetProperFailureIfRefreshFails() throws Exception {
    doThrow(new RuntimeException())
        .when(discoverCatalogHelperActivity).postprocess(any());
    final StandardSyncOutput output = execute();
    assertEquals(ReplicationStatus.FAILED, output.getStandardSyncSummary().getStatus());
    assertEquals(1, output.getFailures().size());
    assertEquals(FailureOrigin.AIRBYTE_PLATFORM, output.getFailures().get(0).getFailureOrigin());
    assertEquals(FailureType.REFRESH_SCHEMA, output.getFailures().get(0).getFailureType());
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  private void cancelWorkflow() {
    final WorkflowServiceBlockingStub temporalService = testEnv.getWorkflowService().blockingStub();
    // there should only be one execution running.
    final String workflowId = temporalService.listOpenWorkflowExecutions(null).getExecutionsList().get(0).getExecution().getWorkflowId();

    final WorkflowExecution workflowExecution = WorkflowExecution.newBuilder()
        .setWorkflowId(workflowId)
        .build();

    final RequestCancelWorkflowExecutionRequest cancelRequest = RequestCancelWorkflowExecutionRequest.newBuilder()
        .setWorkflowExecution(workflowExecution)
        .build();

    testEnv.getWorkflowService().blockingStub().requestCancelWorkflowExecution(cancelRequest);
  }

}
