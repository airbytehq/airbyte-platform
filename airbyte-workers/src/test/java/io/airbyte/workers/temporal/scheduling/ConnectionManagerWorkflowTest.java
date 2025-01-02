/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.when;

import io.airbyte.commons.constants.WorkerConstants;
import io.airbyte.commons.temporal.TemporalJobType;
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow;
import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput;
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow;
import io.airbyte.commons.temporal.scheduling.SyncWorkflow;
import io.airbyte.commons.temporal.scheduling.retries.BackoffPolicy;
import io.airbyte.commons.temporal.scheduling.retries.RetryManager;
import io.airbyte.commons.temporal.scheduling.state.WorkflowState;
import io.airbyte.commons.temporal.scheduling.state.listener.TestStateListener;
import io.airbyte.commons.temporal.scheduling.state.listener.WorkflowStateChangedListener.ChangedStateEvent;
import io.airbyte.commons.temporal.scheduling.state.listener.WorkflowStateChangedListener.StateField;
import io.airbyte.commons.version.Version;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.micronaut.temporal.TemporalProxyHelper;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.models.JobInput;
import io.airbyte.workers.models.SyncJobCheckConnectionInputs;
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity;
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity.LogOutput;
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity;
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionActivityInput;
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionOutput;
import io.airbyte.workers.temporal.scheduling.activities.CheckRunProgressActivity;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.GetMaxAttemptOutput;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.ScheduleRetrieverOutput;
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity;
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity.FeatureFlagFetchOutput;
import io.airbyte.workers.temporal.scheduling.activities.GenerateInputActivity.SyncInputWithAttemptNumber;
import io.airbyte.workers.temporal.scheduling.activities.GenerateInputActivityImpl;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptNumberCreationOutput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptNumberFailureInput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCancelledInputWithAttemptNumber;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCreationOutput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobSuccessInputWithAttemptNumber;
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.HydrateInput;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.HydrateOutput;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.PersistInput;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.PersistOutput;
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivity;
import io.airbyte.workers.temporal.scheduling.activities.WorkflowConfigActivity;
import io.airbyte.workers.temporal.scheduling.testcheckworkflow.CheckConnectionDestinationSystemErrorWorkflow;
import io.airbyte.workers.temporal.scheduling.testcheckworkflow.CheckConnectionFailedWorkflow;
import io.airbyte.workers.temporal.scheduling.testcheckworkflow.CheckConnectionSourceSuccessOnlyWorkflow;
import io.airbyte.workers.temporal.scheduling.testcheckworkflow.CheckConnectionSuccessWorkflow;
import io.airbyte.workers.temporal.scheduling.testcheckworkflow.CheckConnectionSystemErrorWorkflow;
import io.airbyte.workers.temporal.scheduling.testsyncworkflow.CancelledSyncWorkflow;
import io.airbyte.workers.temporal.scheduling.testsyncworkflow.EmptySyncWorkflow;
import io.airbyte.workers.temporal.scheduling.testsyncworkflow.ReplicateFailureSyncWorkflow;
import io.airbyte.workers.temporal.scheduling.testsyncworkflow.SleepingSyncWorkflow;
import io.airbyte.workers.temporal.scheduling.testsyncworkflow.SourceAndDestinationFailureSyncWorkflow;
import io.airbyte.workers.temporal.scheduling.testsyncworkflow.SyncWorkflowFailingOutputWorkflow;
import io.micronaut.context.BeanRegistration;
import io.micronaut.inject.BeanIdentifier;
import io.temporal.activity.ActivityOptions;
import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.api.filter.v1.WorkflowExecutionFilter;
import io.temporal.api.workflowservice.v1.ListClosedWorkflowExecutionsRequest;
import io.temporal.api.workflowservice.v1.ListClosedWorkflowExecutionsResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.ApplicationFailure;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the core state machine of the connection manager workflow.
 * <p>
 * We've had race conditions in this in the past which is why (after addressing them) we have
 * repeated cases, just in case there's a regression where a race condition is added back to a test.
 */
// Forcing SAME_THREAD execution as we seem to face the issues described in
// https://github.com/mockito/mockito/wiki/FAQ#is-mockito-thread-safe
@Execution(ExecutionMode.SAME_THREAD)
class ConnectionManagerWorkflowTest {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long JOB_ID = 1L;
  private static final int ATTEMPT_ID = 1;
  private static final int ATTEMPT_NO = 1;

  private static final Duration SCHEDULE_WAIT = Duration.ofMinutes(20L);
  private static final String WORKFLOW_ID = "workflow-id";

  private static final Duration WORKFLOW_FAILURE_RESTART_DELAY = Duration.ofSeconds(600);
  private static final String SOURCE_DOCKER_IMAGE = "some_source";

  private static final int TEN_SECONDS = 10 * 1000;
  private static final VerificationMode VERIFY_TIMEOUT = timeout(TEN_SECONDS).times(1);

  private final ConfigFetchActivity mConfigFetchActivity =
      mock(ConfigFetchActivity.class, Mockito.withSettings().withoutAnnotations());
  private static final GenerateInputActivityImpl mGenerateInputActivityImpl =
      mock(GenerateInputActivityImpl.class, Mockito.withSettings().withoutAnnotations());
  private static final JobCreationAndStatusUpdateActivity mJobCreationAndStatusUpdateActivity =
      mock(JobCreationAndStatusUpdateActivity.class, Mockito.withSettings().withoutAnnotations());
  private static final AutoDisableConnectionActivity mAutoDisableConnectionActivity =
      mock(AutoDisableConnectionActivity.class, Mockito.withSettings().withoutAnnotations());
  private static final StreamResetActivity mStreamResetActivity =
      mock(StreamResetActivity.class, Mockito.withSettings().withoutAnnotations());
  private static final RecordMetricActivity mRecordMetricActivity =
      mock(RecordMetricActivity.class, Mockito.withSettings().withoutAnnotations());
  private static final WorkflowConfigActivity mWorkflowConfigActivity =
      mock(WorkflowConfigActivity.class, Mockito.withSettings().withoutAnnotations());
  private static final FeatureFlagFetchActivity mFeatureFlagFetchActivity =
      mock(FeatureFlagFetchActivity.class, Mockito.withSettings().withoutAnnotations());
  private static final CheckRunProgressActivity mCheckRunProgressActivity =
      mock(CheckRunProgressActivity.class, Mockito.withSettings().withoutAnnotations());
  private static final RetryStatePersistenceActivity mRetryStatePersistenceActivity =
      mock(RetryStatePersistenceActivity.class, Mockito.withSettings().withoutAnnotations());
  private static final AppendToAttemptLogActivity mAppendToAttemptLogActivity =
      mock(AppendToAttemptLogActivity.class, Mockito.withSettings().withoutAnnotations());
  private static final String EVENT = "event = ";
  private static final String FAILED_CHECK_MESSAGE = "nope";

  private TestWorkflowEnvironment testEnv;
  private WorkflowClient client;
  private ConnectionManagerWorkflow workflow;
  private ActivityOptions activityOptions;
  private TemporalProxyHelper temporalProxyHelper;

  static Stream<Arguments> getMaxAttemptForResetRetry() {
    return Stream.of(
        // "The max attempt is 3, it will test that after a failed reset attempt the next attempt will also
        // be a reset")
        Arguments.of(3),
        // "The max attempt is 3, it will test that after a failed reset job the next attempt will also be a
        // job")
        Arguments.of(1));
  }

  @BeforeEach
  void setUp() throws Exception {
    Mockito.reset(mConfigFetchActivity);
    Mockito.reset(mGenerateInputActivityImpl);
    Mockito.reset(mJobCreationAndStatusUpdateActivity);
    Mockito.reset(mAutoDisableConnectionActivity);
    Mockito.reset(mStreamResetActivity);
    Mockito.reset(mRecordMetricActivity);
    Mockito.reset(mWorkflowConfigActivity);
    Mockito.reset(mCheckRunProgressActivity);
    Mockito.reset(mRetryStatePersistenceActivity);
    Mockito.reset(mAppendToAttemptLogActivity);

    // default is to wait "forever"
    when(mConfigFetchActivity.getTimeToWait(Mockito.any())).thenReturn(new ScheduleRetrieverOutput(
        Duration.ofDays(100 * 365)));

    when(mJobCreationAndStatusUpdateActivity.createNewJob(Mockito.any()))
        .thenReturn(new JobCreationOutput(
            1L));

    when(mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(Mockito.any()))
        .thenReturn(new AttemptNumberCreationOutput(
            1));

    when(mGenerateInputActivityImpl.getCheckConnectionInputs(Mockito.any(SyncInputWithAttemptNumber.class)))
        .thenReturn(new SyncJobCheckConnectionInputs(
            new IntegrationLauncherConfig().withDockerImage(SOURCE_DOCKER_IMAGE),
            new IntegrationLauncherConfig(),
            new StandardCheckConnectionInput(),
            new StandardCheckConnectionInput()));

    when(mGenerateInputActivityImpl.getSyncWorkflowInputWithAttemptNumber(Mockito.any(SyncInputWithAttemptNumber.class)))
        .thenReturn(
            new JobInput(
                new JobRunConfig(),
                new IntegrationLauncherConfig().withDockerImage(SOURCE_DOCKER_IMAGE),
                new IntegrationLauncherConfig(),
                new StandardSyncInput()));
    when(mAutoDisableConnectionActivity.autoDisableFailingConnection(Mockito.any()))
        .thenReturn(new AutoDisableConnectionOutput(false));

    when(mWorkflowConfigActivity.getWorkflowRestartDelaySeconds())
        .thenReturn(WORKFLOW_FAILURE_RESTART_DELAY);
    when(mFeatureFlagFetchActivity.getFeatureFlags(Mockito.any()))
        .thenReturn(new FeatureFlagFetchOutput(Map.of()));
    when(mCheckRunProgressActivity.checkProgress(Mockito.any()))
        .thenReturn(new CheckRunProgressActivity.Output(false)); // false == complete failure
    // just run once
    final var manager = new RetryManager(null, null, Integer.MAX_VALUE, Integer.MAX_VALUE, 1, Integer.MAX_VALUE);

    when(mRetryStatePersistenceActivity.hydrateRetryState(Mockito.any()))
        .thenReturn(new HydrateOutput(manager));
    when(mRetryStatePersistenceActivity.persistRetryState(Mockito.any()))
        .thenReturn(new PersistOutput(true));
    when(mAppendToAttemptLogActivity.log(Mockito.any()))
        .thenReturn(new LogOutput(true));

    activityOptions = ActivityOptions.newBuilder()
        .setHeartbeatTimeout(Duration.ofSeconds(30))
        .setStartToCloseTimeout(Duration.ofSeconds(120))
        .setRetryOptions(RetryOptions.newBuilder()
            .setMaximumAttempts(5)
            .setInitialInterval(Duration.ofSeconds(30))
            .setMaximumInterval(Duration.ofSeconds(600))
            .build())

        .build();

    final BeanIdentifier activityOptionsBeanIdentifier = mock(BeanIdentifier.class);
    final BeanRegistration activityOptionsBeanRegistration = mock(BeanRegistration.class);
    when(activityOptionsBeanIdentifier.getName()).thenReturn("shortActivityOptions");
    when(activityOptionsBeanRegistration.getIdentifier()).thenReturn(activityOptionsBeanIdentifier);
    when(activityOptionsBeanRegistration.getBean()).thenReturn(activityOptions);
    temporalProxyHelper = new TemporalProxyHelper(List.of(activityOptionsBeanRegistration));
  }

  private void returnTrueForLastJobOrAttemptFailure() throws Exception {
    when(mJobCreationAndStatusUpdateActivity.isLastJobOrAttemptFailure(Mockito.any()))
        .thenReturn(true);

    final JobRunConfig jobRunConfig = new JobRunConfig();
    jobRunConfig.setJobId(Long.toString(JOB_ID));
    jobRunConfig.setAttemptId((long) ATTEMPT_ID);
    when(mGenerateInputActivityImpl.getSyncWorkflowInputWithAttemptNumber(Mockito.any(SyncInputWithAttemptNumber.class)))
        .thenReturn(
            new JobInput(
                jobRunConfig,
                new IntegrationLauncherConfig().withDockerImage(SOURCE_DOCKER_IMAGE).withProtocolVersion(new Version("0.2.0")),
                new IntegrationLauncherConfig().withProtocolVersion(new Version("0.2.0")),
                new StandardSyncInput()));
  }

  @AfterEach
  void tearDown() {
    testEnv.shutdown();
    TestStateListener.reset();
  }

  private void mockResetJobInput(final JobRunConfig jobRunConfig) throws Exception {
    when(mGenerateInputActivityImpl.getCheckConnectionInputs(Mockito.any(SyncInputWithAttemptNumber.class)))
        .thenReturn(
            new SyncJobCheckConnectionInputs(
                new IntegrationLauncherConfig().withDockerImage(WorkerConstants.RESET_JOB_SOURCE_DOCKER_IMAGE_STUB),
                new IntegrationLauncherConfig(),
                new StandardCheckConnectionInput(),
                new StandardCheckConnectionInput()));
    when(mGenerateInputActivityImpl.getSyncWorkflowInputWithAttemptNumber(Mockito.any(SyncInputWithAttemptNumber.class)))
        .thenReturn(
            new JobInput(
                jobRunConfig,
                new IntegrationLauncherConfig().withDockerImage(WorkerConstants.RESET_JOB_SOURCE_DOCKER_IMAGE_STUB),
                new IntegrationLauncherConfig(),
                new StandardSyncInput()));
  }

  @Nested
  @DisplayName("Test which without a long running child workflow")
  class AsynchronousWorkflow {

    @BeforeEach
    void setup() {
      setupSpecificChildWorkflow(EmptySyncWorkflow.class, CheckConnectionSuccessWorkflow.class);
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that a successful workflow restarts waits")
    void runSuccess() throws Exception {
      returnTrueForLastJobOrAttemptFailure();
      when(mConfigFetchActivity.getTimeToWait(Mockito.any()))
          .thenReturn(new ScheduleRetrieverOutput(SCHEDULE_WAIT));

      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);
      // wait to be scheduled, then to run, then schedule again
      testEnv.sleep(Duration.ofMinutes(SCHEDULE_WAIT.toMinutes() + SCHEDULE_WAIT.toMinutes() + 1));
      Mockito.verify(mConfigFetchActivity, Mockito.atLeast(2)).getTimeToWait(Mockito.any());
      final Queue<ChangedStateEvent> events = testStateListener.events(testId);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.RUNNING && changedStateEvent.isValue())
          .hasSizeGreaterThan(0);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.DONE_WAITING && changedStateEvent.isValue())
          .hasSizeGreaterThan(0);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> (changedStateEvent.getField() != StateField.RUNNING
              && changedStateEvent.getField() != StateField.SUCCESS
              && changedStateEvent.getField() != StateField.DONE_WAITING)
              && changedStateEvent.isValue())
          .isEmpty();
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test workflow does not wait to run after a failure")
    void retryAfterFail() throws Exception {
      returnTrueForLastJobOrAttemptFailure();
      when(mConfigFetchActivity.getTimeToWait(Mockito.any()))
          .thenReturn(new ScheduleRetrieverOutput(SCHEDULE_WAIT));

      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          true,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);
      testEnv.sleep(Duration.ofMinutes(SCHEDULE_WAIT.toMinutes() - 1));
      final Queue<ChangedStateEvent> events = testStateListener.events(testId);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.RUNNING && changedStateEvent.isValue())
          .hasSizeGreaterThan(0);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.DONE_WAITING && changedStateEvent.isValue())
          .hasSizeGreaterThan(0);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> (changedStateEvent.getField() != StateField.RUNNING
              && changedStateEvent.getField() != StateField.SUCCESS
              && changedStateEvent.getField() != StateField.DONE_WAITING)
              && changedStateEvent.isValue())
          .isEmpty();
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test workflow which receives a manual run signal stops waiting")
    void manualRun() throws Exception {

      returnTrueForLastJobOrAttemptFailure();
      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);
      testEnv.sleep(Duration.ofMinutes(1L)); // any value here, just so it's started
      workflow.submitManualSync();

      Mockito.verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
          .jobSuccessWithAttemptNumber(any());

      final Queue<ChangedStateEvent> events = testStateListener.events(testId);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.RUNNING && changedStateEvent.isValue())
          .hasSize(1);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.SKIPPED_SCHEDULING && changedStateEvent.isValue())
          .hasSize(1);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.DONE_WAITING && changedStateEvent.isValue())
          .hasSize(1);

      Assertions.assertThat(events)
          .filteredOn(
              changedStateEvent -> (changedStateEvent.getField() != StateField.RUNNING
                  && changedStateEvent.getField() != StateField.SKIPPED_SCHEDULING
                  && changedStateEvent.getField() != StateField.SUCCESS
                  && changedStateEvent.getField() != StateField.DONE_WAITING)
                  && changedStateEvent.isValue())
          .isEmpty();
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test workflow which receives an update signal stops waiting, doesn't run, and doesn't update the job status")
    void updatedSignalReceived() throws Exception {

      returnTrueForLastJobOrAttemptFailure();
      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);
      testEnv.sleep(Duration.ofSeconds(30L));

      reset(mConfigFetchActivity);
      when(mConfigFetchActivity.getTimeToWait(Mockito.any())).thenReturn(new ScheduleRetrieverOutput(
          Duration.ofDays(100 * 365)));
      workflow.connectionUpdated();

      Mockito.verify(mConfigFetchActivity, VERIFY_TIMEOUT)
          .getTimeToWait(any());

      final Queue<ChangedStateEvent> events = testStateListener.events(testId);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.RUNNING && changedStateEvent.isValue())
          .isEmpty();

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.UPDATED && changedStateEvent.isValue())
          .hasSize(1);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.DONE_WAITING && changedStateEvent.isValue())
          .hasSize(1);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> (changedStateEvent.getField() != StateField.UPDATED
              && changedStateEvent.getField() != StateField.SUCCESS
              && changedStateEvent.getField() != StateField.DONE_WAITING)
              && changedStateEvent.isValue())
          .isEmpty();

      Mockito.verifyNoInteractions(mJobCreationAndStatusUpdateActivity);
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that cancelling a non-running workflow doesn't do anything")
    void cancelNonRunning() throws Exception {

      returnTrueForLastJobOrAttemptFailure();
      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);
      testEnv.sleep(Duration.ofSeconds(30L));
      workflow.cancelJob();
      testEnv.sleep(Duration.ofSeconds(20L));

      final Queue<ChangedStateEvent> events = testStateListener.events(testId);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.RUNNING && changedStateEvent.isValue())
          .isEmpty();

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.CANCELLED && changedStateEvent.isValue())
          .isEmpty();

      Assertions.assertThat(events)
          .filteredOn(
              changedStateEvent -> (changedStateEvent.getField() != StateField.CANCELLED && changedStateEvent.getField() != StateField.SUCCESS)
                  && changedStateEvent.isValue())
          .isEmpty();

      Mockito.verifyNoInteractions(mJobCreationAndStatusUpdateActivity);
    }

    // TODO: delete when the signal method can be removed
    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that the sync is properly deleted")
    void deleteSync() throws Exception {

      returnTrueForLastJobOrAttemptFailure();
      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = spy(new WorkflowState(testId, testStateListener));

      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);
      testEnv.sleep(Duration.ofSeconds(30L));
      workflow.deleteConnection();

      waitUntilDeleted(workflow);

      final Queue<ChangedStateEvent> events = testStateListener.events(testId);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.RUNNING && changedStateEvent.isValue())
          .isEmpty();

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.DELETED && changedStateEvent.isValue())
          .hasSizeGreaterThan(1);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.DONE_WAITING && changedStateEvent.isValue())
          .hasSizeGreaterThan(1);

      Assertions.assertThat(events)
          .filteredOn(
              changedStateEvent -> changedStateEvent.getField() != StateField.DELETED
                  && changedStateEvent.getField() != StateField.SUCCESS
                  && changedStateEvent.getField() != StateField.DONE_WAITING
                  && changedStateEvent.isValue())
          .isEmpty();
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that fresh workflow cleans the job state")
    void testStartFromCleanJobState() throws Exception {

      returnTrueForLastJobOrAttemptFailure();
      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          null,
          null,
          false,
          1,
          null,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);
      testEnv.sleep(Duration.ofSeconds(30L));

      Mockito.verify(mJobCreationAndStatusUpdateActivity, Mockito.times(1)).ensureCleanJobState(Mockito.any());
    }

  }

  @Nested
  @DisplayName("Test which with a long running child workflow")
  class SynchronousWorkflow {

    @BeforeEach
    void setup() {
      setupSpecificChildWorkflow(SleepingSyncWorkflow.class, CheckConnectionSuccessWorkflow.class);
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test workflow which receives a manual sync while running a scheduled sync does nothing")
    void manualRun() throws Exception {
      returnTrueForLastJobOrAttemptFailure();
      when(mConfigFetchActivity.getTimeToWait(Mockito.any()))
          .thenReturn(new ScheduleRetrieverOutput(SCHEDULE_WAIT));

      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);

      // wait until the middle of the run
      testEnv.sleep(Duration.ofMinutes(SCHEDULE_WAIT.toMinutes() + SleepingSyncWorkflow.RUN_TIME.toMinutes() / 2));

      // trigger the manual sync
      workflow.submitManualSync();

      // wait for the rest of the workflow
      testEnv.sleep(Duration.ofMinutes(SleepingSyncWorkflow.RUN_TIME.toMinutes() / 2 + 1));

      final Queue<ChangedStateEvent> events = testStateListener.events(testId);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.SKIPPED_SCHEDULING && changedStateEvent.isValue())
          .isEmpty();

    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that cancelling a running workflow cancels the sync")
    void cancelRunning() throws Exception {

      returnTrueForLastJobOrAttemptFailure();
      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1));

      workflow.submitManualSync();

      // wait for the manual sync to start working
      testEnv.sleep(Duration.ofMinutes(1));

      workflow.cancelJob();

      Mockito.verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
          .jobCancelledWithAttemptNumber(Mockito.argThat(new HasCancellationFailure(JOB_ID, ATTEMPT_ID)));

      final Queue<ChangedStateEvent> eventQueue = testStateListener.events(testId);
      final List<ChangedStateEvent> events = new ArrayList<>(eventQueue);

      for (final ChangedStateEvent event : events) {
        if (event.isValue()) {
          log.info(EVENT + event);
        }
      }

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.CANCELLED && changedStateEvent.isValue())
          .hasSizeGreaterThanOrEqualTo(1);
    }

    @Timeout(value = 40,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that deleting a running workflow cancels the sync")
    void deleteRunning() throws Exception {

      returnTrueForLastJobOrAttemptFailure();
      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1));

      workflow.submitManualSync();

      // wait for the manual sync to start working
      testEnv.sleep(Duration.ofMinutes(1));

      workflow.deleteConnection();

      final Queue<ChangedStateEvent> eventQueue = testStateListener.events(testId);
      final List<ChangedStateEvent> events = new ArrayList<>(eventQueue);

      Mockito.verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
          .jobCancelledWithAttemptNumber(Mockito.argThat(new HasCancellationFailure(JOB_ID, ATTEMPT_ID)));

      for (final ChangedStateEvent event : events) {
        if (event.isValue()) {
          log.info(EVENT + event);
        }
      }

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.CANCELLED && changedStateEvent.isValue())
          .hasSizeGreaterThanOrEqualTo(1);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.DELETED && changedStateEvent.isValue())
          .hasSizeGreaterThanOrEqualTo(1);
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that resetting a non-running workflow starts a reset job")
    void resetStart() throws Exception {

      returnTrueForLastJobOrAttemptFailure();
      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);
      testEnv.sleep(Duration.ofMinutes(5L));
      workflow.resetConnection();
      Mockito.verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
          .createNewAttemptNumber(any());

      final Queue<ChangedStateEvent> events = testStateListener.events(testId);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.SKIPPED_SCHEDULING && changedStateEvent.isValue())
          .hasSizeGreaterThanOrEqualTo(1);

    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that resetting a non-running workflow starts a reset job")
    void resetAndContinue() throws Exception {

      returnTrueForLastJobOrAttemptFailure();
      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);
      testEnv.sleep(Duration.ofMinutes(5L));
      workflow.resetConnectionAndSkipNextScheduling();
      Mockito.verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
          .createNewAttemptNumber(any());

      final Queue<ChangedStateEvent> events = testStateListener.events(testId);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.SKIPPED_SCHEDULING && changedStateEvent.isValue())
          .hasSizeGreaterThanOrEqualTo(1);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.SKIP_SCHEDULING_NEXT_WORKFLOW && changedStateEvent.isValue())
          .hasSizeGreaterThanOrEqualTo(1);
    }

    @Test
    @Timeout(value = 60,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that resetting a running workflow cancels the running workflow")
    void resetCancelRunningWorkflow() throws Exception {

      returnTrueForLastJobOrAttemptFailure();
      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);

      testEnv.sleep(Duration.ofSeconds(30L));
      workflow.submitManualSync();
      testEnv.sleep(Duration.ofSeconds(30L));
      workflow.resetConnection();

      Mockito.verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
          .jobCancelledWithAttemptNumber(Mockito.any(JobCancelledInputWithAttemptNumber.class));

      final Queue<ChangedStateEvent> eventQueue = testStateListener.events(testId);
      final List<ChangedStateEvent> events = new ArrayList<>(eventQueue);

      for (final ChangedStateEvent event : events) {
        if (event.isValue()) {
          log.info(EVENT + event);
        }
      }

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.CANCELLED_FOR_RESET && changedStateEvent.isValue())
          .hasSizeGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("Test that running workflow which receives an update signal waits for the current run and reports the job status")
    void updatedSignalReceivedWhileRunning() throws Exception {

      returnTrueForLastJobOrAttemptFailure();
      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1));

      // submit sync
      workflow.submitManualSync();

      // wait until the middle of the manual run
      testEnv.sleep(Duration.ofMinutes(SleepingSyncWorkflow.RUN_TIME.toMinutes() / 2));

      // indicate connection update
      workflow.connectionUpdated();

      // wait after the rest of the run
      testEnv.sleep(Duration.ofMinutes(SleepingSyncWorkflow.RUN_TIME.toMinutes() / 2 + 1));

      final Queue<ChangedStateEvent> eventQueue = testStateListener.events(testId);
      final List<ChangedStateEvent> events = new ArrayList<>(eventQueue);

      for (final ChangedStateEvent event : events) {
        if (event.isValue()) {
          log.info(EVENT + event);
        }
      }

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.RUNNING && changedStateEvent.isValue())
          .hasSizeGreaterThanOrEqualTo(1);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.UPDATED && changedStateEvent.isValue())
          .hasSizeGreaterThanOrEqualTo(1);

      Mockito.verify(mJobCreationAndStatusUpdateActivity).jobSuccessWithAttemptNumber(Mockito.any(JobSuccessInputWithAttemptNumber.class));
    }

  }

  @Nested
  @DisplayName("Test that connections are auto disabled if conditions are met")
  class AutoDisableConnection {

    private static final long JOB_ID = 111L;
    private static final int ATTEMPT_ID = 222;

    @BeforeEach
    void setup() {
      setupSimpleConnectionManagerWorkflow();
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that auto disable activity is touched during failure")
    void testAutoDisableOnFailure() throws Exception {
      final UUID connectionId = UUID.randomUUID();
      setupSourceAndDestinationFailure(connectionId);

      Mockito.verify(mJobCreationAndStatusUpdateActivity, atLeastOnce()).attemptFailureWithAttemptNumber(Mockito.any());
      Mockito.verify(mJobCreationAndStatusUpdateActivity, atLeastOnce()).jobFailure(Mockito.any());
      final AutoDisableConnectionActivityInput autoDisableConnectionActivityInput = new AutoDisableConnectionActivityInput();
      autoDisableConnectionActivityInput.setConnectionId(connectionId);
      Mockito.verify(mAutoDisableConnectionActivity).autoDisableFailingConnection(autoDisableConnectionActivityInput);
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that auto disable activity is not touched during job success")
    void testNoAutoDisableOnSuccess() throws Exception {
      returnTrueForLastJobOrAttemptFailure();
      final Worker syncWorker = testEnv.newWorker(TemporalJobType.SYNC.name());
      syncWorker.registerWorkflowImplementationTypes(EmptySyncWorkflow.class);
      final Worker checkWorker = testEnv.newWorker(TemporalJobType.CHECK_CONNECTION.name());
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionSuccessWorkflow.class);
      testEnv.start();

      final UUID testId = UUID.randomUUID();
      final UUID connectionId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);
      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          connectionId,
          JOB_ID,
          ATTEMPT_ID,
          false,
          0,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1));

      workflow.submitManualSync();

      Mockito.verifyNoInteractions(mAutoDisableConnectionActivity);
    }

  }

  @Nested
  @DisplayName("Test that sync workflow failures are recorded")
  class SyncWorkflowReplicationFailuresRecorded {

    private static final long JOB_ID = 111L;
    private static final int ATTEMPT_ID = 222;

    @BeforeEach
    void setup() {
      setupSimpleConnectionManagerWorkflow();
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that Source CHECK failures are recorded")
    void testSourceCheckFailuresRecorded() throws Exception {
      returnTrueForLastJobOrAttemptFailure();
      when(mJobCreationAndStatusUpdateActivity.createNewJob(Mockito.any()))
          .thenReturn(new JobCreationOutput(JOB_ID));
      when(mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(Mockito.any()))
          .thenReturn(new AttemptNumberCreationOutput(ATTEMPT_ID));

      final Worker checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name());
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionFailedWorkflow.class);

      testEnv.start();

      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);
      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1));

      workflow.submitManualSync();

      Mockito.verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
          .attemptFailureWithAttemptNumber(Mockito.argThat(new HasFailureFromOriginWithType(FailureOrigin.SOURCE, FailureType.CONFIG_ERROR)));
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that Source CHECK failures are recorded when running in child workflow")
    void testSourceCheckInChildWorkflowFailuresRecorded() throws Exception {
      returnTrueForLastJobOrAttemptFailure();
      when(mJobCreationAndStatusUpdateActivity.createNewJob(Mockito.any()))
          .thenReturn(new JobCreationOutput(JOB_ID));
      when(mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(Mockito.any()))
          .thenReturn(new AttemptNumberCreationOutput(ATTEMPT_ID));
      final Worker checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name());
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionFailedWorkflow.class);
      testEnv.start();

      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);
      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1));

      workflow.submitManualSync();

      Mockito.verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
          .attemptFailureWithAttemptNumber(Mockito.argThat(new HasFailureFromOriginWithType(FailureOrigin.SOURCE, FailureType.CONFIG_ERROR)));
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that Source CHECK failure reasons are recorded")
    void testSourceCheckFailureReasonsRecorded() throws Exception {
      returnTrueForLastJobOrAttemptFailure();
      when(mJobCreationAndStatusUpdateActivity.createNewJob(Mockito.any()))
          .thenReturn(new JobCreationOutput(JOB_ID));
      when(mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(Mockito.any()))
          .thenReturn(new AttemptNumberCreationOutput(ATTEMPT_ID));
      final Worker checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name());
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionSystemErrorWorkflow.class);
      testEnv.start();

      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);
      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1));

      workflow.submitManualSync();

      Mockito.verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
          .attemptFailureWithAttemptNumber(Mockito.argThat(new HasFailureFromOriginWithType(FailureOrigin.SOURCE, FailureType.SYSTEM_ERROR)));
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that Destination CHECK failures are recorded")
    void testDestinationCheckFailuresRecorded() throws Exception {
      returnTrueForLastJobOrAttemptFailure();
      when(mJobCreationAndStatusUpdateActivity.createNewJob(Mockito.any()))
          .thenReturn(new JobCreationOutput(JOB_ID));
      when(mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(Mockito.any()))
          .thenReturn(new AttemptNumberCreationOutput(ATTEMPT_ID));
      final Worker checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name());
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionSourceSuccessOnlyWorkflow.class);

      testEnv.start();

      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);
      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1));

      workflow.submitManualSync();

      Mockito.verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
          .attemptFailureWithAttemptNumber(Mockito.argThat(new HasFailureFromOriginWithType(FailureOrigin.DESTINATION, FailureType.CONFIG_ERROR)));
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that Destination CHECK failure reasons are recorded")
    void testDestinationCheckFailureReasonsRecorded() throws Exception {
      returnTrueForLastJobOrAttemptFailure();
      when(mJobCreationAndStatusUpdateActivity.createNewJob(Mockito.any()))
          .thenReturn(new JobCreationOutput(JOB_ID));
      when(mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(Mockito.any()))
          .thenReturn(new AttemptNumberCreationOutput(ATTEMPT_ID));
      final Worker checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name());
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionDestinationSystemErrorWorkflow.class);

      testEnv.start();

      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);
      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1));

      workflow.submitManualSync();

      Mockito.verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
          .attemptFailureWithAttemptNumber(Mockito.argThat(new HasFailureFromOriginWithType(FailureOrigin.DESTINATION, FailureType.SYSTEM_ERROR)));
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that reset workflows do not CHECK the source")
    void testSourceCheckSkippedWhenReset() throws Exception {

      when(mJobCreationAndStatusUpdateActivity.isLastJobOrAttemptFailure(Mockito.any()))
          .thenReturn(true);

      final JobRunConfig jobRunConfig = new JobRunConfig();
      jobRunConfig.setJobId(Long.toString(JOB_ID));
      jobRunConfig.setAttemptId((long) ATTEMPT_ID);
      when(mGenerateInputActivityImpl.getSyncWorkflowInputWithAttemptNumber(Mockito.any(SyncInputWithAttemptNumber.class)))
          .thenReturn(
              new JobInput(
                  jobRunConfig,
                  new IntegrationLauncherConfig().withDockerImage(SOURCE_DOCKER_IMAGE),
                  new IntegrationLauncherConfig(),
                  new StandardSyncInput()));

      when(mJobCreationAndStatusUpdateActivity.createNewJob(Mockito.any()))
          .thenReturn(new JobCreationOutput(JOB_ID));
      when(mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(Mockito.any()))
          .thenReturn(new AttemptNumberCreationOutput(ATTEMPT_ID));
      mockResetJobInput(jobRunConfig);
      final Worker checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name());
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionFailedWorkflow.class);

      testEnv.start();

      final UUID testId = UUID.randomUUID();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);
      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1));

      workflow.submitManualSync();

      Mockito.verify(mJobCreationAndStatusUpdateActivity, timeout(TEN_SECONDS).atLeastOnce())
          .attemptFailureWithAttemptNumber(Mockito.argThat(new HasFailureFromOriginWithType(FailureOrigin.DESTINATION, FailureType.CONFIG_ERROR)));
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Test that source and destination failures are recorded")
    void testSourceAndDestinationFailuresRecorded() throws Exception {
      setupSourceAndDestinationFailure(UUID.randomUUID());

      Mockito.verify(mJobCreationAndStatusUpdateActivity)
          .attemptFailureWithAttemptNumber(Mockito.argThat(new HasFailureFromOrigin(FailureOrigin.SOURCE)));
      Mockito.verify(mJobCreationAndStatusUpdateActivity)
          .attemptFailureWithAttemptNumber(Mockito.argThat(new HasFailureFromOrigin(FailureOrigin.DESTINATION)));
    }

  }

  @Nested
  @DisplayName("Test that the workflow is properly restarted after activity failures.")
  class FailedActivityWorkflow {

    @ParameterizedTest
    @MethodSource("getSetupFailingActivity")
    void testWorkflowRestartedAfterFailedActivity(final Thread mockSetup, final int expectedEventsCount) throws Exception {
      returnTrueForLastJobOrAttemptFailure();
      mockSetup.run();
      when(mConfigFetchActivity.getTimeToWait(Mockito.any())).thenReturn(new ScheduleRetrieverOutput(
          Duration.ZERO));

      final UUID testId = UUID.randomUUID();
      TestStateListener.reset();
      final TestStateListener testStateListener = new TestStateListener();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final ConnectionUpdaterInput input = new ConnectionUpdaterInput(
          UUID.randomUUID(),
          null,
          null,
          false,
          1,
          workflowState,
          false, false, false);

      startWorkflowAndWaitUntilReady(workflow, input);

      // Sleep test env for restart delay, plus a small buffer to ensure that the workflow executed the
      // logic after the delay
      testEnv.sleep(WORKFLOW_FAILURE_RESTART_DELAY.plus(Duration.ofSeconds(10)));

      final Queue<ChangedStateEvent> events = testStateListener.events(testId);

      final var filteredAssertionList = Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.RUNNING && changedStateEvent.isValue());

      if (expectedEventsCount == 0) {
        filteredAssertionList.isEmpty();
      } else {
        filteredAssertionList.hasSizeGreaterThanOrEqualTo(expectedEventsCount);
      }

      assertWorkflowWasContinuedAsNew();
    }

    @BeforeEach
    void setup() {
      setupSpecificChildWorkflow(SleepingSyncWorkflow.class, CheckConnectionSuccessWorkflow.class);
    }

    static Stream<Arguments> getSetupFailingActivity() {
      return Stream.of(
          Arguments.of(new Thread(() -> when(mJobCreationAndStatusUpdateActivity.createNewJob(Mockito.any()))
              .thenThrow(ApplicationFailure.newNonRetryableFailure("", ""))), 0),
          Arguments.of(new Thread(() -> when(mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(Mockito.any()))
              .thenThrow(ApplicationFailure.newNonRetryableFailure("", ""))), 0),
          Arguments.of(new Thread(() -> Mockito.doThrow(ApplicationFailure.newNonRetryableFailure("", ""))
              .when(mJobCreationAndStatusUpdateActivity).reportJobStart(Mockito.any())), 0),
          Arguments.of(new Thread(
              () -> {
                when(mGenerateInputActivityImpl.getCheckConnectionInputs(Mockito.any(SyncInputWithAttemptNumber.class)))
                    .thenThrow(ApplicationFailure.newNonRetryableFailure("", ""));
              }),
              1),
          Arguments.of(new Thread(
              () -> {
                try {
                  when(mGenerateInputActivityImpl.getSyncWorkflowInputWithAttemptNumber(Mockito.any(SyncInputWithAttemptNumber.class)))
                      .thenThrow(ApplicationFailure.newNonRetryableFailure("", ""));
                } catch (final Exception e) {
                  throw new RuntimeException(e);
                }
              }),
              1));
    }

  }

  @Nested
  @DisplayName("New 'resilient' retries and progress checking")
  class Retries {

    @BeforeEach
    void setup() {
      setupSimpleConnectionManagerWorkflow();
    }

    @ParameterizedTest
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("We check the progress of the last attempt on failure")
    @MethodSource("coreFailureTypesMatrix")
    void checksProgressOnFailure(final Class<? extends SyncWorkflow> failureCase) throws Exception {
      // We check attempt progress using the 0-based attempt number counting system used everywhere except
      // the ConnectionUpdaterInput where it is 1-based. This will be fixed to be more consistent later.
      // The concrete value passed here is inconsequential—the important part is that it is _not_ the
      // attempt number set on the ConnectionUpdaterInput.
      final var attemptNumber = 42;
      when(mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(Mockito.any()))
          .thenReturn(new AttemptNumberCreationOutput(attemptNumber));

      setupFailureCase(failureCase);

      final var captor = ArgumentCaptor.forClass(CheckRunProgressActivity.Input.class);
      Mockito.verify(mCheckRunProgressActivity, Mockito.times(1)).checkProgress(captor.capture());
      Assertions.assertThat(captor.getValue().getJobId()).isEqualTo(JOB_ID);
      Assertions.assertThat(captor.getValue().getAttemptNo()).isEqualTo(attemptNumber);
    }

    @ParameterizedTest
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("We hydrate, persist and use retry manager.")
    @MethodSource("coreFailureTypesMatrix")
    @Disabled("Flaky in CI.")
    void hydratePersistRetryManagerFlow(final Class<? extends SyncWorkflow> failureCase) throws Exception {
      final var connectionId = UUID.randomUUID();
      final var jobId = 32198714L;
      final var input = testInputBuilder();
      input.setConnectionId(connectionId);
      input.setJobId(null);

      final var retryLimit = 2;

      final var manager1 = new RetryManager(null, null, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, retryLimit);
      final var manager2 = new RetryManager(null, null, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, retryLimit, 0, 1, 0, 1);
      final var manager3 = new RetryManager(null, null, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, retryLimit);

      when(mJobCreationAndStatusUpdateActivity.createNewJob(Mockito.any()))
          .thenReturn(new JobCreationOutput(jobId));
      when(mRetryStatePersistenceActivity.hydrateRetryState(Mockito.any()))
          .thenReturn(new HydrateOutput(manager1)) // run 1: pre scheduling
          .thenReturn(new HydrateOutput(manager1)) // run 1: pre run
          .thenReturn(new HydrateOutput(manager2)) // run 2: pre scheduling
          .thenReturn(new HydrateOutput(manager2)) // run 2: pre run
          .thenReturn(new HydrateOutput(manager3)); // run 3: pre run
      when(mCheckRunProgressActivity.checkProgress(Mockito.any()))
          .thenReturn(new CheckRunProgressActivity.Output(true)); // true to hit partial failure limit

      setupFailureCase(failureCase, input);
      // Wait a little extra for resiliency

      final var hydrateCaptor = ArgumentCaptor.forClass(HydrateInput.class);
      final var persistCaptor = ArgumentCaptor.forClass(PersistInput.class);
      // If the test timeouts expire before we wrap around to the backoff/scheduling step it will run
      // exactly twice per attempt.
      // Otherwise, there's 1 extra hydration to resolve backoff.
      Mockito.verify(mRetryStatePersistenceActivity, timeout(TEN_SECONDS).atLeast(2 * retryLimit)).hydrateRetryState(hydrateCaptor.capture());
      Mockito.verify(mCheckRunProgressActivity, timeout(TEN_SECONDS).times(retryLimit)).checkProgress(Mockito.any());
      Mockito.verify(mRetryStatePersistenceActivity, timeout(TEN_SECONDS).times(retryLimit)).persistRetryState(persistCaptor.capture());
      Mockito.verify(mJobCreationAndStatusUpdateActivity, timeout(TEN_SECONDS).times(retryLimit)).createNewAttemptNumber(Mockito.any());

      // run 1: hydrate pre scheduling
      Assertions.assertThat(hydrateCaptor.getAllValues().get(0).getConnectionId()).isEqualTo(connectionId);
      Assertions.assertThat(hydrateCaptor.getAllValues().get(0).getJobId()).isEqualTo(null);
      // run 1: hydrate pre run
      Assertions.assertThat(hydrateCaptor.getAllValues().get(1).getConnectionId()).isEqualTo(connectionId);
      Assertions.assertThat(hydrateCaptor.getAllValues().get(1).getJobId()).isEqualTo(null);
      // run 1: persist
      Assertions.assertThat(persistCaptor.getAllValues().get(0).getConnectionId()).isEqualTo(connectionId);
      Assertions.assertThat(persistCaptor.getAllValues().get(0).getJobId()).isEqualTo(jobId);
      Assertions.assertThat(persistCaptor.getAllValues().get(0).getManager().getSuccessivePartialFailures()).isEqualTo(1);
      Assertions.assertThat(persistCaptor.getAllValues().get(0).getManager().getTotalPartialFailures()).isEqualTo(1);

      // run 2: hydrate pre scheduling
      Assertions.assertThat(hydrateCaptor.getAllValues().get(2).getConnectionId()).isEqualTo(connectionId);
      Assertions.assertThat(hydrateCaptor.getAllValues().get(2).getJobId()).isEqualTo(jobId);
      // run 2: hydrate pre run
      Assertions.assertThat(hydrateCaptor.getAllValues().get(3).getConnectionId()).isEqualTo(connectionId);
      Assertions.assertThat(hydrateCaptor.getAllValues().get(3).getJobId()).isEqualTo(jobId);
      // run 2: persist
      Assertions.assertThat(persistCaptor.getAllValues().get(1).getConnectionId()).isEqualTo(connectionId);
      Assertions.assertThat(persistCaptor.getAllValues().get(1).getJobId()).isEqualTo(jobId);
      Assertions.assertThat(persistCaptor.getAllValues().get(1).getManager().getSuccessivePartialFailures()).isEqualTo(2);
      Assertions.assertThat(persistCaptor.getAllValues().get(1).getManager().getTotalPartialFailures()).isEqualTo(2);
      // run 3: hydrate pre scheduling
      Assertions.assertThat(hydrateCaptor.getAllValues().get(4).getConnectionId()).isEqualTo(connectionId);
      Assertions.assertThat(hydrateCaptor.getAllValues().get(4).getJobId()).isEqualTo(null);
    }

    @ParameterizedTest
    @Timeout(value = 30,
             unit = TimeUnit.SECONDS)
    @DisplayName("We use attempt-based retries when retry manager not present.")
    @MethodSource("coreFailureTypesMatrix")
    void usesAttemptBasedRetriesIfRetryManagerUnset(final Class<? extends SyncWorkflow> failureCase) throws Exception {
      final var connectionId = UUID.randomUUID();
      final var jobId = 32198714L;
      final var input = testInputBuilder();
      input.setConnectionId(connectionId);
      input.setJobId(null);

      final var retryLimit = 1;

      // attempt-based retry configuration
      when(mConfigFetchActivity.getMaxAttempt()).thenReturn(new GetMaxAttemptOutput(retryLimit));

      when(mJobCreationAndStatusUpdateActivity.createNewJob(Mockito.any()))
          .thenReturn(new JobCreationOutput(jobId));
      when(mRetryStatePersistenceActivity.hydrateRetryState(Mockito.any()))
          .thenReturn(new HydrateOutput(null));
      when(mCheckRunProgressActivity.checkProgress(Mockito.any()))
          .thenReturn(new CheckRunProgressActivity.Output(true));

      setupFailureCase(failureCase, input);

      Mockito.verify(mRetryStatePersistenceActivity, Mockito.never()).persistRetryState(Mockito.any());
      Mockito.verify(mJobCreationAndStatusUpdateActivity, Mockito.times(retryLimit)).createNewAttemptNumber(Mockito.any());
    }

    // Since we can't directly unit test the failure path, we enumerate the core failure cases as a
    // proxy. This is deliberately incomplete as the permutations of failure cases is large.
    public static Stream<Arguments> coreFailureTypesMatrix() {
      return Stream.of(
          Arguments.of(SourceAndDestinationFailureSyncWorkflow.class),
          Arguments.of(ReplicateFailureSyncWorkflow.class),
          Arguments.of(SyncWorkflowFailingOutputWorkflow.class));
    }

    @ParameterizedTest
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Uses scheduling resolution if no retry manager.")
    @MethodSource("noBackoffSchedulingMatrix")
    void useSchedulingIfNoRetryManager(final boolean fromFailure, final Duration timeToWait) throws Exception {
      final var timeTilNextScheduledRun = Duration.ofHours(1);
      when(mConfigFetchActivity.getTimeToWait(Mockito.any()))
          .thenReturn(new ScheduleRetrieverOutput(timeTilNextScheduledRun));

      when(mRetryStatePersistenceActivity.hydrateRetryState(Mockito.any()))
          .thenReturn(new HydrateOutput(null));

      final TestStateListener testStateListener = new TestStateListener();
      final var testId = UUID.randomUUID();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final var input = testInputBuilder();
      input.setFromFailure(fromFailure);
      input.setWorkflowState(workflowState);

      setupSuccessfulWorkflow(input);

      testEnv.sleep(timeToWait.plus(Duration.ofSeconds(5)));

      final Queue<ChangedStateEvent> events = testStateListener.events(testId);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.DONE_WAITING && changedStateEvent.isValue())
          .hasSizeGreaterThan(0);
    }

    public static Stream<Arguments> noBackoffSchedulingMatrix() {
      return Stream.of(
          Arguments.of(true, Duration.ZERO),
          Arguments.of(false, Duration.ofHours(1)));
    }

    @Test
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Uses scheduling if not from failure and retry manager present.")
    void useSchedulingIfNotFromFailure() throws Exception {
      final var backoff = Duration.ofMinutes(1);
      final var policy = new BackoffPolicy(backoff, backoff);
      final var manager = new RetryManager(policy, null, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
      manager.setSuccessiveCompleteFailures(1);

      when(mRetryStatePersistenceActivity.hydrateRetryState(Mockito.any()))
          .thenReturn(new HydrateOutput(manager));

      final var timeTilNextScheduledRun = Duration.ofHours(1);
      when(mConfigFetchActivity.getTimeToWait(Mockito.any()))
          .thenReturn(new ScheduleRetrieverOutput(timeTilNextScheduledRun));

      final TestStateListener testStateListener = new TestStateListener();
      final var testId = UUID.randomUUID();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final var input = testInputBuilder();
      input.setFromFailure(false);
      input.setWorkflowState(workflowState);

      setupSuccessfulWorkflow(input);

      final Queue<ChangedStateEvent> events = testStateListener.events(testId);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.DONE_WAITING && changedStateEvent.isValue())
          .hasSize(0);

      testEnv.sleep(timeTilNextScheduledRun.plus(Duration.ofSeconds(5)));

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.DONE_WAITING && changedStateEvent.isValue())
          .hasSizeGreaterThan(0);
    }

    @ParameterizedTest
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Uses backoff policy if present and from failure.")
    @ValueSource(longs = {1, 5, 20, 30, 1439, 21})
    void usesBackoffPolicyIfPresent(final long minutes) throws Exception {
      final var backoff = Duration.ofMinutes(minutes);
      final var policy = new BackoffPolicy(backoff, backoff);
      final var manager = new RetryManager(policy, null, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
      manager.setSuccessiveCompleteFailures(1);

      when(mRetryStatePersistenceActivity.hydrateRetryState(Mockito.any()))
          .thenReturn(new HydrateOutput(manager));

      when(mConfigFetchActivity.getTimeToWait(Mockito.any()))
          .thenReturn(new ScheduleRetrieverOutput(Duration.ofDays(1)));

      final TestStateListener testStateListener = new TestStateListener();
      final var testId = UUID.randomUUID();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final var input = testInputBuilder();
      input.setFromFailure(true);
      input.setWorkflowState(workflowState);

      setupSuccessfulWorkflow(input);

      final Queue<ChangedStateEvent> events = testStateListener.events(testId);

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.DONE_WAITING && changedStateEvent.isValue())
          .hasSize(0);

      testEnv.sleep(backoff.plus(Duration.ofSeconds(5)));

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.DONE_WAITING && changedStateEvent.isValue())
          .hasSizeGreaterThan(0);
    }

    @ParameterizedTest
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Jobs can be cancelled during the backoff.")
    @ValueSource(longs = {1, 5, 20, 30, 1439, 21})
    @Disabled("Flaky in CI")
    void cancelWorksDuringBackoff(final long minutes) throws Exception {
      final var backoff = Duration.ofMinutes(minutes);
      final var policy = new BackoffPolicy(backoff, backoff);
      final var manager = new RetryManager(policy, null, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
      manager.setSuccessiveCompleteFailures(1);

      when(mRetryStatePersistenceActivity.hydrateRetryState(Mockito.any()))
          .thenReturn(new HydrateOutput(manager));

      when(mConfigFetchActivity.getTimeToWait(Mockito.any()))
          .thenReturn(new ScheduleRetrieverOutput(Duration.ofDays(1)));

      final TestStateListener testStateListener = new TestStateListener();
      final var testId = UUID.randomUUID();
      final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

      final var jobId = 124198715L;
      final var attemptNo = 72;

      final var input = testInputBuilder();
      input.setJobId(jobId);
      input.setAttemptNumber(attemptNo);
      input.setFromFailure(true);
      input.setWorkflowState(workflowState);

      setupSuccessfulWorkflow(input);

      final Queue<ChangedStateEvent> events = testStateListener.events(testId);

      workflow.cancelJob();

      testEnv.sleep(Duration.ofMinutes(1));

      Mockito.verify(mJobCreationAndStatusUpdateActivity)
          .jobCancelledWithAttemptNumber(Mockito.argThat(new HasCancellationFailure(jobId, attemptNo - 1))); // input attempt number is 1 based

      Assertions.assertThat(events)
          .filteredOn(changedStateEvent -> changedStateEvent.getField() == StateField.CANCELLED && changedStateEvent.isValue())
          .hasSizeGreaterThan(0);
    }

    @ParameterizedTest
    @Timeout(value = 10,
             unit = TimeUnit.SECONDS)
    @DisplayName("Does not fail job if backoff longer than time til next scheduled run.")
    @MethodSource("backoffJobFailureMatrix")
    void doesNotFailJobIfBackoffTooLong(final long backoffMinutes) throws Exception {
      final var backoff = Duration.ofMinutes(backoffMinutes);
      final var policy = new BackoffPolicy(backoff, backoff);
      final var manager = new RetryManager(policy, null, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
      manager.setSuccessiveCompleteFailures(1);

      when(mRetryStatePersistenceActivity.hydrateRetryState(Mockito.any()))
          .thenReturn(new HydrateOutput(manager));

      final var timeTilNextScheduledRun = Duration.ofMinutes(60);
      when(mConfigFetchActivity.getTimeToWait(Mockito.any()))
          .thenReturn(new ScheduleRetrieverOutput(timeTilNextScheduledRun));

      final var input = testInputBuilder();
      input.setFromFailure(true);

      setupSuccessfulWorkflow(input);
      testEnv.sleep(Duration.ofMinutes(1));

      Mockito.verify(mJobCreationAndStatusUpdateActivity, Mockito.times(0)).jobFailure(Mockito.any());
    }

    private static Stream<Arguments> backoffJobFailureMatrix() {
      return Stream.of(
          Arguments.of(1),
          Arguments.of(10),
          Arguments.of(55),
          Arguments.of(60),
          Arguments.of(123),
          Arguments.of(214),
          Arguments.of(7));
    }

  }

  @Nested
  @DisplayName("General functionality")
  class General {

    @BeforeEach
    void setup() {
      setupSimpleConnectionManagerWorkflow();
    }

    @Test
    @DisplayName("When a sync returns a status of cancelled we report the run as cancelled")
    void reportsCancelledWhenConnectionDisabled() throws Exception {
      final var input = testInputBuilder();
      setupSuccessfulWorkflow(CancelledSyncWorkflow.class, input);
      workflow.submitManualSync();
      testEnv.sleep(Duration.ofSeconds(60));

      Mockito.verify(mJobCreationAndStatusUpdateActivity).jobCancelledWithAttemptNumber(Mockito.any(JobCancelledInputWithAttemptNumber.class));
    }

  }

  private class HasFailureFromOrigin implements ArgumentMatcher<AttemptNumberFailureInput> {

    private final FailureOrigin expectedFailureOrigin;

    HasFailureFromOrigin(final FailureOrigin failureOrigin) {
      this.expectedFailureOrigin = failureOrigin;
    }

    @Override
    public boolean matches(final AttemptNumberFailureInput arg) {
      return arg.getAttemptFailureSummary().getFailures().stream().anyMatch(f -> f.getFailureOrigin().equals(expectedFailureOrigin));
    }

  }

  private class HasFailureFromOriginWithType implements ArgumentMatcher<AttemptNumberFailureInput> {

    private final FailureOrigin expectedFailureOrigin;
    private final FailureType expectedFailureType;

    HasFailureFromOriginWithType(final FailureOrigin failureOrigin, final FailureType failureType) {
      this.expectedFailureOrigin = failureOrigin;
      this.expectedFailureType = failureType;
    }

    @Override
    public boolean matches(final AttemptNumberFailureInput arg) {
      final Stream<FailureReason> stream = arg.getAttemptFailureSummary().getFailures().stream();
      return stream.anyMatch(f -> f.getFailureOrigin().equals(expectedFailureOrigin) && f.getFailureType().equals(expectedFailureType));
    }

  }

  private class HasCancellationFailure implements ArgumentMatcher<JobCancelledInputWithAttemptNumber> {

    private final long expectedJobId;
    private final int expectedAttemptNumber;

    HasCancellationFailure(final long jobId, final int attemptNumber) {
      this.expectedJobId = jobId;
      this.expectedAttemptNumber = attemptNumber;
    }

    @Override
    public boolean matches(final JobCancelledInputWithAttemptNumber arg) {
      return arg.getAttemptFailureSummary().getFailures().stream().anyMatch(f -> f.getFailureType().equals(FailureType.MANUAL_CANCELLATION))
          && arg.getJobId() == expectedJobId && arg.getAttemptNumber() == expectedAttemptNumber;
    }

  }

  private static void startWorkflowAndWaitUntilReady(final ConnectionManagerWorkflow workflow, final ConnectionUpdaterInput input)
      throws InterruptedException {
    WorkflowClient.start(workflow::run, input);

    boolean isReady = false;

    while (!isReady) {
      try {
        isReady = workflow.getState() != null;
      } catch (final Exception e) {
        log.info("retrying...");
        Thread.sleep(100);
      }
    }
  }

  private static void waitUntilDeleted(final ConnectionManagerWorkflow workflow)
      throws InterruptedException {
    boolean isDeleted = false;

    while (!isDeleted) {
      try {
        isDeleted = workflow.getState() != null && workflow.getState().isDeleted();
      } catch (final Exception e) {
        log.info("retrying...");
        Thread.sleep(100);
      }
    }
  }

  private <T1 extends SyncWorkflow, T2 extends ConnectorCommandWorkflow> void setupSpecificChildWorkflow(final Class<T1> mockedSyncedWorkflow,
                                                                                                         final Class<T2> mockedCheckWorkflow) {
    testEnv = TestWorkflowEnvironment.newInstance();

    final Worker syncWorker = testEnv.newWorker(TemporalJobType.SYNC.name());
    syncWorker.registerWorkflowImplementationTypes(mockedSyncedWorkflow);

    final Worker checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name());
    checkWorker.registerWorkflowImplementationTypes(mockedCheckWorkflow);

    final Worker managerWorker = testEnv.newWorker(TemporalJobType.CONNECTION_UPDATER.name());
    managerWorker.registerWorkflowImplementationTypes(temporalProxyHelper.proxyWorkflowClass(ConnectionManagerWorkflowImpl.class));
    managerWorker.registerActivitiesImplementations(mConfigFetchActivity, mGenerateInputActivityImpl,
        mJobCreationAndStatusUpdateActivity, mAutoDisableConnectionActivity, mRecordMetricActivity, mWorkflowConfigActivity,
        mFeatureFlagFetchActivity, mCheckRunProgressActivity, mRetryStatePersistenceActivity, mAppendToAttemptLogActivity);

    client = testEnv.getWorkflowClient();
    testEnv.start();

    workflow = client
        .newWorkflowStub(
            ConnectionManagerWorkflow.class,
            WorkflowOptions.newBuilder()
                .setTaskQueue(TemporalJobType.CONNECTION_UPDATER.name())
                .setWorkflowId(WORKFLOW_ID)
                .build());
  }

  private void assertWorkflowWasContinuedAsNew() {
    final ListClosedWorkflowExecutionsRequest request = ListClosedWorkflowExecutionsRequest.newBuilder()
        .setNamespace(testEnv.getNamespace())
        .setExecutionFilter(WorkflowExecutionFilter.newBuilder().setWorkflowId(WORKFLOW_ID))
        .build();
    final ListClosedWorkflowExecutionsResponse listResponse = testEnv
        .getWorkflowService()
        .blockingStub()
        .listClosedWorkflowExecutions(request);
    Assertions.assertThat(listResponse.getExecutionsCount()).isGreaterThanOrEqualTo(1);
    Assertions.assertThat(listResponse.getExecutionsList().get(0).getStatus())
        .isEqualTo(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW);
  }

  private ConnectionUpdaterInput testInputBuilder() {
    final UUID testId = UUID.randomUUID();
    final TestStateListener testStateListener = new TestStateListener();
    final WorkflowState workflowState = new WorkflowState(testId, testStateListener);

    return new ConnectionUpdaterInput(
        UUID.randomUUID(),
        JOB_ID,
        ATTEMPT_ID,
        false,
        ATTEMPT_NO,
        workflowState,
        false, false, false);
  }

  /**
   * Given a failure case class, this will set up a manual sync to fail in that fashion.
   * ConnectionUpdaterInput is pluggable for various test needs. Feel free to update input/return
   * values as is necessary.
   */
  private void setupFailureCase(final Class<? extends SyncWorkflow> failureClass, final ConnectionUpdaterInput input) throws Exception {
    returnTrueForLastJobOrAttemptFailure();
    final Worker syncWorker = testEnv.newWorker(TemporalJobType.SYNC.name());
    syncWorker.registerWorkflowImplementationTypes(failureClass);

    final Worker checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name());
    checkWorker.registerWorkflowImplementationTypes(CheckConnectionSuccessWorkflow.class);

    testEnv.start();

    startWorkflowAndWaitUntilReady(workflow, input);

    // wait for workflow to initialize
    testEnv.sleep(Duration.ofMinutes(1));

    workflow.submitManualSync();
    testEnv.sleep(Duration.ofMinutes(1));
  }

  private void setupFailureCase(final Class<? extends SyncWorkflow> failureClass) throws Exception {
    final var input = testInputBuilder();

    setupFailureCase(failureClass, input);
  }

  private void setupSourceAndDestinationFailure(final UUID connectionId) throws Exception {
    final ConnectionUpdaterInput input = testInputBuilder();
    input.setConnectionId(connectionId);

    setupFailureCase(SourceAndDestinationFailureSyncWorkflow.class, input);
  }

  /**
   * Does all the legwork for setting up a workflow for simple runs. NOTE: Don't forget to add your
   * mock activity below.
   */
  private void setupSimpleConnectionManagerWorkflow() {
    testEnv = TestWorkflowEnvironment.newInstance();

    final Worker managerWorker = testEnv.newWorker(TemporalJobType.CONNECTION_UPDATER.name());
    managerWorker.registerWorkflowImplementationTypes(temporalProxyHelper.proxyWorkflowClass(ConnectionManagerWorkflowImpl.class));
    managerWorker.registerActivitiesImplementations(mConfigFetchActivity, mGenerateInputActivityImpl,
        mJobCreationAndStatusUpdateActivity, mAutoDisableConnectionActivity, mRecordMetricActivity, mWorkflowConfigActivity,
        mFeatureFlagFetchActivity, mCheckRunProgressActivity, mRetryStatePersistenceActivity, mAppendToAttemptLogActivity);

    client = testEnv.getWorkflowClient();
    workflow = client.newWorkflowStub(ConnectionManagerWorkflow.class,
        WorkflowOptions.newBuilder().setTaskQueue(TemporalJobType.CONNECTION_UPDATER.name()).build());
  }

  private void setupSuccessfulWorkflow(final ConnectionUpdaterInput input) throws Exception {
    setupSuccessfulWorkflow(EmptySyncWorkflow.class, input);
  }

  private <T> void setupSuccessfulWorkflow(final Class<T> syncWorkflowMockClass, final ConnectionUpdaterInput input) throws Exception {
    returnTrueForLastJobOrAttemptFailure();
    final Worker syncWorker = testEnv.newWorker(TemporalJobType.SYNC.name());
    syncWorker.registerWorkflowImplementationTypes(syncWorkflowMockClass);
    final Worker checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name());
    checkWorker.registerWorkflowImplementationTypes(CheckConnectionSuccessWorkflow.class);
    testEnv.start();

    startWorkflowAndWaitUntilReady(workflow, input);
  }

}
