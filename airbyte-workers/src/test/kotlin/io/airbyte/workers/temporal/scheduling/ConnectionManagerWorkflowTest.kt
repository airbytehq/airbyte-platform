/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling

import io.airbyte.commons.temporal.TemporalJobType
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow
import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2
import io.airbyte.commons.temporal.scheduling.retries.BackoffPolicy
import io.airbyte.commons.temporal.scheduling.retries.RetryManager
import io.airbyte.commons.temporal.scheduling.state.WorkflowState
import io.airbyte.commons.temporal.scheduling.state.listener.TestStateListener
import io.airbyte.commons.temporal.scheduling.state.listener.TestStateListener.Companion.reset
import io.airbyte.commons.temporal.scheduling.state.listener.WorkflowStateChangedListener.ChangedStateEvent
import io.airbyte.commons.temporal.scheduling.state.listener.WorkflowStateChangedListener.StateField
import io.airbyte.config.ConnectionContext
import io.airbyte.micronaut.temporal.TemporalProxyHelper
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.temporal.activities.GetConnectionContextOutput
import io.airbyte.workers.temporal.activities.GetLoadShedBackoffOutput
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity.LogOutput
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivityImpl
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionActivityInput
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionOutput
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivityImpl
import io.airbyte.workers.temporal.scheduling.activities.CheckRunProgressActivity
import io.airbyte.workers.temporal.scheduling.activities.CheckRunProgressActivityImpl
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.GetMaxAttemptOutput
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.ScheduleRetrieverInput
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.ScheduleRetrieverOutput
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivityImpl
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity.FeatureFlagFetchOutput
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivityImpl
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptNumberCreationOutput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCreationOutput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivityImpl
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivityImpl
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.HydrateInput
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.HydrateOutput
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.PersistInput
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.PersistOutput
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivityImpl
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivity
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivityImpl
import io.airbyte.workers.temporal.scheduling.activities.WorkflowConfigActivity
import io.airbyte.workers.temporal.scheduling.activities.WorkflowConfigActivityImpl
import io.airbyte.workers.temporal.scheduling.testcheckworkflow.CheckConnectionDestinationSystemErrorWorkflow
import io.airbyte.workers.temporal.scheduling.testcheckworkflow.CheckConnectionFailedWorkflow
import io.airbyte.workers.temporal.scheduling.testcheckworkflow.CheckConnectionSourceSuccessOnlyWorkflow
import io.airbyte.workers.temporal.scheduling.testcheckworkflow.CheckConnectionSuccessWorkflow
import io.airbyte.workers.temporal.scheduling.testcheckworkflow.CheckConnectionSystemErrorWorkflow
import io.airbyte.workers.temporal.scheduling.testsyncworkflow.CancelledSyncWorkflow
import io.airbyte.workers.temporal.scheduling.testsyncworkflow.EmptySyncWorkflow
import io.airbyte.workers.temporal.scheduling.testsyncworkflow.ReplicateFailureSyncWorkflow
import io.airbyte.workers.temporal.scheduling.testsyncworkflow.SleepingSyncWorkflow
import io.airbyte.workers.temporal.scheduling.testsyncworkflow.SourceAndDestinationFailureSyncWorkflow
import io.airbyte.workers.temporal.scheduling.testsyncworkflow.SyncWorkflowFailingOutputWorkflow
import io.airbyte.workers.temporal.workflows.JobPostProcessingWorkflowStub
import io.airbyte.workers.temporal.workflows.ValidateJobPostProcessingWorkflowStartedActivity
import io.micronaut.context.BeanRegistration
import io.micronaut.inject.BeanIdentifier
import io.mockk.Called
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import io.mockk.verify
import io.temporal.activity.ActivityOptions
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.filter.v1.WorkflowExecutionFilter
import io.temporal.api.workflowservice.v1.ListClosedWorkflowExecutionsRequest
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.common.RetryOptions
import io.temporal.failure.ApplicationFailure
import io.temporal.testing.TestWorkflowEnvironment
import org.assertj.core.api.AbstractCollectionAssert
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.time.Duration
import java.util.Queue
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.function.Predicate

// Forcing SAME_THREAD execution as we seem to face the issues described in
// https://github.com/mockito/mockito/wiki/FAQ#is-mockito-thread-safe

/**
 * Tests the core state machine of the connection manager workflow.
 *
 *
 * We've had race conditions in this in the past which is why (after addressing them) we have
 * repeated cases, just in case there's a regression where a race condition is added back to a test.
 */
@Execution(ExecutionMode.SAME_THREAD)
internal class ConnectionManagerWorkflowTest {
  private val mConfigFetchActivity: ConfigFetchActivity = mockk<ConfigFetchActivityImpl>(relaxed = true)
  private val mJobPostProcessingActivity: ValidateJobPostProcessingWorkflowStartedActivity =
    mockk<ValidateJobPostProcessingWorkflowStartedActivityImpl>(relaxed = true)
  private lateinit var testEnv: TestWorkflowEnvironment
  private lateinit var client: WorkflowClient
  private lateinit var workflow: ConnectionManagerWorkflow
  private lateinit var activityOptions: ActivityOptions
  private lateinit var temporalProxyHelper: TemporalProxyHelper

  // Internal class to be able to mock the ValidateJobPostProcessingWorkflowStartedActivity class.  This is to
  // avoid the annotations on the interface being present in the mock, which Temporal does not like
  internal class ValidateJobPostProcessingWorkflowStartedActivityImpl : ValidateJobPostProcessingWorkflowStartedActivity {
    override fun wasStarted() {}
  }

  @BeforeEach
  fun setUp() {
    clearMocks(mConfigFetchActivity)
    clearMocks(mJobCreationAndStatusUpdateActivity)
    clearMocks(mAutoDisableConnectionActivity)
    clearMocks(mStreamResetActivity)
    clearMocks(mRecordMetricActivity)
    clearMocks(mWorkflowConfigActivity)
    clearMocks(mCheckRunProgressActivity)
    clearMocks(mRetryStatePersistenceActivity)
    clearMocks(mAppendToAttemptLogActivity)

    // default is to wait "forever"
    every { mConfigFetchActivity.getTimeToWait(any()) } returns
      ScheduleRetrieverOutput(
        Duration.ofDays((100 * 365).toLong()),
      )

    // default is to not back off
    every { mConfigFetchActivity.getLoadShedBackoff(any()) } returns
      GetLoadShedBackoffOutput(
        Duration.ZERO,
      )

    every { mConfigFetchActivity.getConnectionContext(any()) } returns
      GetConnectionContextOutput(
        ConnectionContext().withSourceId(SOURCE_ID).withDestinationId(DESTINATION_ID),
      )

    every { mJobCreationAndStatusUpdateActivity.createNewJob(any()) } returns
      JobCreationOutput(1L)

    every { mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()) } returns
      AttemptNumberCreationOutput(1)

    every { mAutoDisableConnectionActivity.autoDisableFailingConnection(any()) } returns
      AutoDisableConnectionOutput(false)

    every { mWorkflowConfigActivity.getWorkflowRestartDelaySeconds() } returns
      WORKFLOW_FAILURE_RESTART_DELAY
    every { mFeatureFlagFetchActivity.getFeatureFlags(any()) } returns
      FeatureFlagFetchOutput(mutableMapOf())
    every { mCheckRunProgressActivity.checkProgress(any()) } returns
      CheckRunProgressActivity.Output(false) // false == complete failure
    // just run once
    val manager = RetryManager(null, null, Int.MAX_VALUE, Int.MAX_VALUE, 1, Int.MAX_VALUE)

    every { mRetryStatePersistenceActivity.hydrateRetryState(any()) } returns
      HydrateOutput(manager)
    every { mRetryStatePersistenceActivity.persistRetryState(any()) } returns
      PersistOutput(true)
    every { mAppendToAttemptLogActivity.log(any()) } returns
      LogOutput(true)

    activityOptions =
      ActivityOptions
        .newBuilder()
        .setHeartbeatTimeout(Duration.ofSeconds(30))
        .setStartToCloseTimeout(Duration.ofSeconds(120))
        .setRetryOptions(
          RetryOptions
            .newBuilder()
            .setMaximumAttempts(5)
            .setInitialInterval(Duration.ofSeconds(30))
            .setMaximumInterval(Duration.ofSeconds(600))
            .build(),
        ).build()

    val activityOptionsBeanIdentifier = mockk<BeanIdentifier>()
    val activityOptionsBeanRegistration = mockk<BeanRegistration<ActivityOptions>>()
    every { activityOptionsBeanIdentifier.name } returns "shortActivityOptions"
    every { activityOptionsBeanRegistration.getIdentifier() } returns activityOptionsBeanIdentifier
    every { activityOptionsBeanRegistration.getBean() } returns activityOptions
    temporalProxyHelper = TemporalProxyHelper(listOf(activityOptionsBeanRegistration))
  }

  private fun returnTrueForLastJobOrAttemptFailure() {
    every { mJobCreationAndStatusUpdateActivity.shouldRunSourceCheck(any()) } returns true

    every { mJobCreationAndStatusUpdateActivity.shouldRunDestinationCheck(any()) } returns true

    val jobRunConfig = JobRunConfig()
    jobRunConfig.jobId = JOB_ID.toString()
    jobRunConfig.attemptId = ATTEMPT_ID.toLong()
  }

  @AfterEach
  fun tearDown() {
    testEnv.shutdown()
    reset()
  }

  @Nested
  @DisplayName("Test which without a long running child workflow")
  internal inner class AsynchronousWorkflow {
    @BeforeEach
    fun setup() {
      setupSpecificChildWorkflow(
        EmptySyncWorkflow::class.java,
        CheckConnectionSuccessWorkflow::class.java,
      )
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that a successful workflow restarts waits")
    fun runSuccess() {
      returnTrueForLastJobOrAttemptFailure()
      every { mConfigFetchActivity.getTimeToWait(any()) } returns ScheduleRetrieverOutput(SCHEDULE_WAIT)

      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)

      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)
      // wait to be scheduled, then to run, then schedule again
      testEnv.sleep(Duration.ofMinutes(SCHEDULE_WAIT.toMinutes() + SCHEDULE_WAIT.toMinutes() + 1))
      verify(atLeast = 2) { mConfigFetchActivity.getTimeToWait(any()) }
      val events: Queue<ChangedStateEvent> = testStateListener.events(testId)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.RUNNING &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThan(0)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.DONE_WAITING &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThan(0)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            (
              changedStateEvent!!.field != StateField.RUNNING &&
                changedStateEvent.field != StateField.SUCCESS &&
                changedStateEvent.field != StateField.DONE_WAITING
            ) &&
              changedStateEvent.isValue
          },
        ).isEmpty()
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test workflow does not wait to run after a failure")
    fun retryAfterFail() {
      returnTrueForLastJobOrAttemptFailure()
      every { mConfigFetchActivity.getTimeToWait(any()) } returns ScheduleRetrieverOutput(SCHEDULE_WAIT)

      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)

      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = true,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)
      testEnv.sleep(Duration.ofMinutes(SCHEDULE_WAIT.toMinutes() - 1))
      val events: Queue<ChangedStateEvent> = testStateListener.events(testId)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.RUNNING &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThan(0)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.DONE_WAITING &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThan(0)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            (
              changedStateEvent!!.field != StateField.RUNNING &&
                changedStateEvent.field != StateField.SUCCESS &&
                changedStateEvent.field != StateField.DONE_WAITING
            ) &&
              changedStateEvent.isValue
          },
        ).isEmpty()
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test workflow which receives a manual run signal stops waiting")
    fun manualRun() {
      returnTrueForLastJobOrAttemptFailure()
      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)

      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)
      testEnv.sleep(Duration.ofMinutes(1L)) // any value here, just so it's started
      workflow.submitManualSync()

      verify(timeout = TEN_SECONDS.toLong(), atLeast = 1) {
        mJobCreationAndStatusUpdateActivity.jobSuccessWithAttemptNumber(any())
      }

      val events: Queue<ChangedStateEvent> = testStateListener.events(testId)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.RUNNING &&
              changedStateEvent.isValue
          },
        ).hasSize(1)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.SKIPPED_SCHEDULING &&
              changedStateEvent.isValue
          },
        ).hasSize(1)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.DONE_WAITING &&
              changedStateEvent.isValue
          },
        ).hasSize(1)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            (
              changedStateEvent!!.field != StateField.RUNNING &&
                changedStateEvent.field != StateField.SKIPPED_SCHEDULING &&
                changedStateEvent.field != StateField.SUCCESS &&
                changedStateEvent.field != StateField.DONE_WAITING
            ) &&
              changedStateEvent.isValue
          },
        ).isEmpty()
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test workflow which receives an update signal stops waiting, doesn't run, and doesn't update the job status")
    fun updatedSignalReceived() {
      returnTrueForLastJobOrAttemptFailure()
      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)

      val input =
        ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)
      testEnv.sleep(Duration.ofSeconds(30L))

      clearMocks(mConfigFetchActivity)
      every { mConfigFetchActivity.getConnectionContext(any()) } returns
        GetConnectionContextOutput(
          ConnectionContext().withSourceId(SOURCE_ID).withDestinationId(DESTINATION_ID),
        )
      every { mConfigFetchActivity.getLoadShedBackoff(any()) } returns
        GetLoadShedBackoffOutput(
          Duration.ZERO,
        )
      every { mConfigFetchActivity.getTimeToWait(any()) } returns
        ScheduleRetrieverOutput(
          Duration.ofDays((100 * 365).toLong()),
        )
      workflow.connectionUpdated()

      verify(timeout = TEN_SECONDS.toLong(), atLeast = 1) {
        mConfigFetchActivity.getTimeToWait(any())
      }

      val events: Queue<ChangedStateEvent> = testStateListener.events(testId)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.RUNNING &&
              changedStateEvent.isValue
          },
        ).isEmpty()

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.UPDATED &&
              changedStateEvent.isValue
          },
        ).hasSize(1)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.DONE_WAITING &&
              changedStateEvent.isValue
          },
        ).hasSize(1)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            (
              changedStateEvent!!.field != StateField.UPDATED &&
                changedStateEvent.field != StateField.SUCCESS &&
                changedStateEvent.field != StateField.DONE_WAITING
            ) &&
              changedStateEvent.isValue
          },
        ).isEmpty()

      verify { mJobCreationAndStatusUpdateActivity wasNot Called }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that cancelling a non-running workflow doesn't do anything")
    fun cancelNonRunning() {
      returnTrueForLastJobOrAttemptFailure()
      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)

      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)
      testEnv.sleep(Duration.ofSeconds(30L))
      workflow.cancelJob()
      testEnv.sleep(Duration.ofSeconds(20L))

      val events: Queue<ChangedStateEvent> = testStateListener.events(testId)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.RUNNING &&
              changedStateEvent.isValue
          },
        ).isEmpty()

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.CANCELLED &&
              changedStateEvent.isValue
          },
        ).isEmpty()

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            (changedStateEvent!!.field != StateField.CANCELLED && changedStateEvent.field != StateField.SUCCESS) &&
              changedStateEvent.isValue
          },
        ).isEmpty()

      verify { mJobCreationAndStatusUpdateActivity wasNot Called }
    }

    // TODO: delete when the signal method can be removed
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that the sync is properly deleted")
    fun deleteSync() {
      returnTrueForLastJobOrAttemptFailure()
      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = spyk(WorkflowState(testId, testStateListener))

      val input =
        ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)
      testEnv.sleep(Duration.ofSeconds(30L))
      workflow.deleteConnection()

      waitUntilDeleted(workflow)

      val events: Queue<ChangedStateEvent> = testStateListener.events(testId)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.RUNNING &&
              changedStateEvent.isValue
          },
        ).isEmpty()

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.DELETED &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThan(1)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.DONE_WAITING &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThan(1)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field != StateField.DELETED &&
              changedStateEvent.field != StateField.SUCCESS &&
              changedStateEvent.field != StateField.DONE_WAITING &&
              changedStateEvent.isValue
          },
        ).isEmpty()
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that fresh workflow cleans the job state")
    fun testStartFromCleanJobState() {
      returnTrueForLastJobOrAttemptFailure()
      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = null,
          attemptId = null,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = null,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)
      testEnv.sleep(Duration.ofSeconds(30L))

      verify(exactly = 1) {
        mJobCreationAndStatusUpdateActivity.ensureCleanJobState(any())
      }
    }
  }

  @Nested
  @DisplayName("Test which with a long running child workflow")
  internal inner class SynchronousWorkflow {
    @BeforeEach
    fun setup() {
      setupSpecificChildWorkflow(
        SleepingSyncWorkflow::class.java,
        CheckConnectionSuccessWorkflow::class.java,
      )
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test workflow which receives a manual sync while running a scheduled sync does nothing")
    fun manualRun() {
      returnTrueForLastJobOrAttemptFailure()
      every { mConfigFetchActivity.getTimeToWait(any()) } returns ScheduleRetrieverOutput(SCHEDULE_WAIT)

      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)

      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)

      // wait until the middle of the run
      testEnv.sleep(Duration.ofMinutes(SCHEDULE_WAIT.toMinutes() + SleepingSyncWorkflow.RUN_TIME!!.toMinutes() / 2))

      // trigger the manual sync
      workflow.submitManualSync()

      // wait for the rest of the workflow
      testEnv.sleep(Duration.ofMinutes(SleepingSyncWorkflow.RUN_TIME.toMinutes() / 2 + 1))

      val events: Queue<ChangedStateEvent> = testStateListener.events(testId)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.SKIPPED_SCHEDULING &&
              changedStateEvent.isValue
          },
        ).isEmpty()
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that cancelling a running workflow cancels the sync")
    fun cancelRunning() {
      returnTrueForLastJobOrAttemptFailure()
      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)

      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      // wait for the manual sync to start working
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.cancelJob()

      verify(timeout = TEN_SECONDS.toLong(), atLeast = 1) {
        mJobCreationAndStatusUpdateActivity.jobCancelledWithAttemptNumber(any())
      }

      val eventQueue: Queue<ChangedStateEvent> = testStateListener.events(testId)
      val events: MutableList<ChangedStateEvent> = ArrayList(eventQueue)

      for (event in events) {
        if (event.isValue) {
          log.info("${EVENT}event")
        }
      }

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.CANCELLED &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThanOrEqualTo(1)
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that resetting a non-running workflow starts a reset job")
    fun resetStart() {
      returnTrueForLastJobOrAttemptFailure()
      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)

      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)
      testEnv.sleep(Duration.ofMinutes(5L))
      workflow.resetConnection()
      verify(timeout = TEN_SECONDS.toLong(), atLeast = 1) {
        mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any())
      }

      val events: Queue<ChangedStateEvent> = testStateListener.events(testId)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.SKIPPED_SCHEDULING &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThanOrEqualTo(1)
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that resetting a non-running workflow starts a reset job")
    fun resetAndContinue() {
      returnTrueForLastJobOrAttemptFailure()
      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)

      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)
      testEnv.sleep(Duration.ofMinutes(5L))
      workflow.resetConnectionAndSkipNextScheduling()
      verify(timeout = TEN_SECONDS.toLong(), atLeast = 1) {
        mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any())
      }

      val events: Queue<ChangedStateEvent> = testStateListener.events(testId)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.SKIPPED_SCHEDULING &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThanOrEqualTo(1)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.SKIP_SCHEDULING_NEXT_WORKFLOW &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThanOrEqualTo(1)
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Test that resetting a running workflow cancels the running workflow")
    fun resetCancelRunningWorkflow() {
      returnTrueForLastJobOrAttemptFailure()
      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)

      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)

      testEnv.sleep(Duration.ofSeconds(30L))
      workflow.submitManualSync()
      testEnv.sleep(Duration.ofSeconds(30L))
      workflow.resetConnection()

      verify(timeout = TEN_SECONDS.toLong(), atLeast = 1) {
        mJobCreationAndStatusUpdateActivity.jobCancelledWithAttemptNumber(any())
      }

      val eventQueue: Queue<ChangedStateEvent> = testStateListener.events(testId)
      val events: MutableList<ChangedStateEvent> = ArrayList(eventQueue)

      for (event in events) {
        if (event.isValue) {
          log.info("${EVENT}event")
        }
      }

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.CANCELLED_FOR_RESET &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThanOrEqualTo(1)
    }

    @Test
    @DisplayName("Test that running workflow which receives an update signal waits for the current run and reports the job status")
    fun updatedSignalReceivedWhileRunning() {
      returnTrueForLastJobOrAttemptFailure()
      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)

      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      // submit sync
      workflow.submitManualSync()

      // wait until the middle of the manual run
      testEnv.sleep(Duration.ofMinutes(SleepingSyncWorkflow.RUN_TIME!!.toMinutes() / 2))

      // indicate connection update
      workflow.connectionUpdated()

      // wait after the rest of the run
      testEnv.sleep(Duration.ofMinutes(SleepingSyncWorkflow.RUN_TIME.toMinutes() / 2 + 1))

      val eventQueue: Queue<ChangedStateEvent> = testStateListener.events(testId)
      val events: MutableList<ChangedStateEvent> = ArrayList(eventQueue)

      for (event in events) {
        if (event.isValue) {
          log.info(EVENT + event)
        }
      }

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.RUNNING &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThanOrEqualTo(1)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.UPDATED &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThanOrEqualTo(1)

      verify {
        mJobCreationAndStatusUpdateActivity.jobSuccessWithAttemptNumber(any())
      }
    }
  }

  @Nested
  @DisplayName("Test that connections are auto disabled if conditions are met")
  internal inner class AutoDisableConnection {
    @BeforeEach
    fun setup() {
      setupSimpleConnectionManagerWorkflow()
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that auto disable activity is touched during failure")
    fun testAutoDisableOnFailure() {
      val connectionId = UUID.randomUUID()
      setupSourceAndDestinationFailure(connectionId)

      verify(atLeast = 1) {
        mJobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(any())
      }
      verify(atLeast = 1) {
        mJobCreationAndStatusUpdateActivity.jobFailure(any())
      }
      val autoDisableConnectionActivityInput = AutoDisableConnectionActivityInput()
      autoDisableConnectionActivityInput.connectionId = connectionId
      verify {
        mAutoDisableConnectionActivity.autoDisableFailingConnection(autoDisableConnectionActivityInput)
      }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that auto disable activity is not touched during job success")
    fun testNoAutoDisableOnSuccess() {
      returnTrueForLastJobOrAttemptFailure()
      val syncWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
      syncWorker.registerWorkflowImplementationTypes(EmptySyncWorkflow::class.java)
      val checkWorker = testEnv.newWorker(TemporalJobType.CHECK_CONNECTION.name)
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionSuccessWorkflow::class.java)
      testEnv.start()

      val testId = UUID.randomUUID()
      val connectionId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)
      val input =
        ConnectionUpdaterInput(
          connectionId = connectionId,
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 0,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      verify { mAutoDisableConnectionActivity wasNot Called }
    }
  }

  @Nested
  @DisplayName("Test that sync workflow failures are recorded")
  internal inner class SyncWorkflowReplicationFailuresRecorded {
    @BeforeEach
    fun setup() {
      setupSimpleConnectionManagerWorkflow()
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that Source CHECK failures are recorded")
    fun testSourceCheckFailuresRecorded() {
      returnTrueForLastJobOrAttemptFailure()
      every { mJobCreationAndStatusUpdateActivity.createNewJob(any()) } returns JobCreationOutput(JOB_ID)
      every { mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()) } returns AttemptNumberCreationOutput(ATTEMPT_ID)

      val checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionFailedWorkflow::class.java)

      testEnv.start()

      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)
      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      verify(timeout = TEN_SECONDS.toLong(), atLeast = 1) {
        mJobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(any())
      }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that Source CHECK failures are recorded when running in child workflow")
    fun testSourceCheckInChildWorkflowFailuresRecorded() {
      returnTrueForLastJobOrAttemptFailure()
      every { mJobCreationAndStatusUpdateActivity.createNewJob(any()) } returns JobCreationOutput(JOB_ID)
      every { mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()) } returns AttemptNumberCreationOutput(ATTEMPT_ID)
      val checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionFailedWorkflow::class.java)
      testEnv.start()

      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)
      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      verify(timeout = TEN_SECONDS.toLong(), atLeast = 1) {
        mJobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(any())
      }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that Source CHECK failure reasons are recorded")
    fun testSourceCheckFailureReasonsRecorded() {
      returnTrueForLastJobOrAttemptFailure()
      every { mJobCreationAndStatusUpdateActivity.createNewJob(any()) } returns JobCreationOutput(JOB_ID)
      every { mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()) } returns AttemptNumberCreationOutput(ATTEMPT_ID)
      val checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionSystemErrorWorkflow::class.java)
      testEnv.start()

      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)
      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      verify(timeout = TEN_SECONDS.toLong(), atLeast = 1) {
        mJobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(any())
      }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that Destination CHECK failures are recorded")
    fun testDestinationCheckFailuresRecorded() {
      returnTrueForLastJobOrAttemptFailure()
      every { mJobCreationAndStatusUpdateActivity.createNewJob(any()) } returns JobCreationOutput(JOB_ID)
      every { mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()) } returns AttemptNumberCreationOutput(ATTEMPT_ID)
      val checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionSourceSuccessOnlyWorkflow::class.java)

      testEnv.start()

      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)
      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      verify(timeout = TEN_SECONDS.toLong(), atLeast = 1) {
        mJobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(any())
      }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that Destination CHECK failure reasons are recorded")
    fun testDestinationCheckFailureReasonsRecorded() {
      returnTrueForLastJobOrAttemptFailure()
      every { mJobCreationAndStatusUpdateActivity.createNewJob(any()) } returns JobCreationOutput(JOB_ID)
      every { mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()) } returns AttemptNumberCreationOutput(ATTEMPT_ID)
      val checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionDestinationSystemErrorWorkflow::class.java)

      testEnv.start()

      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)
      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      verify(timeout = TEN_SECONDS.toLong(), atLeast = 1) {
        mJobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(any())
      }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that reset workflows do not CHECK the source")
    fun testSourceCheckSkippedWhenReset() {
      every { mJobCreationAndStatusUpdateActivity.shouldRunSourceCheck(any()) } returns false // Source check should be skipped for reset

      every { mJobCreationAndStatusUpdateActivity.shouldRunDestinationCheck(any()) } returns true

      val jobRunConfig = JobRunConfig()
      jobRunConfig.jobId = JOB_ID.toString()
      jobRunConfig.attemptId = ATTEMPT_ID.toLong()

      every { mJobCreationAndStatusUpdateActivity.createNewJob(any()) } returns JobCreationOutput(JOB_ID)
      every { mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()) } returns AttemptNumberCreationOutput(ATTEMPT_ID)
      val checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionFailedWorkflow::class.java)

      testEnv.start()

      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)
      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      verify(timeout = TEN_SECONDS.toLong(), atLeast = 1) {
        mJobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(any())
      }
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that source and destination failures are recorded")
    fun testSourceAndDestinationFailuresRecorded() {
      setupSourceAndDestinationFailure(UUID.randomUUID())

      verify {
        mJobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(any())
      }
      verify {
        mJobCreationAndStatusUpdateActivity.attemptFailureWithAttemptNumber(any())
      }
    }
  }

  @Nested
  @DisplayName("Test that the workflow is properly restarted after activity failures.")
  internal inner class FailedActivityWorkflow {
    @ParameterizedTest
    @MethodSource("io.airbyte.workers.temporal.scheduling.ConnectionManagerWorkflowTest#getSetupFailingActivity")
    fun testWorkflowRestartedAfterFailedActivity(
      mockSetup: Thread,
      expectedEventsCount: Int,
    ) {
      returnTrueForLastJobOrAttemptFailure()
      mockSetup.start()
      every { mConfigFetchActivity.getTimeToWait(any()) } returns
        ScheduleRetrieverOutput(
          Duration.ZERO,
        )

      val testId = UUID.randomUUID()
      reset()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)

      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = null,
          attemptId = null,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      startWorkflowAndWaitUntilReady(workflow, input)

      // Sleep test env for restart delay, plus a small buffer to ensure that the workflow executed the
      // logic after the delay
      testEnv.sleep(WORKFLOW_FAILURE_RESTART_DELAY.plus(Duration.ofSeconds(10)))

      val events: Queue<ChangedStateEvent> = testStateListener.events(testId)

      val filteredAssertionList: AbstractCollectionAssert<*, *, *, *> =
        Assertions
          .assertThat(events)
          .filteredOn(
            Predicate { changedStateEvent: ChangedStateEvent? ->
              changedStateEvent!!.field == StateField.RUNNING &&
                changedStateEvent.isValue
            },
          )

      if (expectedEventsCount == 0) {
        filteredAssertionList.isEmpty()
      } else {
        filteredAssertionList.hasSizeGreaterThanOrEqualTo(expectedEventsCount)
      }

      assertWorkflowWasContinuedAsNew()
    }

    @BeforeEach
    fun setup() {
      setupSpecificChildWorkflow(
        SleepingSyncWorkflow::class.java,
        CheckConnectionSuccessWorkflow::class.java,
      )
    }
  }

  @Nested
  @DisplayName("New 'resilient' retries and progress checking")
  internal inner class Retries {
    @BeforeEach
    fun setup() {
      setupSimpleConnectionManagerWorkflow()
    }

    @ParameterizedTest
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("We check the progress of the last attempt on failure")
    @MethodSource("io.airbyte.workers.temporal.scheduling.ConnectionManagerWorkflowTest#coreFailureTypesMatrix")
    fun checksProgressOnFailure(failureCase: Class<out SyncWorkflowV2?>?) {
      // We check attempt progress using the 0-based attempt number counting system used everywhere except
      // the ConnectionUpdaterInput where it is 1-based. This will be fixed to be more consistent later.
      // The concrete value passed here is inconsequential—the important part is that it is _not_ the
      // attempt number set on the ConnectionUpdaterInput.
      val attemptNumber = 42
      every { mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()) } returns AttemptNumberCreationOutput(attemptNumber)

      setupFailureCase(failureCase)

      val captor = mutableListOf<CheckRunProgressActivity.Input>()
      verify(exactly = 1) { mCheckRunProgressActivity.checkProgress(capture(captor)) }
      Assertions.assertThat(captor[0].jobId).isEqualTo(JOB_ID)
      Assertions.assertThat(captor[0].attemptNo).isEqualTo(attemptNumber)
    }

    @ParameterizedTest
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("We hydrate, persist and use retry manager.")
    @MethodSource("io.airbyte.workers.temporal.scheduling.ConnectionManagerWorkflowTest#coreFailureTypesMatrix")
    @Disabled("Flaky in CI.")
    fun hydratePersistRetryManagerFlow(failureCase: Class<out SyncWorkflowV2?>?) {
      val connectionId = UUID.randomUUID()
      val jobId = 32198714L
      val input = testInputBuilder()
      input.connectionId = connectionId
      input.jobId = null

      val retryLimit = 2

      val manager1 = RetryManager(null, null, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE, retryLimit)
      val manager2 = RetryManager(null, null, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE, retryLimit, 0, 1, 0, 1)
      val manager3 = RetryManager(null, null, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE, retryLimit)

      every { mJobCreationAndStatusUpdateActivity.createNewJob(any()) } returns JobCreationOutput(jobId)
      every { mRetryStatePersistenceActivity.hydrateRetryState(any()) } returnsMany
        listOf(
          HydrateOutput(manager1), // run 1: pre scheduling
          HydrateOutput(manager1), // run 1: pre run
          HydrateOutput(manager2), // run 2: pre scheduling
          HydrateOutput(manager2), // run 2: pre run
          HydrateOutput(manager3), // run 3: pre run
        )
      every { mCheckRunProgressActivity.checkProgress(any()) } returns CheckRunProgressActivity.Output(true) // true to hit partial failure limit

      setupFailureCase(failureCase, input)

      // Wait a little extra for resiliency
      val hydrateCaptor = mutableListOf<HydrateInput>()
      val persistCaptor = mutableListOf<PersistInput>()
      // If the test timeouts expire before we wrap around to the backoff/scheduling step it will run
      // exactly twice per attempt.
      // Otherwise, there's 1 extra hydration to resolve backoff.
      verify(timeout = TEN_SECONDS.toLong(), atLeast = 2 * retryLimit) {
        mRetryStatePersistenceActivity.hydrateRetryState(capture(hydrateCaptor))
      }
      verify(timeout = TEN_SECONDS.toLong(), exactly = retryLimit) {
        mCheckRunProgressActivity.checkProgress(any())
      }
      verify(timeout = TEN_SECONDS.toLong(), exactly = retryLimit) {
        mRetryStatePersistenceActivity.persistRetryState(capture(persistCaptor))
      }
      verify(timeout = TEN_SECONDS.toLong(), exactly = retryLimit) {
        mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any())
      }

      // run 1: hydrate pre scheduling
      Assertions.assertThat(hydrateCaptor[0].connectionId).isEqualTo(connectionId)
      Assertions.assertThat(hydrateCaptor[0].jobId).isNull()
      // run 1: hydrate pre run
      Assertions.assertThat(hydrateCaptor[1].connectionId).isEqualTo(connectionId)
      Assertions.assertThat(hydrateCaptor[1].jobId).isNull()
      // run 1: persist
      Assertions.assertThat(persistCaptor[0].connectionId).isEqualTo(connectionId)
      Assertions.assertThat(persistCaptor[0].jobId).isEqualTo(jobId)
      Assertions
        .assertThat(
          persistCaptor[0]
            .manager!!
            .successivePartialFailures,
        ).isEqualTo(1)
      Assertions
        .assertThat(
          persistCaptor[0]
            .manager!!
            .totalPartialFailures,
        ).isEqualTo(1)

      // run 2: hydrate pre scheduling
      Assertions.assertThat(hydrateCaptor[2].connectionId).isEqualTo(connectionId)
      Assertions.assertThat(hydrateCaptor[2].jobId).isEqualTo(jobId)
      // run 2: hydrate pre run
      Assertions.assertThat(hydrateCaptor[3].connectionId).isEqualTo(connectionId)
      Assertions.assertThat(hydrateCaptor[3].jobId).isEqualTo(jobId)
      // run 2: persist
      Assertions.assertThat(persistCaptor[1].connectionId).isEqualTo(connectionId)
      Assertions.assertThat(persistCaptor[1].jobId).isEqualTo(jobId)
      Assertions
        .assertThat(
          persistCaptor[1]
            .manager!!
            .successivePartialFailures,
        ).isEqualTo(2)
      Assertions
        .assertThat(
          persistCaptor[1]
            .manager!!
            .totalPartialFailures,
        ).isEqualTo(2)
      // run 3: hydrate pre scheduling
      Assertions.assertThat(hydrateCaptor[4].connectionId).isEqualTo(connectionId)
      Assertions.assertThat(hydrateCaptor[4].jobId).isNull()
    }

    @ParameterizedTest
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("We use attempt-based retries when retry manager not present.")
    @MethodSource("io.airbyte.workers.temporal.scheduling.ConnectionManagerWorkflowTest#coreFailureTypesMatrix")
    fun usesAttemptBasedRetriesIfRetryManagerUnset(failureCase: Class<out SyncWorkflowV2?>?) {
      val connectionId = UUID.randomUUID()
      val jobId = 32198714L
      val input = testInputBuilder()
      input.connectionId = connectionId
      input.jobId = null

      val retryLimit = 1

      // attempt-based retry configuration
      every { mConfigFetchActivity.getMaxAttempt() } returns GetMaxAttemptOutput(retryLimit)

      every { mJobCreationAndStatusUpdateActivity.createNewJob(any()) } returns JobCreationOutput(jobId)
      every { mRetryStatePersistenceActivity.hydrateRetryState(any()) } returns HydrateOutput(null)
      every { mCheckRunProgressActivity.checkProgress(any()) } returns CheckRunProgressActivity.Output(true)

      setupFailureCase(failureCase, input)

      verify(exactly = 0) {
        mRetryStatePersistenceActivity.persistRetryState(any())
      }
      verify(exactly = retryLimit) {
        mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any())
      }
    }

    @ParameterizedTest
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Uses scheduling resolution if no retry manager.")
    @MethodSource("io.airbyte.workers.temporal.scheduling.ConnectionManagerWorkflowTest#noBackoffSchedulingMatrix")
    fun useSchedulingIfNoRetryManager(
      fromFailure: Boolean,
      timeToWait: Duration,
    ) {
      val timeTilNextScheduledRun = Duration.ofHours(1)
      every { mConfigFetchActivity.getTimeToWait(any()) } returns ScheduleRetrieverOutput(timeTilNextScheduledRun)

      every { mRetryStatePersistenceActivity.hydrateRetryState(any()) } returns HydrateOutput(null)

      val testStateListener = TestStateListener()
      val testId = UUID.randomUUID()
      val workflowState = WorkflowState(testId, testStateListener)

      val input = testInputBuilder()
      input.fromFailure = fromFailure
      input.workflowState = workflowState

      setupSuccessfulWorkflow(input)

      testEnv.sleep(timeToWait.plus(Duration.ofSeconds(5)))

      val events: Queue<ChangedStateEvent> = testStateListener.events(testId)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.DONE_WAITING &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThan(0)
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Uses scheduling if not from failure and retry manager present.")
    fun useSchedulingIfNotFromFailure() {
      val backoff = Duration.ofMinutes(1)
      val policy = BackoffPolicy(backoff, backoff)
      val manager =
        RetryManager(policy, null, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE)
      manager.successiveCompleteFailures = 1

      every { mRetryStatePersistenceActivity.hydrateRetryState(any()) } returns HydrateOutput(manager)

      val timeTilNextScheduledRun = Duration.ofHours(1)
      every { mConfigFetchActivity.getTimeToWait(any()) } returns ScheduleRetrieverOutput(timeTilNextScheduledRun)

      val testStateListener = TestStateListener()
      val testId = UUID.randomUUID()
      val workflowState = WorkflowState(testId, testStateListener)

      val input = testInputBuilder()
      input.fromFailure = false
      input.workflowState = workflowState

      setupSuccessfulWorkflow(input)

      val events: Queue<ChangedStateEvent> = testStateListener.events(testId)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.DONE_WAITING &&
              changedStateEvent.isValue
          },
        ).hasSize(0)

      testEnv.sleep(timeTilNextScheduledRun.plus(Duration.ofSeconds(5)))

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.DONE_WAITING &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThan(0)
    }

    @ParameterizedTest
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Uses backoff policy if present and from failure.")
    @ValueSource(longs = [1, 5, 20, 30, 1439, 21])
    fun usesBackoffPolicyIfPresent(minutes: Long) {
      val backoff = Duration.ofMinutes(minutes)
      val policy = BackoffPolicy(backoff, backoff)
      val manager =
        RetryManager(policy, null, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE)
      manager.successiveCompleteFailures = 1

      every { mRetryStatePersistenceActivity.hydrateRetryState(any()) } returns HydrateOutput(manager)

      every { mConfigFetchActivity.getTimeToWait(any()) } returns ScheduleRetrieverOutput(Duration.ofDays(1))

      val testStateListener = TestStateListener()
      val testId = UUID.randomUUID()
      val workflowState = WorkflowState(testId, testStateListener)

      val input = testInputBuilder()
      input.fromFailure = true
      input.workflowState = workflowState

      setupSuccessfulWorkflow(input)

      val events: Queue<ChangedStateEvent> = testStateListener.events(testId)

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.DONE_WAITING &&
              changedStateEvent.isValue
          },
        ).hasSize(0)

      testEnv.sleep(backoff.plus(Duration.ofSeconds(5)))

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.DONE_WAITING &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThan(0)
    }

    @ParameterizedTest
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Jobs can be cancelled during the backoff.")
    @ValueSource(longs = [1, 5, 20, 30, 1439, 21])
    @Disabled("Flaky in CI")
    fun cancelWorksDuringBackoff(minutes: Long) {
      val backoff = Duration.ofMinutes(minutes)
      val policy = BackoffPolicy(backoff, backoff)
      val manager =
        RetryManager(policy, null, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE)
      manager.successiveCompleteFailures = 1

      every { mRetryStatePersistenceActivity.hydrateRetryState(any()) } returns HydrateOutput(manager)

      every { mConfigFetchActivity.getTimeToWait(any()) } returns ScheduleRetrieverOutput(Duration.ofDays(1))

      val testStateListener = TestStateListener()
      val testId = UUID.randomUUID()
      val workflowState = WorkflowState(testId, testStateListener)

      val jobId = 124198715L
      val attemptNo = 72

      val input = testInputBuilder()
      input.jobId = jobId
      input.attemptNumber = attemptNo
      input.fromFailure = true
      input.workflowState = workflowState

      setupSuccessfulWorkflow(input)

      val events: Queue<ChangedStateEvent> = testStateListener.events(testId)

      workflow.cancelJob()

      testEnv.sleep(Duration.ofMinutes(1))

      verify {
        mJobCreationAndStatusUpdateActivity.jobCancelledWithAttemptNumber(any())
      } // input attempt number is 1 based

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.CANCELLED &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThan(0)
    }

    @ParameterizedTest
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Does not fail job if backoff longer than time til next scheduled run.")
    @MethodSource("io.airbyte.workers.temporal.scheduling.ConnectionManagerWorkflowTest#backoffJobFailureMatrix")
    fun doesNotFailJobIfBackoffTooLong(backoffMinutes: Long) {
      val backoff = Duration.ofMinutes(backoffMinutes)
      val policy = BackoffPolicy(backoff, backoff)
      val manager =
        RetryManager(policy, null, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE, Int.MAX_VALUE)
      manager.successiveCompleteFailures = 1

      every { mRetryStatePersistenceActivity.hydrateRetryState(any()) } returns HydrateOutput(manager)

      val timeTilNextScheduledRun = Duration.ofMinutes(60)
      every { mConfigFetchActivity.getTimeToWait(any()) } returns ScheduleRetrieverOutput(timeTilNextScheduledRun)

      val input = testInputBuilder()
      input.fromFailure = true

      setupSuccessfulWorkflow(input)
      testEnv.sleep(Duration.ofMinutes(1))

      verify(exactly = 0) {
        mJobCreationAndStatusUpdateActivity.jobFailure(any())
      }
    }
  }

  @Nested
  @DisplayName("General functionality")
  internal inner class General {
    @BeforeEach
    fun setup() {
      setupSimpleConnectionManagerWorkflow()
    }

    @Test
    @DisplayName("When a sync returns a status of cancelled we report the run as cancelled")
    fun reportsCancelledWhenConnectionDisabled() {
      val input = testInputBuilder()
      setupSuccessfulWorkflow(CancelledSyncWorkflow::class.java, input)
      workflow.submitManualSync()
      testEnv.sleep(Duration.ofSeconds(60))

      verify {
        mJobCreationAndStatusUpdateActivity.jobCancelledWithAttemptNumber(any())
      }
    }

    @Test
    fun `When a sync is cancelled, we still execute the post processing`() {
      val input = testInputBuilder()
      setupSuccessfulWorkflow(CancelledSyncWorkflow::class.java, input)
      workflow.submitManualSync()
      testEnv.sleep(Duration.ofSeconds(60))

      verify {
        mJobPostProcessingActivity.wasStarted()
      }
    }

    @Test
    fun `When a sync fails, we execute the post processing`() {
      val input = testInputBuilder()
      setupSuccessfulWorkflow(SyncWorkflowFailingOutputWorkflow::class.java, input)
      workflow.submitManualSync()
      testEnv.sleep(Duration.ofSeconds(60))

      verify {
        mJobCreationAndStatusUpdateActivity.jobFailure(any())
      }

      verify {
        mJobPostProcessingActivity.wasStarted()
      }
    }

    @Test
    fun `When a sync succeeds, we execute the post processing`() {
      val input = testInputBuilder()
      setupSuccessfulWorkflow(EmptySyncWorkflow::class.java, input)
      workflow.submitManualSync()
      testEnv.sleep(Duration.ofSeconds(60))

      verify {
        mJobCreationAndStatusUpdateActivity.jobSuccessWithAttemptNumber(any())
      }

      verify {
        mJobPostProcessingActivity.wasStarted()
      }
    }
  }

  @DisplayName("Load shedding backoff loop")
  @Nested
  internal inner class LoadShedBackoff {
    @BeforeEach
    fun setup() {
      setupSimpleConnectionManagerWorkflow()
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Load shed backoff loop waits and exits")
    fun happyPath() {
      val backoff = Duration.ofMinutes(10)
      val backoffOutput = GetLoadShedBackoffOutput(backoff)
      val zeroBackoffOutput = GetLoadShedBackoffOutput(Duration.ZERO)
      val zeroSchedulerOutput = ScheduleRetrieverOutput(Duration.ZERO)

      // back off 3 times, then unblock
      every { mConfigFetchActivity.getLoadShedBackoff(any()) } returnsMany
        listOf(backoffOutput, backoffOutput, backoffOutput, zeroBackoffOutput)

      every { mConfigFetchActivity.getTimeToWait(any()) } returns zeroSchedulerOutput
      every { mConfigFetchActivity.isWorkspaceTombstone(any()) } returns false

      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)

      val input =
        ConnectionUpdaterInput(
          connectionId = UUID.randomUUID(),
          jobId = JOB_ID,
          attemptId = ATTEMPT_ID,
          fromFailure = false,
          attemptNumber = 1,
          workflowState = workflowState,
          resetConnection = false,
          fromJobResetFailure = false,
          skipScheduling = false,
        )

      setupSuccessfulWorkflow(input)
      // wait a sec til we get to the backoff step before calling verify
      testEnv.sleep(Duration.ofMinutes(1))
      verify(exactly = 1) { mConfigFetchActivity.getLoadShedBackoff(any()) }
      // we will check the load shed flag after each backoff
      testEnv.sleep(backoff)
      verify(exactly = 2) { mConfigFetchActivity.getLoadShedBackoff(any()) }
      testEnv.sleep(backoff)
      verify(exactly = 3) { mConfigFetchActivity.getLoadShedBackoff(any()) }
      testEnv.sleep(backoff)
      // verify we have exited the loop and will continue
      verify(exactly = 1) { mConfigFetchActivity.getTimeToWait(any<ScheduleRetrieverInput>()) }
    }
  }

  private fun <T1 : SyncWorkflowV2, T2 : ConnectorCommandWorkflow> setupSpecificChildWorkflow(
    mockedSyncWorkflow: Class<T1>,
    mockedCheckWorkflow: Class<T2>,
  ) {
    testEnv = TestWorkflowEnvironment.newInstance()

    val syncWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
    syncWorker.registerWorkflowImplementationTypes(mockedSyncWorkflow)

    val checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
    checkWorker.registerWorkflowImplementationTypes(mockedCheckWorkflow)

    val managerWorker = testEnv.newWorker(TemporalJobType.CONNECTION_UPDATER.name)
    managerWorker.registerWorkflowImplementationTypes(
      temporalProxyHelper.proxyWorkflowClass(
        ConnectionManagerWorkflowImpl::class.java,
      ),
      temporalProxyHelper.proxyWorkflowClass(
        JobPostProcessingWorkflowStub::class.java,
      ),
    )
    managerWorker.registerActivitiesImplementations(
      mConfigFetchActivity,
      mJobCreationAndStatusUpdateActivity,
      mAutoDisableConnectionActivity,
      mRecordMetricActivity,
      mWorkflowConfigActivity,
      mFeatureFlagFetchActivity,
      mCheckRunProgressActivity,
      mRetryStatePersistenceActivity,
      mAppendToAttemptLogActivity,
      mJobPostProcessingActivity,
    )

    client = testEnv.workflowClient
    testEnv.start()

    workflow =
      client
        .newWorkflowStub(
          ConnectionManagerWorkflow::class.java,
          WorkflowOptions
            .newBuilder()
            .setTaskQueue(TemporalJobType.CONNECTION_UPDATER.name)
            .setWorkflowId(WORKFLOW_ID)
            .build(),
        )
  }

  private fun assertWorkflowWasContinuedAsNew() {
    val request =
      ListClosedWorkflowExecutionsRequest
        .newBuilder()
        .setNamespace(testEnv.namespace)
        .setExecutionFilter(WorkflowExecutionFilter.newBuilder().setWorkflowId(WORKFLOW_ID))
        .build()
    val listResponse =
      testEnv
        .workflowService
        .blockingStub()
        .listClosedWorkflowExecutions(request)
    Assertions.assertThat(listResponse.executionsCount).isGreaterThanOrEqualTo(1)
    Assertions
      .assertThat(listResponse.executionsList[0].status)
      .isEqualTo(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW)
  }

  private fun testInputBuilder(): ConnectionUpdaterInput {
    val testId = UUID.randomUUID()
    val testStateListener = TestStateListener()
    val workflowState = WorkflowState(testId, testStateListener)

    return ConnectionUpdaterInput(
      connectionId = UUID.randomUUID(),
      jobId = JOB_ID,
      attemptId = ATTEMPT_ID,
      fromFailure = false,
      attemptNumber = 1,
      workflowState = workflowState,
      resetConnection = false,
      fromJobResetFailure = false,
      skipScheduling = false,
    )
  }

  /**
   * Given a failure case class, this will set up a manual sync to fail in that fashion.
   * ConnectionUpdaterInput is pluggable for various test needs. Feel free to update input/return
   * values as is necessary.
   */
  private fun setupFailureCase(
    failureClass: Class<out SyncWorkflowV2?>?,
    input: ConnectionUpdaterInput?,
  ) {
    returnTrueForLastJobOrAttemptFailure()
    val syncWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
    syncWorker.registerWorkflowImplementationTypes(failureClass)

    val checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
    checkWorker.registerWorkflowImplementationTypes(CheckConnectionSuccessWorkflow::class.java)

    testEnv.start()

    startWorkflowAndWaitUntilReady(workflow, input)

    // wait for workflow to initialize
    testEnv.sleep(Duration.ofMinutes(1))

    workflow.submitManualSync()
    testEnv.sleep(Duration.ofMinutes(1))
  }

  private fun setupFailureCase(failureClass: Class<out SyncWorkflowV2?>?) {
    val input = testInputBuilder()

    setupFailureCase(failureClass, input)
  }

  private fun setupSourceAndDestinationFailure(connectionId: UUID?) {
    val input = testInputBuilder()
    input.connectionId = connectionId

    setupFailureCase(SourceAndDestinationFailureSyncWorkflow::class.java, input)
  }

  /**
   * Does all the legwork for setting up a workflow for simple runs. NOTE: Don't forget to add your
   * mock activity below.
   */
  private fun setupSimpleConnectionManagerWorkflow() {
    testEnv = TestWorkflowEnvironment.newInstance()

    val managerWorker = testEnv.newWorker(TemporalJobType.CONNECTION_UPDATER.name)
    managerWorker.registerWorkflowImplementationTypes(
      temporalProxyHelper.proxyWorkflowClass(
        ConnectionManagerWorkflowImpl::class.java,
      ),
      temporalProxyHelper.proxyWorkflowClass(
        JobPostProcessingWorkflowStub::class.java,
      ),
    )
    managerWorker.registerActivitiesImplementations(
      mConfigFetchActivity,
      mJobCreationAndStatusUpdateActivity,
      mAutoDisableConnectionActivity,
      mRecordMetricActivity,
      mWorkflowConfigActivity,
      mFeatureFlagFetchActivity,
      mCheckRunProgressActivity,
      mRetryStatePersistenceActivity,
      mAppendToAttemptLogActivity,
      mJobPostProcessingActivity,
    )

    client = testEnv.workflowClient
    workflow =
      client.newWorkflowStub(
        ConnectionManagerWorkflow::class.java,
        WorkflowOptions.newBuilder().setTaskQueue(TemporalJobType.CONNECTION_UPDATER.name).build(),
      )
  }

  private fun setupSuccessfulWorkflow(input: ConnectionUpdaterInput?) {
    setupSuccessfulWorkflow(EmptySyncWorkflow::class.java, input)
  }

  private fun <T : SyncWorkflowV2> setupSuccessfulWorkflow(
    syncWorkflowMockClass: Class<T>,
    input: ConnectionUpdaterInput?,
  ) {
    returnTrueForLastJobOrAttemptFailure()
    val syncWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
    syncWorker.registerWorkflowImplementationTypes(syncWorkflowMockClass)
    val checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
    checkWorker.registerWorkflowImplementationTypes(CheckConnectionSuccessWorkflow::class.java)
    testEnv.start()

    startWorkflowAndWaitUntilReady(workflow, input)
  }

  companion object {
    private val log: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    private const val JOB_ID = 1L
    private const val ATTEMPT_ID = 1
    val SOURCE_ID: UUID = UUID.randomUUID()
    val DESTINATION_ID: UUID = UUID.randomUUID()

    private val SCHEDULE_WAIT: Duration = Duration.ofMinutes(20L)
    private const val WORKFLOW_ID = "workflow-id"

    private val WORKFLOW_FAILURE_RESTART_DELAY: Duration = Duration.ofSeconds(600)

    private const val TEN_SECONDS = 10 * 1000

    private val mJobCreationAndStatusUpdateActivity: JobCreationAndStatusUpdateActivity =
      mockk<JobCreationAndStatusUpdateActivityImpl>(relaxed = true)
    private val mAutoDisableConnectionActivity: AutoDisableConnectionActivity =
      mockk<AutoDisableConnectionActivityImpl>(relaxed = true)
    private val mStreamResetActivity: StreamResetActivity =
      mockk<StreamResetActivityImpl>(relaxed = true)
    private val mRecordMetricActivity: RecordMetricActivity =
      mockk<RecordMetricActivityImpl>(relaxed = true)
    private val mWorkflowConfigActivity: WorkflowConfigActivity =
      mockk<WorkflowConfigActivityImpl>(relaxed = true)
    private val mFeatureFlagFetchActivity: FeatureFlagFetchActivity =
      mockk<FeatureFlagFetchActivityImpl>(relaxed = true)
    private val mCheckRunProgressActivity: CheckRunProgressActivity =
      mockk<CheckRunProgressActivityImpl>(relaxed = true)
    private val mRetryStatePersistenceActivity: RetryStatePersistenceActivity =
      mockk<RetryStatePersistenceActivityImpl>(relaxed = true)
    private val mAppendToAttemptLogActivity: AppendToAttemptLogActivity =
      mockk<AppendToAttemptLogActivityImpl>(relaxed = true)
    private const val EVENT = "event = "

    private fun startWorkflowAndWaitUntilReady(
      workflow: ConnectionManagerWorkflow,
      input: ConnectionUpdaterInput?,
    ) {
      WorkflowClient.start<ConnectionUpdaterInput?>(
        { connectionUpdaterInput: ConnectionUpdaterInput? ->
          workflow.run(
            connectionUpdaterInput!!,
          )
        },
        input,
      )

      var isReady = false

      while (!isReady) {
        try {
          isReady = workflow.getState() != null
        } catch (_: Exception) {
          log.info("retrying...")
          Thread.sleep(100)
        }
      }
    }

    private fun waitUntilDeleted(workflow: ConnectionManagerWorkflow) {
      var isDeleted = false

      while (!isDeleted) {
        try {
          isDeleted = workflow.getState() != null && workflow.getState().isDeleted
        } catch (_: Exception) {
          log.info("retrying...")
          Thread.sleep(100)
        }
      }
    }

    @JvmStatic
    fun getSetupFailingActivity() =
      listOf<Arguments?>(
        Arguments.of(
          Thread {
            every {
              mJobCreationAndStatusUpdateActivity.createNewJob(
                any(),
              )
            } answers { throw ApplicationFailure.newNonRetryableFailure("", "") }
          },
          0,
        ),
        Arguments.of(
          Thread {
            every {
              mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(
                any(),
              )
            } answers { throw ApplicationFailure.newNonRetryableFailure("", "") }
          },
          0,
        ),
        Arguments.of(
          Thread {
            every {
              mJobCreationAndStatusUpdateActivity.reportJobStart(any())
            } answers { throw ApplicationFailure.newNonRetryableFailure("", "") }
          },
          0,
        ),
      )

    // Since we can't directly unit test the failure path, we enumerate the core failure cases as a
    // proxy. This is deliberately incomplete as the permutations of failure cases is large.
    @JvmStatic
    fun coreFailureTypesMatrix() =
      listOf<Arguments?>(
        Arguments.of(SourceAndDestinationFailureSyncWorkflow::class.java),
        Arguments.of(ReplicateFailureSyncWorkflow::class.java),
        Arguments.of(SyncWorkflowFailingOutputWorkflow::class.java),
      )

    @JvmStatic
    fun noBackoffSchedulingMatrix() =
      listOf<Arguments?>(
        Arguments.of(true, Duration.ZERO),
        Arguments.of(false, Duration.ofHours(1)),
      )

    @JvmStatic
    private fun backoffJobFailureMatrix() =
      listOf<Arguments?>(
        Arguments.of(1),
        Arguments.of(10),
        Arguments.of(55),
        Arguments.of(60),
        Arguments.of(123),
        Arguments.of(214),
        Arguments.of(7),
      )
  }
}
