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
import io.airbyte.config.FailureReason
import io.airbyte.micronaut.temporal.TemporalProxyHelper
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.temporal.activities.GetConnectionContextOutput
import io.airbyte.workers.temporal.activities.GetLoadShedBackoffOutput
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity.LogOutput
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionActivityInput
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionOutput
import io.airbyte.workers.temporal.scheduling.activities.CheckRunProgressActivity
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.GetMaxAttemptOutput
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.ScheduleRetrieverInput
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.ScheduleRetrieverOutput
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity.FeatureFlagFetchOutput
import io.airbyte.workers.temporal.scheduling.activities.FinalizeJobStatsInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptCreationInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptNumberCreationOutput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptNumberFailureInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCancelledInputWithAttemptNumber
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCreationOutput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobSuccessInputWithAttemptNumber
import io.airbyte.workers.temporal.scheduling.activities.JobPostProcessingActivity
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.HydrateInput
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.HydrateOutput
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.PersistInput
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.PersistOutput
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivity
import io.airbyte.workers.temporal.scheduling.activities.WorkflowConfigActivity
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
import org.mockito.ArgumentMatcher
import org.mockito.Mockito
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.verification.VerificationMode
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.time.Duration
import java.util.Map
import java.util.Queue
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.function.Predicate
import java.util.stream.Stream
import kotlin.collections.ArrayList
import kotlin.collections.MutableList

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
  private val mConfigFetchActivity: ConfigFetchActivity =
    Mockito.mock<ConfigFetchActivity>(ConfigFetchActivity::class.java, Mockito.withSettings().withoutAnnotations())
  private val mJobPostProcessingActivity: ValidateJobPostProcessingWorkflowStartedActivity =
    Mockito.mock(ValidateJobPostProcessingWorkflowStartedActivity::class.java, Mockito.withSettings().withoutAnnotations())
  private lateinit var testEnv: TestWorkflowEnvironment
  private lateinit var client: WorkflowClient
  private lateinit var workflow: ConnectionManagerWorkflow
  private lateinit var activityOptions: ActivityOptions
  private lateinit var temporalProxyHelper: TemporalProxyHelper

  @BeforeEach
  @Throws(Exception::class)
  fun setUp() {
    Mockito.reset<ConfigFetchActivity?>(mConfigFetchActivity)
    Mockito.reset<JobCreationAndStatusUpdateActivity?>(mJobCreationAndStatusUpdateActivity)
    Mockito.reset<AutoDisableConnectionActivity?>(mAutoDisableConnectionActivity)
    Mockito.reset<StreamResetActivity?>(mStreamResetActivity)
    Mockito.reset<RecordMetricActivity?>(mRecordMetricActivity)
    Mockito.reset<WorkflowConfigActivity?>(mWorkflowConfigActivity)
    Mockito.reset<CheckRunProgressActivity?>(mCheckRunProgressActivity)
    Mockito.reset<RetryStatePersistenceActivity?>(mRetryStatePersistenceActivity)
    Mockito.reset<AppendToAttemptLogActivity?>(mAppendToAttemptLogActivity)

    // default is to wait "forever"
    Mockito.`when`<ScheduleRetrieverOutput?>(mConfigFetchActivity.getTimeToWait(any())).thenReturn(
      ScheduleRetrieverOutput(
        Duration.ofDays((100 * 365).toLong()),
      ),
    )

    // default is to not back off
    Mockito.`when`<GetLoadShedBackoffOutput?>(mConfigFetchActivity.getLoadShedBackoff(any())).thenReturn(
      GetLoadShedBackoffOutput(
        Duration.ZERO,
      ),
    )

    Mockito.`when`<GetConnectionContextOutput?>(mConfigFetchActivity.getConnectionContext(any())).thenReturn(
      GetConnectionContextOutput(
        ConnectionContext().withSourceId(SOURCE_ID).withDestinationId(DESTINATION_ID),
      ),
    )

    Mockito
      .`when`<JobCreationOutput?>(mJobCreationAndStatusUpdateActivity.createNewJob(any()))
      .thenReturn(
        JobCreationOutput(
          1L,
        ),
      )

    Mockito
      .`when`<AttemptNumberCreationOutput?>(mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()))
      .thenReturn(
        AttemptNumberCreationOutput(
          1,
        ),
      )

    Mockito
      .`when`<AutoDisableConnectionOutput?>(
        mAutoDisableConnectionActivity.autoDisableFailingConnection(any()),
      ).thenReturn(AutoDisableConnectionOutput(false))

    Mockito
      .`when`<Duration?>(mWorkflowConfigActivity.getWorkflowRestartDelaySeconds())
      .thenReturn(WORKFLOW_FAILURE_RESTART_DELAY)
    Mockito
      .`when`<FeatureFlagFetchOutput?>(mFeatureFlagFetchActivity.getFeatureFlags(any()))
      .thenReturn(FeatureFlagFetchOutput(Map.of<String?, Boolean?>()))
    Mockito
      .`when`<CheckRunProgressActivity.Output?>(mCheckRunProgressActivity.checkProgress(any()))
      .thenReturn(CheckRunProgressActivity.Output(false)) // false == complete failure
    // just run once
    val manager = RetryManager(null, null, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, 1, Int.Companion.MAX_VALUE)

    Mockito
      .`when`<HydrateOutput?>(mRetryStatePersistenceActivity.hydrateRetryState(any()))
      .thenReturn(HydrateOutput(manager))
    Mockito
      .`when`<PersistOutput?>(mRetryStatePersistenceActivity.persistRetryState(any()))
      .thenReturn(PersistOutput(true))
    Mockito
      .`when`<LogOutput?>(mAppendToAttemptLogActivity.log(any()))
      .thenReturn(LogOutput(true))

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

    val activityOptionsBeanIdentifier = Mockito.mock(BeanIdentifier::class.java)
    val activityOptionsBeanRegistration = Mockito.mock<BeanRegistration<ActivityOptions>>()
    Mockito.`when`<String?>(activityOptionsBeanIdentifier.getName()).thenReturn("shortActivityOptions")
    Mockito.`when`<BeanIdentifier?>(activityOptionsBeanRegistration.getIdentifier()).thenReturn(activityOptionsBeanIdentifier)
    Mockito.`when`<ActivityOptions?>(activityOptionsBeanRegistration.getBean()).thenReturn(activityOptions)
    temporalProxyHelper = TemporalProxyHelper(listOf(activityOptionsBeanRegistration))
  }

  @Throws(Exception::class)
  private fun returnTrueForLastJobOrAttemptFailure() {
    Mockito
      .`when`(mJobCreationAndStatusUpdateActivity.isLastJobOrAttemptFailure(any()))
      .thenReturn(true)

    val jobRunConfig = JobRunConfig()
    jobRunConfig.setJobId(JOB_ID.toString())
    jobRunConfig.setAttemptId(ATTEMPT_ID.toLong())
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
    @Throws(Exception::class)
    fun runSuccess() {
      returnTrueForLastJobOrAttemptFailure()
      Mockito
        .`when`(mConfigFetchActivity.getTimeToWait(any()))
        .thenReturn(ScheduleRetrieverOutput(SCHEDULE_WAIT))

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
          false,
          false,
          false,
        )

      Companion.startWorkflowAndWaitUntilReady(workflow, input)
      // wait to be scheduled, then to run, then schedule again
      testEnv.sleep(Duration.ofMinutes(SCHEDULE_WAIT.toMinutes() + SCHEDULE_WAIT.toMinutes() + 1))
      Mockito.verify(mConfigFetchActivity, Mockito.atLeast(2)).getTimeToWait(any())
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
    @Throws(Exception::class)
    fun retryAfterFail() {
      returnTrueForLastJobOrAttemptFailure()
      Mockito
        .`when`(mConfigFetchActivity.getTimeToWait(any()))
        .thenReturn(ScheduleRetrieverOutput(SCHEDULE_WAIT))

      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)

      val input =
        ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          true,
          1,
          workflowState,
          false,
          false,
          false,
        )

      Companion.startWorkflowAndWaitUntilReady(workflow, input)
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
    @Throws(
      Exception::class,
    )
    fun manualRun() {
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
          false,
          false,
          false,
        )

      Companion.startWorkflowAndWaitUntilReady(workflow, input)
      testEnv.sleep(Duration.ofMinutes(1L)) // any value here, just so it's started
      workflow.submitManualSync()

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
        .jobSuccessWithAttemptNumber(any<JobSuccessInputWithAttemptNumber>())

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
    @Throws(
      Exception::class,
    )
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
          false,
          false,
          false,
        )

      Companion.startWorkflowAndWaitUntilReady(workflow, input)
      testEnv.sleep(Duration.ofSeconds(30L))

      Mockito.reset(mConfigFetchActivity)
      Mockito
        .`when`(mConfigFetchActivity.getConnectionContext(any()))
        .thenReturn(
          GetConnectionContextOutput(
            ConnectionContext().withSourceId(SOURCE_ID).withDestinationId(DESTINATION_ID),
          ),
        )
      Mockito.`when`(mConfigFetchActivity.getLoadShedBackoff(any())).thenReturn(
        GetLoadShedBackoffOutput(
          Duration.ZERO,
        ),
      )
      Mockito.`when`(mConfigFetchActivity.getTimeToWait(any())).thenReturn(
        ScheduleRetrieverOutput(
          Duration.ofDays((100 * 365).toLong()),
        ),
      )
      workflow.connectionUpdated()

      Mockito
        .verify(mConfigFetchActivity, VERIFY_TIMEOUT)
        .getTimeToWait(any<ScheduleRetrieverInput>())

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

      Mockito.verifyNoInteractions(mJobCreationAndStatusUpdateActivity)
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that cancelling a non-running workflow doesn't do anything")
    @Throws(
      Exception::class,
    )
    fun cancelNonRunning() {
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
          false,
          false,
          false,
        )

      Companion.startWorkflowAndWaitUntilReady(workflow, input)
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

      Mockito.verifyNoInteractions(mJobCreationAndStatusUpdateActivity)
    }

    // TODO: delete when the signal method can be removed
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that the sync is properly deleted")
    @Throws(Exception::class)
    fun deleteSync() {
      returnTrueForLastJobOrAttemptFailure()
      val testId = UUID.randomUUID()
      val testStateListener = TestStateListener()
      val workflowState = Mockito.spy(WorkflowState(testId, testStateListener))

      val input =
        ConnectionUpdaterInput(
          UUID.randomUUID(),
          JOB_ID,
          ATTEMPT_ID,
          false,
          1,
          workflowState,
          false,
          false,
          false,
        )

      Companion.startWorkflowAndWaitUntilReady(workflow, input)
      testEnv.sleep(Duration.ofSeconds(30L))
      workflow.deleteConnection()

      Companion.waitUntilDeleted(workflow)

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
    @Throws(Exception::class)
    fun testStartFromCleanJobState() {
      returnTrueForLastJobOrAttemptFailure()
      val input =
        ConnectionUpdaterInput(
          UUID.randomUUID(),
          null,
          null,
          false,
          1,
          null,
          false,
          false,
          false,
        )

      Companion.startWorkflowAndWaitUntilReady(workflow, input)
      testEnv.sleep(Duration.ofSeconds(30L))

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, Mockito.times(1))
        .ensureCleanJobState(any())
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
    @Throws(
      Exception::class,
    )
    fun manualRun() {
      returnTrueForLastJobOrAttemptFailure()
      Mockito
        .`when`(mConfigFetchActivity.getTimeToWait(any()))
        .thenReturn(ScheduleRetrieverOutput(SCHEDULE_WAIT))

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
          false,
          false,
          false,
        )

      Companion.startWorkflowAndWaitUntilReady(workflow, input)

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
    @Throws(Exception::class)
    fun cancelRunning() {
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
          false,
          false,
          false,
        )

      Companion.startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      // wait for the manual sync to start working
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.cancelJob()

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
        .jobCancelledWithAttemptNumber(any())

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
            changedStateEvent!!.field == StateField.CANCELLED &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThanOrEqualTo(1)
    }

    @Timeout(value = 40, unit = TimeUnit.SECONDS)
    @DisplayName("Test that deleting a running workflow cancels the sync")
    @Throws(Exception::class)
    fun deleteRunning() {
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
          false,
          false,
          false,
        )

      Companion.startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      // wait for the manual sync to start working
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.deleteConnection()

      val eventQueue: Queue<ChangedStateEvent> = testStateListener.events(testId)
      val events: MutableList<ChangedStateEvent> = ArrayList(eventQueue)

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
        .jobCancelledWithAttemptNumber(any())

      for (event in events) {
        if (event.isValue) {
          log.info(EVENT + event)
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

      Assertions
        .assertThat(events)
        .filteredOn(
          Predicate { changedStateEvent: ChangedStateEvent? ->
            changedStateEvent!!.field == StateField.DELETED &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThanOrEqualTo(1)
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that resetting a non-running workflow starts a reset job")
    @Throws(
      Exception::class,
    )
    fun resetStart() {
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
          false,
          false,
          false,
        )

      Companion.startWorkflowAndWaitUntilReady(workflow, input)
      testEnv.sleep(Duration.ofMinutes(5L))
      workflow.resetConnection()
      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
        .createNewAttemptNumber(any<AttemptCreationInput>())

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
    @Throws(
      Exception::class,
    )
    fun resetAndContinue() {
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
          false,
          false,
          false,
        )

      Companion.startWorkflowAndWaitUntilReady(workflow, input)
      testEnv.sleep(Duration.ofMinutes(5L))
      workflow.resetConnectionAndSkipNextScheduling()
      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
        .createNewAttemptNumber(any<AttemptCreationInput>())

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
    @Throws(
      Exception::class,
    )
    fun resetCancelRunningWorkflow() {
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
          false,
          false,
          false,
        )

      Companion.startWorkflowAndWaitUntilReady(workflow, input)

      testEnv.sleep(Duration.ofSeconds(30L))
      workflow.submitManualSync()
      testEnv.sleep(Duration.ofSeconds(30L))
      workflow.resetConnection()

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
        .jobCancelledWithAttemptNumber(any<JobCancelledInputWithAttemptNumber>())

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
            changedStateEvent!!.field == StateField.CANCELLED_FOR_RESET &&
              changedStateEvent.isValue
          },
        ).hasSizeGreaterThanOrEqualTo(1)
    }

    @Test
    @DisplayName("Test that running workflow which receives an update signal waits for the current run and reports the job status")
    @Throws(
      Exception::class,
    )
    fun updatedSignalReceivedWhileRunning() {
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
          false,
          false,
          false,
        )

      Companion.startWorkflowAndWaitUntilReady(workflow, input)

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

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity)
        .jobSuccessWithAttemptNumber(any<JobSuccessInputWithAttemptNumber>())
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
    @Throws(
      Exception::class,
    )
    fun testAutoDisableOnFailure() {
      val connectionId = UUID.randomUUID()
      setupSourceAndDestinationFailure(connectionId)

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, Mockito.atLeastOnce())
        .attemptFailureWithAttemptNumber(any())
      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, Mockito.atLeastOnce())
        .jobFailure(any())
      val autoDisableConnectionActivityInput = AutoDisableConnectionActivityInput()
      autoDisableConnectionActivityInput.connectionId = connectionId
      Mockito
        .verify(mAutoDisableConnectionActivity)
        .autoDisableFailingConnection(autoDisableConnectionActivityInput)
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that auto disable activity is not touched during job success")
    @Throws(
      Exception::class,
    )
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
          connectionId,
          JOB_ID,
          ATTEMPT_ID,
          false,
          0,
          workflowState,
          false,
          false,
          false,
        )

      ConnectionManagerWorkflowTest.Companion.startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      Mockito.verifyNoInteractions(mAutoDisableConnectionActivity)
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
    @Throws(Exception::class)
    fun testSourceCheckFailuresRecorded() {
      returnTrueForLastJobOrAttemptFailure()
      Mockito
        .`when`(mJobCreationAndStatusUpdateActivity.createNewJob(any()))
        .thenReturn(JobCreationOutput(JOB_ID))
      Mockito
        .`when`(
          mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()),
        ).thenReturn(AttemptNumberCreationOutput(ATTEMPT_ID))

      val checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionFailedWorkflow::class.java)

      testEnv.start()

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
          false,
          false,
          false,
        )

      ConnectionManagerWorkflowTest.Companion.startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
        .attemptFailureWithAttemptNumber(
          any(),
        )
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that Source CHECK failures are recorded when running in child workflow")
    @Throws(
      Exception::class,
    )
    fun testSourceCheckInChildWorkflowFailuresRecorded() {
      returnTrueForLastJobOrAttemptFailure()
      Mockito
        .`when`(mJobCreationAndStatusUpdateActivity.createNewJob(any()))
        .thenReturn(JobCreationOutput(JOB_ID))
      Mockito
        .`when`(
          mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()),
        ).thenReturn(AttemptNumberCreationOutput(ATTEMPT_ID))
      val checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionFailedWorkflow::class.java)
      testEnv.start()

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
          false,
          false,
          false,
        )

      ConnectionManagerWorkflowTest.Companion.startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
        .attemptFailureWithAttemptNumber(
          any(),
        )
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that Source CHECK failure reasons are recorded")
    @Throws(Exception::class)
    fun testSourceCheckFailureReasonsRecorded() {
      returnTrueForLastJobOrAttemptFailure()
      Mockito
        .`when`(mJobCreationAndStatusUpdateActivity.createNewJob(any()))
        .thenReturn(JobCreationOutput(JOB_ID))
      Mockito
        .`when`(
          mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()),
        ).thenReturn(AttemptNumberCreationOutput(ATTEMPT_ID))
      val checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionSystemErrorWorkflow::class.java)
      testEnv.start()

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
          false,
          false,
          false,
        )

      ConnectionManagerWorkflowTest.Companion.startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
        .attemptFailureWithAttemptNumber(
          any(),
        )
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that Destination CHECK failures are recorded")
    @Throws(Exception::class)
    fun testDestinationCheckFailuresRecorded() {
      returnTrueForLastJobOrAttemptFailure()
      Mockito
        .`when`(mJobCreationAndStatusUpdateActivity.createNewJob(any()))
        .thenReturn(JobCreationOutput(JOB_ID))
      Mockito
        .`when`(
          mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()),
        ).thenReturn(AttemptNumberCreationOutput(ATTEMPT_ID))
      val checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionSourceSuccessOnlyWorkflow::class.java)

      testEnv.start()

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
          false,
          false,
          false,
        )

      ConnectionManagerWorkflowTest.Companion.startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
        .attemptFailureWithAttemptNumber(
          any(),
        )
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that Destination CHECK failure reasons are recorded")
    @Throws(Exception::class)
    fun testDestinationCheckFailureReasonsRecorded() {
      returnTrueForLastJobOrAttemptFailure()
      Mockito
        .`when`(mJobCreationAndStatusUpdateActivity.createNewJob(any()))
        .thenReturn(JobCreationOutput(JOB_ID))
      Mockito
        .`when`(
          mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()),
        ).thenReturn(AttemptNumberCreationOutput(ATTEMPT_ID))
      val checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionDestinationSystemErrorWorkflow::class.java)

      testEnv.start()

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
          false,
          false,
          false,
        )

      ConnectionManagerWorkflowTest.Companion.startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, VERIFY_TIMEOUT)
        .attemptFailureWithAttemptNumber(
          any(),
        )
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that reset workflows do not CHECK the source")
    @Throws(Exception::class)
    fun testSourceCheckSkippedWhenReset() {
      Mockito
        .`when`(mJobCreationAndStatusUpdateActivity.isLastJobOrAttemptFailure(any()))
        .thenReturn(true)

      val jobRunConfig = JobRunConfig()
      jobRunConfig.setJobId(JOB_ID.toString())
      jobRunConfig.setAttemptId(ATTEMPT_ID.toLong())

      Mockito
        .`when`(mJobCreationAndStatusUpdateActivity.createNewJob(any()))
        .thenReturn(JobCreationOutput(JOB_ID))
      Mockito
        .`when`(
          mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()),
        ).thenReturn(AttemptNumberCreationOutput(ATTEMPT_ID))
      val checkWorker = testEnv.newWorker(TemporalJobType.SYNC.name)
      checkWorker.registerWorkflowImplementationTypes(CheckConnectionFailedWorkflow::class.java)

      testEnv.start()

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
          false,
          false,
          false,
        )

      ConnectionManagerWorkflowTest.Companion.startWorkflowAndWaitUntilReady(workflow, input)

      // wait for workflow to initialize
      testEnv.sleep(Duration.ofMinutes(1))

      workflow.submitManualSync()

      Mockito
        .verify(
          mJobCreationAndStatusUpdateActivity,
          Mockito.timeout(TEN_SECONDS.toLong()).atLeastOnce(),
        ).attemptFailureWithAttemptNumber(
          any(),
        )
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Test that source and destination failures are recorded")
    @Throws(Exception::class)
    fun testSourceAndDestinationFailuresRecorded() {
      setupSourceAndDestinationFailure(UUID.randomUUID())

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity)
        .attemptFailureWithAttemptNumber(
          any(),
        )
      Mockito
        .verify(mJobCreationAndStatusUpdateActivity)
        .attemptFailureWithAttemptNumber(
          any(),
        )
    }
  }

  @Nested
  @DisplayName("Test that the workflow is properly restarted after activity failures.")
  internal inner class FailedActivityWorkflow {
    @ParameterizedTest
    @MethodSource("io.airbyte.workers.temporal.scheduling.ConnectionManagerWorkflowTest#getSetupFailingActivity")
    @Throws(Exception::class)
    fun testWorkflowRestartedAfterFailedActivity(
      mockSetup: Thread,
      expectedEventsCount: Int,
    ) {
      returnTrueForLastJobOrAttemptFailure()
      mockSetup.run()
      Mockito.`when`(mConfigFetchActivity.getTimeToWait(any())).thenReturn(
        ScheduleRetrieverOutput(
          Duration.ZERO,
        ),
      )

      val testId = UUID.randomUUID()
      reset()
      val testStateListener = TestStateListener()
      val workflowState = WorkflowState(testId, testStateListener)

      val input =
        ConnectionUpdaterInput(
          UUID.randomUUID(),
          null,
          null,
          false,
          1,
          workflowState,
          false,
          false,
          false,
        )

      ConnectionManagerWorkflowTest.Companion.startWorkflowAndWaitUntilReady(workflow, input)

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
    @Throws(
      Exception::class,
    )
    fun checksProgressOnFailure(failureCase: Class<out SyncWorkflowV2?>?) {
      // We check attempt progress using the 0-based attempt number counting system used everywhere except
      // the ConnectionUpdaterInput where it is 1-based. This will be fixed to be more consistent later.
      // The concrete value passed here is inconsequential—the important part is that it is _not_ the
      // attempt number set on the ConnectionUpdaterInput.
      val attemptNumber = 42
      Mockito
        .`when`(
          mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(any()),
        ).thenReturn(AttemptNumberCreationOutput(attemptNumber))

      setupFailureCase(failureCase)

      val captor =
        argumentCaptor<CheckRunProgressActivity.Input>()
      Mockito.verify(mCheckRunProgressActivity, Mockito.times(1)).checkProgress(captor.capture())
      Assertions.assertThat(captor.firstValue.jobId).isEqualTo(JOB_ID)
      Assertions.assertThat(captor.firstValue.attemptNo).isEqualTo(attemptNumber)
    }

    @ParameterizedTest
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("We hydrate, persist and use retry manager.")
    @MethodSource("io.airbyte.workers.temporal.scheduling.ConnectionManagerWorkflowTest#coreFailureTypesMatrix")
    @Disabled("Flaky in CI.")
    @Throws(
      Exception::class,
    )
    fun hydratePersistRetryManagerFlow(failureCase: Class<out SyncWorkflowV2?>?) {
      val connectionId = UUID.randomUUID()
      val jobId = 32198714L
      val input = testInputBuilder()
      input.connectionId = connectionId
      input.jobId = null

      val retryLimit = 2

      val manager1 = RetryManager(null, null, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, retryLimit)
      val manager2 = RetryManager(null, null, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, retryLimit, 0, 1, 0, 1)
      val manager3 = RetryManager(null, null, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, retryLimit)

      Mockito
        .`when`(mJobCreationAndStatusUpdateActivity.createNewJob(any()))
        .thenReturn(JobCreationOutput(jobId))
      Mockito
        .`when`(mRetryStatePersistenceActivity.hydrateRetryState(any()))
        .thenReturn(HydrateOutput(manager1)) // run 1: pre scheduling
        .thenReturn(HydrateOutput(manager1)) // run 1: pre run
        .thenReturn(HydrateOutput(manager2)) // run 2: pre scheduling
        .thenReturn(HydrateOutput(manager2)) // run 2: pre run
        .thenReturn(HydrateOutput(manager3)) // run 3: pre run
      Mockito
        .`when`(mCheckRunProgressActivity.checkProgress(any()))
        .thenReturn(CheckRunProgressActivity.Output(true)) // true to hit partial failure limit

      setupFailureCase(failureCase, input)

      // Wait a little extra for resiliency
      val hydrateCaptor = argumentCaptor<HydrateInput>()
      val persistCaptor = argumentCaptor<PersistInput>()
      // If the test timeouts expire before we wrap around to the backoff/scheduling step it will run
      // exactly twice per attempt.
      // Otherwise, there's 1 extra hydration to resolve backoff.
      Mockito
        .verify(
          mRetryStatePersistenceActivity,
          Mockito.timeout(TEN_SECONDS.toLong()).atLeast(2 * retryLimit),
        ).hydrateRetryState(
          hydrateCaptor.capture(),
        )
      Mockito
        .verify(mCheckRunProgressActivity, Mockito.timeout(TEN_SECONDS.toLong()).times(retryLimit))
        .checkProgress(any())
      Mockito
        .verify(mRetryStatePersistenceActivity, Mockito.timeout(TEN_SECONDS.toLong()).times(retryLimit))
        .persistRetryState(
          persistCaptor.capture(),
        )
      Mockito
        .verify(
          mJobCreationAndStatusUpdateActivity,
          Mockito.timeout(TEN_SECONDS.toLong()).times(retryLimit),
        ).createNewAttemptNumber(any())

      // run 1: hydrate pre scheduling
      Assertions.assertThat<UUID?>(hydrateCaptor.allValues.get(0).connectionId).isEqualTo(connectionId)
      Assertions.assertThat(hydrateCaptor.allValues.get(0).jobId).isEqualTo(null)
      // run 1: hydrate pre run
      Assertions.assertThat<UUID?>(hydrateCaptor.allValues.get(1).connectionId).isEqualTo(connectionId)
      Assertions.assertThat(hydrateCaptor.allValues.get(1).jobId).isEqualTo(null)
      // run 1: persist
      Assertions.assertThat<UUID?>(persistCaptor.allValues.get(0).connectionId).isEqualTo(connectionId)
      Assertions.assertThat(persistCaptor.allValues.get(0).jobId).isEqualTo(jobId)
      Assertions
        .assertThat(
          persistCaptor.allValues
            .get(0)
            .manager!!
            .successivePartialFailures,
        ).isEqualTo(1)
      Assertions
        .assertThat(
          persistCaptor.allValues
            .get(0)
            .manager!!
            .totalPartialFailures,
        ).isEqualTo(1)

      // run 2: hydrate pre scheduling
      Assertions.assertThat<UUID?>(hydrateCaptor.allValues.get(2).connectionId).isEqualTo(connectionId)
      Assertions.assertThat(hydrateCaptor.allValues.get(2).jobId).isEqualTo(jobId)
      // run 2: hydrate pre run
      Assertions.assertThat<UUID?>(hydrateCaptor.allValues.get(3).connectionId).isEqualTo(connectionId)
      Assertions.assertThat(hydrateCaptor.allValues.get(3).jobId).isEqualTo(jobId)
      // run 2: persist
      Assertions.assertThat<UUID?>(persistCaptor.allValues.get(1).connectionId).isEqualTo(connectionId)
      Assertions.assertThat(persistCaptor.allValues.get(1).jobId).isEqualTo(jobId)
      Assertions
        .assertThat(
          persistCaptor.allValues
            .get(1)
            .manager!!
            .successivePartialFailures,
        ).isEqualTo(2)
      Assertions
        .assertThat(
          persistCaptor.allValues
            .get(1)
            .manager!!
            .totalPartialFailures,
        ).isEqualTo(2)
      // run 3: hydrate pre scheduling
      Assertions.assertThat<UUID?>(hydrateCaptor.allValues.get(4).connectionId).isEqualTo(connectionId)
      Assertions.assertThat(hydrateCaptor.allValues.get(4).jobId).isEqualTo(null)
    }

    @ParameterizedTest
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("We use attempt-based retries when retry manager not present.")
    @MethodSource("io.airbyte.workers.temporal.scheduling.ConnectionManagerWorkflowTest#coreFailureTypesMatrix")
    @Throws(
      Exception::class,
    )
    fun usesAttemptBasedRetriesIfRetryManagerUnset(failureCase: Class<out SyncWorkflowV2?>?) {
      val connectionId = UUID.randomUUID()
      val jobId = 32198714L
      val input = testInputBuilder()
      input.connectionId = connectionId
      input.jobId = null

      val retryLimit = 1

      // attempt-based retry configuration
      Mockito.`when`(mConfigFetchActivity.getMaxAttempt()).thenReturn(GetMaxAttemptOutput(retryLimit))

      Mockito
        .`when`(mJobCreationAndStatusUpdateActivity.createNewJob(any()))
        .thenReturn(JobCreationOutput(jobId))
      Mockito
        .`when`(mRetryStatePersistenceActivity.hydrateRetryState(any()))
        .thenReturn(HydrateOutput(null))
      Mockito
        .`when`(mCheckRunProgressActivity.checkProgress(any()))
        .thenReturn(CheckRunProgressActivity.Output(true))

      setupFailureCase(failureCase, input)

      Mockito
        .verify(mRetryStatePersistenceActivity, Mockito.never())
        .persistRetryState(any())
      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, Mockito.times(retryLimit))
        .createNewAttemptNumber(any())
    }

    @ParameterizedTest
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Uses scheduling resolution if no retry manager.")
    @MethodSource("io.airbyte.workers.temporal.scheduling.ConnectionManagerWorkflowTest#noBackoffSchedulingMatrix")
    @Throws(
      Exception::class,
    )
    fun useSchedulingIfNoRetryManager(
      fromFailure: Boolean,
      timeToWait: Duration,
    ) {
      val timeTilNextScheduledRun = Duration.ofHours(1)
      Mockito
        .`when`(mConfigFetchActivity.getTimeToWait(any()))
        .thenReturn(ScheduleRetrieverOutput(timeTilNextScheduledRun))

      Mockito
        .`when`(mRetryStatePersistenceActivity.hydrateRetryState(any()))
        .thenReturn(HydrateOutput(null))

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
    @Throws(
      Exception::class,
    )
    fun useSchedulingIfNotFromFailure() {
      val backoff = Duration.ofMinutes(1)
      val policy = BackoffPolicy(backoff, backoff)
      val manager =
        RetryManager(policy, null, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE)
      manager.successiveCompleteFailures = 1

      Mockito
        .`when`(mRetryStatePersistenceActivity.hydrateRetryState(any()))
        .thenReturn(HydrateOutput(manager))

      val timeTilNextScheduledRun = Duration.ofHours(1)
      Mockito
        .`when`(mConfigFetchActivity.getTimeToWait(any()))
        .thenReturn(ScheduleRetrieverOutput(timeTilNextScheduledRun))

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
    @Throws(
      Exception::class,
    )
    fun usesBackoffPolicyIfPresent(minutes: Long) {
      val backoff = Duration.ofMinutes(minutes)
      val policy = BackoffPolicy(backoff, backoff)
      val manager =
        RetryManager(policy, null, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE)
      manager.successiveCompleteFailures = 1

      Mockito
        .`when`(mRetryStatePersistenceActivity.hydrateRetryState(any()))
        .thenReturn(HydrateOutput(manager))

      Mockito
        .`when`(mConfigFetchActivity.getTimeToWait(any()))
        .thenReturn(ScheduleRetrieverOutput(Duration.ofDays(1)))

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
    @Throws(
      Exception::class,
    )
    fun cancelWorksDuringBackoff(minutes: Long) {
      val backoff = Duration.ofMinutes(minutes)
      val policy = BackoffPolicy(backoff, backoff)
      val manager =
        RetryManager(policy, null, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE)
      manager.successiveCompleteFailures = 1

      Mockito
        .`when`(mRetryStatePersistenceActivity.hydrateRetryState(any()))
        .thenReturn(HydrateOutput(manager))

      Mockito
        .`when`(mConfigFetchActivity.getTimeToWait(any()))
        .thenReturn(ScheduleRetrieverOutput(Duration.ofDays(1)))

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

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity)
        .jobCancelledWithAttemptNumber(
          any(),
        ) // input attempt number is 1 based

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
    @Throws(
      Exception::class,
    )
    fun doesNotFailJobIfBackoffTooLong(backoffMinutes: Long) {
      val backoff = Duration.ofMinutes(backoffMinutes)
      val policy = BackoffPolicy(backoff, backoff)
      val manager =
        RetryManager(policy, null, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE, Int.Companion.MAX_VALUE)
      manager.successiveCompleteFailures = 1

      Mockito
        .`when`(mRetryStatePersistenceActivity.hydrateRetryState(any()))
        .thenReturn(HydrateOutput(manager))

      val timeTilNextScheduledRun = Duration.ofMinutes(60)
      Mockito
        .`when`(mConfigFetchActivity.getTimeToWait(any()))
        .thenReturn(ScheduleRetrieverOutput(timeTilNextScheduledRun))

      val input = testInputBuilder()
      input.fromFailure = true

      setupSuccessfulWorkflow(input)
      testEnv.sleep(Duration.ofMinutes(1))

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity, Mockito.times(0))
        .jobFailure(any())
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
    @Throws(Exception::class)
    fun reportsCancelledWhenConnectionDisabled() {
      val input = testInputBuilder()
      setupSuccessfulWorkflow(CancelledSyncWorkflow::class.java, input)
      workflow.submitManualSync()
      testEnv.sleep(Duration.ofSeconds(60))

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity)
        .jobCancelledWithAttemptNumber(any<JobCancelledInputWithAttemptNumber>())
    }

    @Test
    fun `When a sync is cancelled, we still execute the post processing`() {
      val input = testInputBuilder()
      setupSuccessfulWorkflow(CancelledSyncWorkflow::class.java, input)
      workflow.submitManualSync()
      testEnv.sleep(Duration.ofSeconds(60))

      Mockito
        .verify(mJobPostProcessingActivity)
        .wasStarted()
    }

    @Test
    fun `When a sync fails, we execute the post processing`() {
      val input = testInputBuilder()
      setupSuccessfulWorkflow(SyncWorkflowFailingOutputWorkflow::class.java, input)
      workflow.submitManualSync()
      testEnv.sleep(Duration.ofSeconds(60))

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity)
        .jobFailure(any<JobCreationAndStatusUpdateActivity.JobFailureInput>())

      Mockito
        .verify(mJobPostProcessingActivity)
        .wasStarted()
    }

    @Test
    fun `When a sync succeeds, we execute the post processing`() {
      val input = testInputBuilder()
      setupSuccessfulWorkflow(EmptySyncWorkflow::class.java, input)
      workflow.submitManualSync()
      testEnv.sleep(Duration.ofSeconds(60))

      Mockito
        .verify(mJobCreationAndStatusUpdateActivity)
        .jobSuccessWithAttemptNumber(any<JobSuccessInputWithAttemptNumber>())

      Mockito
        .verify(mJobPostProcessingActivity)
        .wasStarted()
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
    @Throws(Exception::class)
    fun happyPath() {
      val backoff = Duration.ofMinutes(10)
      val backoffOutput = GetLoadShedBackoffOutput(backoff)
      val zeroBackoffOutput = GetLoadShedBackoffOutput(Duration.ZERO)
      val zeroSchedulerOutput = ScheduleRetrieverOutput(Duration.ZERO)

      // back off 3 times, then unblock
      Mockito
        .`when`(mConfigFetchActivity.getLoadShedBackoff(any()))
        .thenReturn(backoffOutput, backoffOutput, backoffOutput, zeroBackoffOutput)

      Mockito
        .`when`(mConfigFetchActivity.getTimeToWait(any()))
        .thenReturn(zeroSchedulerOutput)
      Mockito
        .`when`(mConfigFetchActivity.isWorkspaceTombstone(any()))
        .thenReturn(false)

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
          false,
          false,
          false,
        )

      setupSuccessfulWorkflow(input)
      // wait a sec til we get to the backoff step before calling verify
      testEnv.sleep(Duration.ofMinutes(1))
      Mockito.verify(mConfigFetchActivity, Mockito.times(1)).getLoadShedBackoff(any())
      // we will check the load shed flag after each backoff
      testEnv.sleep(backoff)
      Mockito.verify(mConfigFetchActivity, Mockito.times(2)).getLoadShedBackoff(any())
      testEnv.sleep(backoff)
      Mockito.verify(mConfigFetchActivity, Mockito.times(3)).getLoadShedBackoff(any())
      testEnv.sleep(backoff)
      // verify we have exited the loop and will continue
      Mockito
        .verify(mConfigFetchActivity, Mockito.times(1))
        .getTimeToWait(any<ScheduleRetrieverInput>())
    }
  }

  private inner class HasFailureFromOrigin(
    private val expectedFailureOrigin: FailureReason.FailureOrigin?,
  ) : ArgumentMatcher<AttemptNumberFailureInput?> {
    override fun matches(arg: AttemptNumberFailureInput?): Boolean =
      arg!!
        .attemptFailureSummary!!
        .getFailures()
        .stream()
        .anyMatch { f: FailureReason? -> f!!.getFailureOrigin() == expectedFailureOrigin }
  }

  private inner class HasFailureFromOriginWithType(
    private val expectedFailureOrigin: FailureReason.FailureOrigin?,
    private val expectedFailureType: FailureReason.FailureType?,
  ) : ArgumentMatcher<AttemptNumberFailureInput?> {
    override fun matches(arg: AttemptNumberFailureInput?): Boolean {
      val stream = arg!!.attemptFailureSummary!!.getFailures().stream()
      return stream.anyMatch { f: FailureReason? ->
        f!!.getFailureOrigin() == expectedFailureOrigin && f.getFailureType() == expectedFailureType
      }
    }
  }

  private inner class HasCancellationFailure(
    private val expectedJobId: Long,
    private val expectedAttemptNumber: Int,
  ) : ArgumentMatcher<JobCancelledInputWithAttemptNumber?> {
    override fun matches(arg: JobCancelledInputWithAttemptNumber?): Boolean =
      arg!!
        .attemptFailureSummary!!
        .getFailures()
        .stream()
        .anyMatch { f: FailureReason? -> f!!.getFailureType() == FailureReason.FailureType.MANUAL_CANCELLATION } &&
        arg.jobId == expectedJobId &&
        arg.attemptNumber == expectedAttemptNumber
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

    client = testEnv.getWorkflowClient()
    testEnv.start()

    workflow =
      client!!
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
        .setNamespace(testEnv.getNamespace())
        .setExecutionFilter(WorkflowExecutionFilter.newBuilder().setWorkflowId(WORKFLOW_ID))
        .build()
    val listResponse =
      testEnv
        .getWorkflowService()
        .blockingStub()
        .listClosedWorkflowExecutions(request)
    Assertions.assertThat(listResponse.getExecutionsCount()).isGreaterThanOrEqualTo(1)
    Assertions
      .assertThat(listResponse.getExecutionsList().get(0).getStatus())
      .isEqualTo(WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW)
  }

  private fun testInputBuilder(): ConnectionUpdaterInput {
    val testId = UUID.randomUUID()
    val testStateListener = TestStateListener()
    val workflowState = WorkflowState(testId, testStateListener)

    return ConnectionUpdaterInput(
      UUID.randomUUID(),
      JOB_ID,
      ATTEMPT_ID,
      false,
      ATTEMPT_NO,
      workflowState,
      false,
      false,
      false,
    )
  }

  /**
   * Given a failure case class, this will set up a manual sync to fail in that fashion.
   * ConnectionUpdaterInput is pluggable for various test needs. Feel free to update input/return
   * values as is necessary.
   */
  @Throws(Exception::class)
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

    Companion.startWorkflowAndWaitUntilReady(workflow, input)

    // wait for workflow to initialize
    testEnv.sleep(Duration.ofMinutes(1))

    workflow.submitManualSync()
    testEnv.sleep(Duration.ofMinutes(1))
  }

  @Throws(Exception::class)
  private fun setupFailureCase(failureClass: Class<out SyncWorkflowV2?>?) {
    val input = testInputBuilder()

    setupFailureCase(failureClass, input)
  }

  @Throws(Exception::class)
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

    client = testEnv.getWorkflowClient()
    workflow =
      client.newWorkflowStub(
        ConnectionManagerWorkflow::class.java,
        WorkflowOptions.newBuilder().setTaskQueue(TemporalJobType.CONNECTION_UPDATER.name).build(),
      )
  }

  @Throws(Exception::class)
  private fun setupSuccessfulWorkflow(input: ConnectionUpdaterInput?) {
    setupSuccessfulWorkflow(EmptySyncWorkflow::class.java, input)
  }

  @Throws(Exception::class)
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

    Companion.startWorkflowAndWaitUntilReady(workflow, input)
  }

  companion object {
    private val log: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    private const val JOB_ID = 1L
    private const val ATTEMPT_ID = 1
    private const val ATTEMPT_NO = 1
    val SOURCE_ID: UUID = UUID.randomUUID()
    val DESTINATION_ID: UUID = UUID.randomUUID()

    private val SCHEDULE_WAIT: Duration = Duration.ofMinutes(20L)
    private const val WORKFLOW_ID = "workflow-id"

    private val WORKFLOW_FAILURE_RESTART_DELAY: Duration = Duration.ofSeconds(600)
    private const val SOURCE_DOCKER_IMAGE = "some_source"

    private val TEN_SECONDS = 10 * 1000
    private val VERIFY_TIMEOUT: VerificationMode? = Mockito.timeout(TEN_SECONDS.toLong()).times(1)

    private val mJobCreationAndStatusUpdateActivity: JobCreationAndStatusUpdateActivity =
      Mockito.mock<JobCreationAndStatusUpdateActivity>(
        JobCreationAndStatusUpdateActivity::class.java,
        Mockito.withSettings().withoutAnnotations(),
      )
    private val mAutoDisableConnectionActivity: AutoDisableConnectionActivity =
      Mockito.mock<AutoDisableConnectionActivity>(AutoDisableConnectionActivity::class.java, Mockito.withSettings().withoutAnnotations())
    private val mStreamResetActivity: StreamResetActivity? =
      Mockito.mock<StreamResetActivity?>(StreamResetActivity::class.java, Mockito.withSettings().withoutAnnotations())
    private val mRecordMetricActivity: RecordMetricActivity? =
      Mockito.mock<RecordMetricActivity?>(RecordMetricActivity::class.java, Mockito.withSettings().withoutAnnotations())
    private val mWorkflowConfigActivity: WorkflowConfigActivity =
      Mockito.mock<WorkflowConfigActivity>(WorkflowConfigActivity::class.java, Mockito.withSettings().withoutAnnotations())
    private val mFeatureFlagFetchActivity: FeatureFlagFetchActivity =
      Mockito.mock<FeatureFlagFetchActivity>(FeatureFlagFetchActivity::class.java, Mockito.withSettings().withoutAnnotations())
    private val mCheckRunProgressActivity: CheckRunProgressActivity =
      Mockito.mock<CheckRunProgressActivity>(CheckRunProgressActivity::class.java, Mockito.withSettings().withoutAnnotations())
    private val mRetryStatePersistenceActivity: RetryStatePersistenceActivity =
      Mockito.mock<RetryStatePersistenceActivity>(RetryStatePersistenceActivity::class.java, Mockito.withSettings().withoutAnnotations())
    private val mAppendToAttemptLogActivity: AppendToAttemptLogActivity =
      Mockito.mock<AppendToAttemptLogActivity>(AppendToAttemptLogActivity::class.java, Mockito.withSettings().withoutAnnotations())
    private const val EVENT = "event = "
    private const val FAILED_CHECK_MESSAGE = "nope"

    val maxAttemptForResetRetry: Stream<Arguments?>
      get() =
        Stream.of<Arguments?>( // "The max attempt is 3, it will test that after a failed reset attempt the next attempt will also
          // be a reset")
          Arguments.of(3), // "The max attempt is 3, it will test that after a failed reset job the next attempt will also be a
          // job")
          Arguments.of(1),
        )

    @Throws(InterruptedException::class)
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
        } catch (e: Exception) {
          log.info("retrying...")
          Thread.sleep(100)
        }
      }
    }

    @Throws(InterruptedException::class)
    private fun waitUntilDeleted(workflow: ConnectionManagerWorkflow) {
      var isDeleted = false

      while (!isDeleted) {
        try {
          isDeleted = workflow.getState() != null && workflow.getState().isDeleted
        } catch (e: Exception) {
          log.info("retrying...")
          Thread.sleep(100)
        }
      }
    }

    @JvmStatic
    fun getSetupFailingActivity(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(
          Thread {
            Mockito
              .`when`(
                mJobCreationAndStatusUpdateActivity.createNewJob(
                  any(),
                ),
              ).thenThrow(ApplicationFailure.newNonRetryableFailure("", ""))
          },
          0,
        ),
        Arguments.of(
          Thread {
            Mockito
              .`when`(
                mJobCreationAndStatusUpdateActivity.createNewAttemptNumber(
                  any(),
                ),
              ).thenThrow(ApplicationFailure.newNonRetryableFailure("", ""))
          },
          0,
        ),
        Arguments.of(
          Thread {
            Mockito
              .doThrow(ApplicationFailure.newNonRetryableFailure("", ""))
              .`when`(mJobCreationAndStatusUpdateActivity)
              .reportJobStart(any())
          },
          0,
        ),
      )

    // Since we can't directly unit test the failure path, we enumerate the core failure cases as a
    // proxy. This is deliberately incomplete as the permutations of failure cases is large.
    @JvmStatic
    fun coreFailureTypesMatrix(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(SourceAndDestinationFailureSyncWorkflow::class.java),
        Arguments.of(ReplicateFailureSyncWorkflow::class.java),
        Arguments.of(SyncWorkflowFailingOutputWorkflow::class.java),
      )

    @JvmStatic
    fun noBackoffSchedulingMatrix(): Stream<Arguments?> =
      Stream.of<Arguments?>(
        Arguments.of(true, Duration.ZERO),
        Arguments.of(false, Duration.ofHours(1)),
      )

    @JvmStatic
    private fun backoffJobFailureMatrix(): Stream<Arguments?> =
      Stream.of<Arguments?>(
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
