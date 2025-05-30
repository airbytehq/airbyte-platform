/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling;

import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import datadog.trace.api.Trace;
import io.airbyte.commons.constants.WorkerConstants;
import io.airbyte.commons.temporal.TemporalJobType;
import io.airbyte.commons.temporal.TemporalTaskQueueUtils;
import io.airbyte.commons.temporal.TemporalWorkflowUtils;
import io.airbyte.commons.temporal.annotations.TemporalActivityStub;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.commons.temporal.scheduling.CheckCommandApiInput;
import io.airbyte.commons.temporal.scheduling.CheckCommandInput;
import io.airbyte.commons.temporal.scheduling.CheckCommandInput.CheckConnectionInput;
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow;
import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput;
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow;
import io.airbyte.commons.temporal.scheduling.SyncWorkflow;
import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2;
import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2Input;
import io.airbyte.commons.temporal.scheduling.retries.RetryManager;
import io.airbyte.commons.temporal.scheduling.state.WorkflowInternalState;
import io.airbyte.commons.temporal.scheduling.state.WorkflowState;
import io.airbyte.commons.temporal.scheduling.state.listener.NoopStateListener;
import io.airbyte.config.ActorType;
import io.airbyte.config.ConnectionContext;
import io.airbyte.config.ConnectorJobOutput;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.StandardCheckConnectionInput;
import io.airbyte.config.StandardSyncInput;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StandardSyncSummary;
import io.airbyte.config.StandardSyncSummary.ReplicationStatus;
import io.airbyte.config.WorkloadPriority;
import io.airbyte.featureflag.UseCommandCheck;
import io.airbyte.featureflag.UseSyncV2;
import io.airbyte.metrics.MetricAttribute;
import io.airbyte.metrics.OssMetricsRegistry;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.persistence.job.models.IntegrationLauncherConfig;
import io.airbyte.persistence.job.models.JobRunConfig;
import io.airbyte.workers.helper.FailureHelper;
import io.airbyte.workers.helpers.ContextConversionHelper;
import io.airbyte.workers.models.JobInput;
import io.airbyte.workers.models.SyncJobCheckConnectionInputs;
import io.airbyte.workers.temporal.activities.GetConnectionContextInput;
import io.airbyte.workers.temporal.activities.GetLoadShedBackoffInput;
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity;
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity.LogInput;
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity.LogLevel;
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity.LogOutput;
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity;
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionActivityInput;
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionOutput;
import io.airbyte.workers.temporal.scheduling.activities.CheckRunProgressActivity;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.ScheduleRetrieverInput;
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.ScheduleRetrieverOutput;
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity;
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity.FeatureFlagFetchInput;
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity.FeatureFlagFetchOutput;
import io.airbyte.workers.temporal.scheduling.activities.GenerateInputActivity;
import io.airbyte.workers.temporal.scheduling.activities.GenerateInputActivity.SyncInputWithAttemptNumber;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptCreationInput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptNumberCreationOutput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptNumberFailureInput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.EnsureCleanJobStateInput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCancelledInputWithAttemptNumber;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCheckFailureInput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCreationInput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCreationOutput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobFailureInput;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobSuccessInputWithAttemptNumber;
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.ReportJobStartInput;
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity;
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity.FailureCause;
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity.RecordMetricInput;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.HydrateInput;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.HydrateOutput;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.PersistInput;
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.PersistOutput;
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivity;
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivity.DeleteStreamResetRecordsForJobInput;
import io.airbyte.workers.temporal.scheduling.activities.WorkflowConfigActivity;
import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.failure.ActivityFailure;
import io.temporal.failure.CanceledFailure;
import io.temporal.failure.ChildWorkflowFailure;
import io.temporal.workflow.CancellationScope;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Workflow;
import jakarta.annotation.Nullable;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConnectionManagerWorkflowImpl.
 */
@SuppressWarnings("PMD.AvoidDuplicateLiterals")
public class ConnectionManagerWorkflowImpl implements ConnectionManagerWorkflow {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String GENERATE_CHECK_INPUT_TAG = "generate_check_input";
  private static final int GENERATE_CHECK_INPUT_CURRENT_VERSION = 1;
  private static final String CHECK_WORKSPACE_TOMBSTONE_TAG = "check_workspace_tombstone";
  private static final int CHECK_WORKSPACE_TOMBSTONE_CURRENT_VERSION = 1;
  private static final String LOAD_SHED_BACK_OFF_TAG = "load_shed_back_off";
  private static final int LOAD_SHED_BACK_OFF_CURRENT_VERSION = 1;
  private static final String PASS_DEST_REQS_TO_CHECK_TAG = "pass_dest_reqs_to_check";
  private static final int PASS_DEST_REQS_TO_CHECK_CURRENT_VERSION = 1;

  private final WorkflowState workflowState = new WorkflowState(UUID.randomUUID(), new NoopStateListener());

  private final WorkflowInternalState workflowInternalState = new WorkflowInternalState();

  private static final String GET_FEATURE_FLAGS_TAG = "get_feature_flags";
  private static final int GET_FEATURE_FLAGS_CURRENT_VERSION = 1;

  private static final String CHECK_USING_COMMAND_API_TAG = "check_using_command_api";
  private static final int CHECK_USING_COMMAND_API_VERSION = 1;

  private static final String USE_SYNC_WORKFLOW_V2_TAG = "use_sync_workflow_v2";
  private static final int USE_SYNC_WORKFLOW_V2_VERSION = 1;

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private GenerateInputActivity getSyncInputActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private JobCreationAndStatusUpdateActivity jobCreationAndStatusUpdateActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private ConfigFetchActivity configFetchActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private AutoDisableConnectionActivity autoDisableConnectionActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private StreamResetActivity streamResetActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private RecordMetricActivity recordMetricActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private WorkflowConfigActivity workflowConfigActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private FeatureFlagFetchActivity featureFlagFetchActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private CheckRunProgressActivity checkRunProgressActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private RetryStatePersistenceActivity retryStatePersistenceActivity;
  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private AppendToAttemptLogActivity appendToAttemptLogActivity;

  private CancellationScope cancellableSyncWorkflow;

  private UUID connectionId;

  private Duration workflowDelay;

  private RetryManager retryManager;

  private ConnectionContext connectionContext;

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public void run(final ConnectionUpdaterInput connectionUpdaterInput) throws RetryableException {
    try {

      if (isTombstone(connectionUpdaterInput.getConnectionId())) {
        return;
      }

      /*
       * Hydrate the connection context (workspace, org, source, dest, etc. ids) as soon as possible.
       */
      final var hydratedContext = runMandatoryActivityWithOutput(configFetchActivity::getConnectionContext,
          new GetConnectionContextInput(connectionUpdaterInput.getConnectionId()));
      setConnectionContext(hydratedContext.getConnectionContext());

      /*
       * Sleep and periodically check in a loop until we're no longer load shed.
       */
      backoffIfLoadShedEnabled(hydratedContext.getConnectionContext());

      /*
       * Always ensure that the connection ID is set from the input before performing any additional work.
       * Failure to set the connection ID before performing any work in this workflow could result in
       * additional failures when attempting to handle a failed workflow AND/OR the inability to identify
       * impacted connections when errors do occur.
       */
      setConnectionId(connectionUpdaterInput);

      // Copy over data from the input to workflowState early to minimize gaps with signals
      initializeWorkflowStateFromInput(connectionUpdaterInput);

      // Fetch workflow delay first so that it is set if any subsequent activities fail and need to be
      // re-attempted.
      workflowDelay = getWorkflowRestartDelaySeconds();

      recordMetric(new RecordMetricInput(connectionUpdaterInput, Optional.empty(), OssMetricsRegistry.TEMPORAL_WORKFLOW_ATTEMPT, null));

      try {
        cancellableSyncWorkflow = generateSyncWorkflowRunnable(connectionUpdaterInput);
        cancellableSyncWorkflow.run();
      } catch (final CanceledFailure cf) {
        // When a scope is cancelled temporal will throw a CanceledFailure as you can see here:
        // https://github.com/temporalio/sdk-java/blob/master/temporal-sdk/src/main/java/io/temporal/workflow/CancellationScope.java#L72
        // The naming is very misleading, it is not a failure but the expected behavior...
        recordMetric(
            new RecordMetricInput(connectionUpdaterInput, Optional.of(FailureCause.CANCELED), OssMetricsRegistry.TEMPORAL_WORKFLOW_FAILURE, null));
      }

      if (workflowState.isDeleted()) {
        if (workflowState.isRunning()) {
          log.info(
              "Cancelling jobId '{}' because connection '{}' was deleted",
              Objects.toString(connectionUpdaterInput.getJobId(), "null"),
              connectionUpdaterInput.getConnectionId());
          // This call is not needed anymore since this will be cancel using the cancellation state
          reportCancelled(connectionId);
        }

        return;
      }

      // this means that the current workflow is being cancelled so that a reset can be run instead.
      if (workflowState.isCancelledForReset()) {
        reportCancelledAndContinueWith(true, connectionUpdaterInput);
      }

      // "Cancel" button was pressed on a job
      if (workflowState.isCancelled()) {
        reportCancelledAndContinueWith(false, connectionUpdaterInput);
      }

    } catch (final Exception e) {
      log.error("The connection update workflow has failed, will create a new attempt.", e);
      reportFailure(connectionUpdaterInput, null, FailureCause.UNKNOWN);
      prepareForNextRunAndContinueAsNew(connectionUpdaterInput);

      // Add the exception to the span, as it represents a platform failure
      ApmTraceUtils.addExceptionToTrace(e);
    }
  }

  private boolean isTombstone(UUID connectionId) {
    final int checkTombstoneVersion =
        Workflow.getVersion(CHECK_WORKSPACE_TOMBSTONE_TAG, Workflow.DEFAULT_VERSION, CHECK_WORKSPACE_TOMBSTONE_CURRENT_VERSION);
    if (checkTombstoneVersion == Workflow.DEFAULT_VERSION || connectionId == null) {
      return false;
    }

    return configFetchActivity.isWorkspaceTombstone(connectionId);
  }

  private void backoffIfLoadShedEnabled(final ConnectionContext connectionContext) {
    final int version =
        Workflow.getVersion(LOAD_SHED_BACK_OFF_TAG, Workflow.DEFAULT_VERSION, LOAD_SHED_BACK_OFF_CURRENT_VERSION);
    if (version == Workflow.DEFAULT_VERSION || connectionContext == null) {
      return;
    }

    final var scheduleRetrieverInput = new GetLoadShedBackoffInput(connectionContext);
    var backoff = configFetchActivity.getLoadShedBackoff(scheduleRetrieverInput);
    while (backoff.getDuration().isPositive()) {
      Workflow.sleep(backoff.getDuration());
      backoff = configFetchActivity.getLoadShedBackoff(scheduleRetrieverInput);
    }
  }

  @SuppressWarnings("PMD.UnusedLocalVariable")
  private CancellationScope generateSyncWorkflowRunnable(final ConnectionUpdaterInput connectionUpdaterInput) {
    return Workflow.newCancellationScope(() -> {
      if (connectionUpdaterInput.getSkipScheduling()) {
        workflowState.setSkipScheduling(true);
      }

      // Clean the job state by failing any jobs for this connection that are currently non-terminal.
      // This catches cases where the temporal workflow was terminated and restarted while a job was
      // actively running, leaving that job in an orphaned and non-terminal state.
      ensureCleanJobState(connectionUpdaterInput);

      // setup retry manager before scheduling to resolve schedule with backoff
      retryManager = hydrateRetryManager();
      if (retryManager != null) {
        runAppendToAttemptLogActivity(String.format("Retry State: %s", retryManager), LogLevel.INFO);
      }

      final Duration timeTilScheduledRun = getTimeTilScheduledRun(connectionUpdaterInput.getConnectionId());

      final Duration timeToWait;
      if (connectionUpdaterInput.getFromFailure()) {
        // note this can fail the job if the backoff is longer than scheduled time to wait
        timeToWait = resolveBackoff();
      } else {
        timeToWait = timeTilScheduledRun;
      }

      if (!timeToWait.isZero()) {
        Workflow.await(timeToWait, this::shouldInterruptWaiting);
      }

      workflowState.setDoneWaiting(true);

      if (workflowState.isDeleted()) {
        log.info("Returning from workflow cancellation scope because workflow deletion was requested.");
        return;
      }

      if (workflowState.isUpdated()) {
        // Act as a return
        prepareForNextRunAndContinueAsNew(connectionUpdaterInput);
      }

      if (workflowState.isCancelled()) {
        reportCancelledAndContinueWith(false, connectionUpdaterInput);
      }

      // re-hydrate retry manager on run-start because FFs may have changed
      retryManager = hydrateRetryManager();

      // This var is unused since not feature flags are currently required in this workflow
      // We keep the activity around to get any feature flags that might be needed in the future
      final Map<String, Boolean> featureFlags = getFeatureFlags(connectionUpdaterInput.getConnectionId());

      workflowInternalState.setJobId(getOrCreateJobId(connectionUpdaterInput));
      workflowInternalState.setAttemptNumber(createAttempt(workflowInternalState.getJobId()));

      JobInput jobInputs = null;
      final boolean shouldRunCheckInputGeneration = shouldRunCheckInputGeneration();
      if (!shouldRunCheckInputGeneration) {
        jobInputs = getJobInput();
      }

      reportJobStarting(connectionUpdaterInput.getConnectionId());
      StandardSyncOutput standardSyncOutput = null;

      try {
        final Boolean shouldRunWithCheckCommandFFValue = featureFlags.get(UseCommandCheck.INSTANCE.getKey());
        final boolean shouldRunWithCheckCommand = shouldRunWithCheckCommandFFValue == null ? false : shouldRunWithCheckCommandFFValue;
        final boolean canUseCheckWithCommandApi = Workflow.getVersion(CHECK_USING_COMMAND_API_TAG, Workflow.DEFAULT_VERSION,
            CHECK_USING_COMMAND_API_VERSION) > Workflow.DEFAULT_VERSION;

        final SyncCheckConnectionResult syncCheckConnectionResult = shouldRunWithCheckCommand && canUseCheckWithCommandApi
            ? checkConnectionsWithCommandApi(
                connectionContext.getSourceId(),
                connectionContext.getDestinationId(),
                workflowInternalState.getJobId(),
                workflowInternalState.getAttemptNumber().longValue(),
                connectionUpdaterInput.getResetConnection())
            : checkConnections(getJobRunConfig(),
                jobInputs);
        if (syncCheckConnectionResult.isFailed()) {
          final StandardSyncOutput checkFailureOutput = syncCheckConnectionResult.buildFailureOutput();
          workflowState.setFailed(getFailStatus(checkFailureOutput));
          reportFailure(connectionUpdaterInput, checkFailureOutput, FailureCause.CONNECTION);
        } else {
          if (shouldRunCheckInputGeneration) {
            jobInputs = getJobInput();
          }

          final boolean useSyncWorkflowV2 = featureFlags.getOrDefault(UseSyncV2.INSTANCE.getKey(), false);
          final boolean canSyncWorkflowV2 = Workflow.getVersion(USE_SYNC_WORKFLOW_V2_TAG, Workflow.DEFAULT_VERSION,
              USE_SYNC_WORKFLOW_V2_VERSION) > Workflow.DEFAULT_VERSION;
          standardSyncOutput = canSyncWorkflowV2 && useSyncWorkflowV2 ? runChildWorkflowV2(
              connectionId,
              workflowInternalState.getJobId(),
              workflowInternalState.getAttemptNumber(),
              connectionContext.getSourceId())
              : runChildWorkflow(jobInputs);
          workflowState.setFailed(getFailStatus(standardSyncOutput));
          workflowState.setCancelled(getCancelledStatus(standardSyncOutput));

          if (workflowState.isFailed()) {
            reportFailure(connectionUpdaterInput, standardSyncOutput, FailureCause.UNKNOWN);
          } else if (workflowState.isCancelled()) {
            reportCancelledAndContinueWith(false, connectionUpdaterInput);
          } else {
            reportSuccess(connectionUpdaterInput, standardSyncOutput);
          }
        }

        prepareForNextRunAndContinueAsNew(connectionUpdaterInput);
      } catch (final ChildWorkflowFailure childWorkflowFailure) {
        // when we cancel a method, we call the cancel method of the cancellation scope. This will throw an
        // exception since we expect it, we just
        // silently ignore it.
        if (childWorkflowFailure.getCause() instanceof CanceledFailure) {
          log.debug("Ignoring canceled failure as it is handled by the cancellation scope.");
          // do nothing, cancellation handled by cancellationScope
        } else if (childWorkflowFailure.getCause()instanceof final ActivityFailure af) {
          // Allows us to classify unhandled failures from the sync workflow.
          workflowInternalState.getFailures().add(FailureHelper.failureReasonFromWorkflowAndActivity(
              childWorkflowFailure.getWorkflowType(),
              af.getCause(),
              workflowInternalState.getJobId(),
              workflowInternalState.getAttemptNumber()));
          ApmTraceUtils.addExceptionToTrace(af.getCause());
          reportFailure(connectionUpdaterInput, standardSyncOutput, FailureCause.ACTIVITY);
          prepareForNextRunAndContinueAsNew(connectionUpdaterInput);
        } else {
          workflowInternalState.getFailures().add(
              FailureHelper.unknownOriginFailure(childWorkflowFailure.getCause(), workflowInternalState.getJobId(),
                  workflowInternalState.getAttemptNumber()));
          ApmTraceUtils.addExceptionToTrace(childWorkflowFailure.getCause());
          reportFailure(connectionUpdaterInput, standardSyncOutput, FailureCause.WORKFLOW);
          prepareForNextRunAndContinueAsNew(connectionUpdaterInput);
        }
      }
    });
  }

  private void reportSuccess(final ConnectionUpdaterInput connectionUpdaterInput, final StandardSyncOutput standardSyncOutput) {
    workflowState.setSuccess(true);

    runMandatoryActivity(jobCreationAndStatusUpdateActivity::jobSuccessWithAttemptNumber, new JobSuccessInputWithAttemptNumber(
        workflowInternalState.getJobId(),
        workflowInternalState.getAttemptNumber(),
        connectionUpdaterInput.getConnectionId(),
        standardSyncOutput));

    deleteResetJobStreams();

    // Record the success metric
    recordMetric(new RecordMetricInput(connectionUpdaterInput, Optional.empty(), OssMetricsRegistry.TEMPORAL_WORKFLOW_SUCCESS, null));

    resetNewConnectionInput(connectionUpdaterInput);
  }

  private void reportFailure(final ConnectionUpdaterInput connectionUpdaterInput,
                             final StandardSyncOutput standardSyncOutput,
                             final FailureCause failureCause) {
    reportFailure(connectionUpdaterInput, standardSyncOutput, failureCause, new HashSet<>());
  }

  private void reportFailure(final ConnectionUpdaterInput connectionUpdaterInput,
                             final StandardSyncOutput standardSyncOutput,
                             final FailureCause failureCause,
                             final Set<FailureReason> failureReasonsOverride) {

    final Set<FailureReason> failureReasons = failureReasonsOverride.isEmpty() ? workflowInternalState.getFailures() : failureReasonsOverride;

    runMandatoryActivity(jobCreationAndStatusUpdateActivity::attemptFailureWithAttemptNumber, new AttemptNumberFailureInput(
        workflowInternalState.getJobId(),
        workflowInternalState.getAttemptNumber(),
        connectionUpdaterInput.getConnectionId(),
        standardSyncOutput,
        FailureHelper.failureSummary(failureReasons, workflowInternalState.getPartialSuccess())));

    // ATTENTION: connectionUpdaterInput.getAttemptNumber() is 1-based (usually)
    // this differs from workflowInternalState.getAttemptNumber() being 0-based.
    // TODO: Don't mix these bases. Bug filed https://github.com/airbytehq/airbyte/issues/27808
    final int attemptNumber = connectionUpdaterInput.getAttemptNumber();
    ApmTraceUtils.addTagsToTrace(Map.of(ATTEMPT_NUMBER_KEY, attemptNumber));

    // This is outside the retry if/else block because we will pass it to our retry manager regardless
    // of retry state.
    // This will be added in a future PR as we develop this feature.
    final boolean madeProgress = checkRunProgress();
    accumulateFailureAndPersist(madeProgress);

    final FailureType failureType =
        standardSyncOutput != null ? standardSyncOutput.getFailures().isEmpty() ? null : standardSyncOutput.getFailures().getFirst().getFailureType()
            : null;
    if (isWithinRetryLimit(attemptNumber) && failureType != FailureType.CONFIG_ERROR) {
      // restart from failure
      connectionUpdaterInput.setAttemptNumber(attemptNumber + 1);
      connectionUpdaterInput.setFromFailure(true);

      if (madeProgress) {
        recordProgressMetric(connectionUpdaterInput, failureCause, true);
      }

    } else {
      final String internalFailureMessage =
          failureReasons.stream().findFirst().map(FailureReason::getInternalMessage).orElse("Unknown failure reason");
      final String failureReason = failureType == FailureType.CONFIG_ERROR ? internalFailureMessage
          : "Job failed after too many retries for connection " + connectionId;
      failJob(connectionUpdaterInput, failureReason);

      final var attrs = new MetricAttribute[] {
        new MetricAttribute(MetricTags.MADE_PROGRESS, String.valueOf(madeProgress))
      };
      // Record the failure metric
      recordMetric(new RecordMetricInput(connectionUpdaterInput, Optional.of(failureCause), OssMetricsRegistry.TEMPORAL_WORKFLOW_FAILURE, attrs));
      // Record whether we made progress
      if (madeProgress) {
        recordProgressMetric(connectionUpdaterInput, failureCause, false);
      }

      resetNewConnectionInput(connectionUpdaterInput);
    }
  }

  private void failJob(final ConnectionUpdaterInput input, final String failureReason) {
    runAppendToAttemptLogActivity(String.format("Failing job: %d, reason: %s", input.getJobId(), failureReason), LogLevel.ERROR);

    runMandatoryActivity(
        jobCreationAndStatusUpdateActivity::jobFailure,
        new JobFailureInput(
            input.getJobId(),
            input.getAttemptNumber(),
            input.getConnectionId(),
            failureReason));

    final AutoDisableConnectionActivityInput autoDisableConnectionActivityInput = new AutoDisableConnectionActivityInput();
    autoDisableConnectionActivityInput.setConnectionId(connectionId);
    final AutoDisableConnectionOutput output = runMandatoryActivityWithOutput(
        autoDisableConnectionActivity::autoDisableFailingConnection,
        autoDisableConnectionActivityInput);
    if (output.isDisabled()) {
      log.info("Auto-disabled for constantly failing for Connection {}", connectionId);
    }
  }

  private void recordProgressMetric(final ConnectionUpdaterInput input, final FailureCause cause, final boolean willRetry) {
    // job id and other attrs get populated by the wrapping activity
    final var attrs = new MetricAttribute[] {
      new MetricAttribute(MetricTags.WILL_RETRY, String.valueOf(willRetry)),
      new MetricAttribute(MetricTags.ATTEMPT_NUMBER, String.valueOf(workflowInternalState.getAttemptNumber()))
    };

    tryRecordCountMetric(
        new RecordMetricInput(
            input,
            Optional.ofNullable(cause),
            OssMetricsRegistry.REPLICATION_MADE_PROGRESS,
            attrs));
  }

  private boolean isWithinRetryLimit(final int attemptNumber) {
    if (useAttemptCountRetries()) {
      final int maxAttempt = configFetchActivity.getMaxAttempt().maxAttempt();

      return maxAttempt > attemptNumber;
    }

    return retryManager.shouldRetry();
  }

  private boolean checkRunProgress() {

    // we don't use `runMandatoryActivity` to prevent an infinite loop (reportFailure ->
    // runMandatoryActivity -> reportFailure...)
    final var result = runActivityWithFallback(
        checkRunProgressActivity::checkProgress,
        new CheckRunProgressActivity.Input(
            workflowInternalState.getJobId(),
            workflowInternalState.getAttemptNumber(),
            connectionId),
        new CheckRunProgressActivity.Output(false),
        CheckRunProgressActivity.class.getName(),
        "checkProgress");

    return result.madeProgress();
  }

  /**
   * Returns whether the new check input generation activity should be called, depending on the
   * presence of workflow versioning. This should be removed once the new activity is fully rolled
   * out.
   */
  private boolean shouldRunCheckInputGeneration() {
    final int generateCheckInputVersion =
        Workflow.getVersion(GENERATE_CHECK_INPUT_TAG, Workflow.DEFAULT_VERSION, GENERATE_CHECK_INPUT_CURRENT_VERSION);
    return generateCheckInputVersion >= GENERATE_CHECK_INPUT_CURRENT_VERSION;
  }

  private SyncJobCheckConnectionInputs getCheckConnectionInputFromSync(final JobInput jobInputs) {
    final StandardSyncInput syncInput = jobInputs.getSyncInput();
    final JsonNode sourceConfig = syncInput.getSourceConfiguration();
    final JsonNode destinationConfig = syncInput.getDestinationConfiguration();
    final IntegrationLauncherConfig sourceLauncherConfig = jobInputs.getSourceLauncherConfig();
    final IntegrationLauncherConfig destinationLauncherConfig = jobInputs.getDestinationLauncherConfig();

    final StandardCheckConnectionInput standardCheckInputSource = new StandardCheckConnectionInput()
        .withActorType(ActorType.SOURCE)
        .withActorId(syncInput.getSourceId())
        .withConnectionConfiguration(sourceConfig)
        .withActorContext(ContextConversionHelper.connectionContextToSourceContext(syncInput.getConnectionContext()));

    final StandardCheckConnectionInput standardCheckInputDestination = new StandardCheckConnectionInput()
        .withActorType(ActorType.DESTINATION)
        .withActorId(syncInput.getDestinationId())
        .withConnectionConfiguration(destinationConfig)
        .withActorContext(ContextConversionHelper.connectionContextToDestinationContext(syncInput.getConnectionContext()));

    return new SyncJobCheckConnectionInputs(
        sourceLauncherConfig,
        destinationLauncherConfig,
        standardCheckInputSource,
        standardCheckInputDestination);
  }

  private SyncCheckConnectionResult checkConnections(final JobRunConfig jobRunConfig,
                                                     @Nullable final JobInput jobInputs) {
    final SyncCheckConnectionResult checkConnectionResult = new SyncCheckConnectionResult(jobRunConfig);

    final JobCheckFailureInput jobStateInput =
        new JobCheckFailureInput(Long.parseLong(jobRunConfig.getJobId()), jobRunConfig.getAttemptId().intValue(), connectionId);
    final boolean isLastJobOrAttemptFailure =
        runMandatoryActivityWithOutput(jobCreationAndStatusUpdateActivity::isLastJobOrAttemptFailure, jobStateInput);

    if (!isLastJobOrAttemptFailure) {
      log.info("SOURCE CHECK: Skipped, last attempt was not a failure");
      log.info("DESTINATION CHECK: Skipped, last attempt was not a failure");
      return checkConnectionResult;
    }

    final SyncJobCheckConnectionInputs checkInputs;
    if (!shouldRunCheckInputGeneration() && jobInputs != null) {
      checkInputs = getCheckConnectionInputFromSync(jobInputs);
    } else {
      checkInputs = getCheckConnectionInput();
    }

    final IntegrationLauncherConfig sourceLauncherConfig = checkInputs.getSourceLauncherConfig()
        .withPriority(WorkloadPriority.DEFAULT);

    if (isResetJob(sourceLauncherConfig) || checkConnectionResult.isFailed()) {
      // reset jobs don't need to connect to any external source, so check connection is unnecessary
      log.info("SOURCE CHECK: Skipped, reset job");
    } else {
      log.info("SOURCE CHECK: Starting");
      final ConnectorJobOutput sourceCheckResponse;
      sourceCheckResponse = runCheckInChildWorkflow(jobRunConfig, sourceLauncherConfig, new StandardCheckConnectionInput()
          .withActorType(ActorType.SOURCE)
          .withActorId(checkInputs.getSourceCheckConnectionInput().getActorId())
          .withConnectionConfiguration(checkInputs.getSourceCheckConnectionInput().getConnectionConfiguration())
          .withResourceRequirements(checkInputs.getSourceCheckConnectionInput().getResourceRequirements())
          .withActorContext(ContextConversionHelper.buildSourceContextFrom(jobInputs, checkInputs)));

      if (SyncCheckConnectionResult.isOutputFailed(sourceCheckResponse)) {
        checkConnectionResult.setFailureOrigin(FailureReason.FailureOrigin.SOURCE);
        checkConnectionResult.setFailureOutput(sourceCheckResponse);
        log.info("SOURCE CHECK: Failed");
      } else {
        log.info("SOURCE CHECK: Successful");
      }
    }

    if (checkConnectionResult.isFailed()) {
      log.info("DESTINATION CHECK: Skipped, source check failed");
    } else {
      IntegrationLauncherConfig launcherConfig = checkInputs.getDestinationLauncherConfig()
          .withPriority(WorkloadPriority.DEFAULT);
      log.info("DESTINATION CHECK: Starting");
      final var checkDestInput =
          new StandardCheckConnectionInput()
              .withActorType(ActorType.DESTINATION)
              .withActorId(checkInputs.getDestinationCheckConnectionInput().getActorId())
              .withConnectionConfiguration(checkInputs.getDestinationCheckConnectionInput().getConnectionConfiguration())
              .withActorContext(ContextConversionHelper.buildDestinationContextFrom(jobInputs, checkInputs));

      final boolean shouldPassReqs =
          Workflow.getVersion(PASS_DEST_REQS_TO_CHECK_TAG, Workflow.DEFAULT_VERSION,
              PASS_DEST_REQS_TO_CHECK_CURRENT_VERSION) >= PASS_DEST_REQS_TO_CHECK_CURRENT_VERSION;
      if (shouldPassReqs) {
        checkDestInput.setResourceRequirements(checkInputs.getDestinationCheckConnectionInput().getResourceRequirements());
      }

      final ConnectorJobOutput destinationCheckResponse = runCheckInChildWorkflow(jobRunConfig,
          launcherConfig,
          checkDestInput);
      if (SyncCheckConnectionResult.isOutputFailed(destinationCheckResponse)) {
        checkConnectionResult.setFailureOrigin(FailureReason.FailureOrigin.DESTINATION);
        checkConnectionResult.setFailureOutput(destinationCheckResponse);
        log.info("DESTINATION CHECK: Failed");
      } else {
        log.info("DESTINATION CHECK: Successful");
      }
    }

    return checkConnectionResult;
  }

  private SyncCheckConnectionResult checkConnectionsWithCommandApi(final UUID sourceActorId,
                                                                   final UUID destinationActorId,
                                                                   final Long jobId,
                                                                   final Long attemptId,
                                                                   final boolean isReset) {
    final SyncCheckConnectionResult checkConnectionResult = new SyncCheckConnectionResult(jobId, attemptId.intValue());

    final JobCheckFailureInput jobStateInput =
        new JobCheckFailureInput(jobId, attemptId.intValue(), connectionId);
    final boolean isLastJobOrAttemptFailure =
        runMandatoryActivityWithOutput(jobCreationAndStatusUpdateActivity::isLastJobOrAttemptFailure, jobStateInput);

    if (!isLastJobOrAttemptFailure) {
      log.info("SOURCE CHECK: Skipped, last attempt was not a failure");
      log.info("DESTINATION CHECK: Skipped, last attempt was not a failure");
      return checkConnectionResult;
    }

    if (isReset) {
      // reset jobs don't need to connect to any external source, so check connection is unnecessary
      log.info("SOURCE CHECK: Skipped, reset job");
    } else {
      log.info("SOURCE CHECK: Starting");
      final ConnectorJobOutput sourceCheckResponse;
      sourceCheckResponse = runCheckWithCommandApiInChildWorkflow(sourceActorId, jobId.toString(), attemptId, ActorType.SOURCE.value());

      if (SyncCheckConnectionResult.isOutputFailed(sourceCheckResponse)) {
        checkConnectionResult.setFailureOrigin(FailureReason.FailureOrigin.SOURCE);
        checkConnectionResult.setFailureOutput(sourceCheckResponse);
        log.info("SOURCE CHECK: Failed");
      } else {
        log.info("SOURCE CHECK: Successful");
      }
    }

    if (checkConnectionResult.isFailed()) {
      log.info("DESTINATION CHECK: Skipped, source check failed");
    } else {
      final ConnectorJobOutput destinationCheckResponse = runCheckWithCommandApiInChildWorkflow(destinationActorId,
          jobId.toString(),
          attemptId,
          ActorType.DESTINATION.value());
      if (SyncCheckConnectionResult.isOutputFailed(destinationCheckResponse)) {
        checkConnectionResult.setFailureOrigin(FailureReason.FailureOrigin.DESTINATION);
        checkConnectionResult.setFailureOutput(destinationCheckResponse);
        log.info("DESTINATION CHECK: Failed");
      } else {
        log.info("DESTINATION CHECK: Successful");
      }
    }

    return checkConnectionResult;
  }

  private boolean isResetJob(final IntegrationLauncherConfig sourceLauncherConfig) {
    return WorkerConstants.RESET_JOB_SOURCE_DOCKER_IMAGE_STUB.equals(sourceLauncherConfig.getDockerImage());
  }

  // reset the ConnectionUpdaterInput back to a default state
  private void resetNewConnectionInput(final ConnectionUpdaterInput connectionUpdaterInput) {
    connectionUpdaterInput.setJobId(null);
    connectionUpdaterInput.setAttemptNumber(1);
    connectionUpdaterInput.setFromFailure(false);
    connectionUpdaterInput.setSkipScheduling(false);
  }

  @Override
  public void submitManualSync() {
    if (workflowState.isRunning()) {
      log.info("Can't schedule a manual workflow if a sync is running for connection {}", connectionId);
      return;
    }

    workflowState.setSkipScheduling(true);
  }

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public void cancelJob() {
    traceConnectionId();
    if (!workflowState.isRunning()) {
      log.info("Can't cancel a non-running sync for connection {}", connectionId);
      return;
    }
    workflowState.setCancelled(true);
    cancelSyncChildWorkflow();
  }

  // TODO: Delete when the don't delete in temporal is removed
  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public void deleteConnection() {
    traceConnectionId();
    workflowState.setDeleted(true);
    log.info("Set as deleted and canceling job for connection {}", connectionId);
    cancelJob();
  }

  @Override
  public void connectionUpdated() {
    workflowState.setUpdated(true);
  }

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public void resetConnection() {
    traceConnectionId();

    // Assumes that the streams_reset has already been populated with streams to reset for this
    // connection
    if (workflowState.isDoneWaiting()) {
      workflowState.setCancelledForReset(true);
      cancelSyncChildWorkflow();
    } else {
      workflowState.setSkipScheduling(true);
    }
  }

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Override
  public void resetConnectionAndSkipNextScheduling() {
    traceConnectionId();

    if (workflowState.isDoneWaiting()) {
      workflowState.setCancelledForReset(true);
      workflowState.setSkipSchedulingNextWorkflow(true);
      cancelSyncChildWorkflow();
    } else {
      workflowState.setSkipScheduling(true);
      workflowState.setSkipSchedulingNextWorkflow(true);
    }
  }

  @Override
  public WorkflowState getState() {
    return workflowState;
  }

  @Override
  public JobInformation getJobInformation() {
    final long jobId = workflowInternalState.getJobId() != null ? workflowInternalState.getJobId() : NON_RUNNING_JOB_ID;
    final Integer attemptNumber = workflowInternalState.getAttemptNumber();
    return new JobInformation(
        jobId,
        attemptNumber == null ? NON_RUNNING_ATTEMPT_ID : attemptNumber);
  }

  /**
   * return true if the workflow is in a state that require it to continue. If the state is to process
   * an update or delete the workflow, it won't continue with a run of the {@link SyncWorkflow} but it
   * will: - restart for an update - Update the connection status and terminate the workflow for a
   * delete
   */
  private Boolean shouldInterruptWaiting() {
    return workflowState.isSkipScheduling() || workflowState.isDeleted() || workflowState.isUpdated() || workflowState.isCancelled();
  }

  private void prepareForNextRunAndContinueAsNew(final ConnectionUpdaterInput connectionUpdaterInput) {
    // Continue the workflow as new
    workflowInternalState.getFailures().clear();
    workflowInternalState.setPartialSuccess(null);
    final boolean isDeleted = workflowState.isDeleted();
    if (workflowState.isSkipSchedulingNextWorkflow()) {
      connectionUpdaterInput.setSkipScheduling(true);
    }
    workflowState.reset();
    if (!isDeleted) {
      Workflow.continueAsNew(connectionUpdaterInput);
    }
  }

  /**
   * This is running a lambda function that takes {@param input} as an input. If the run of the lambda
   * throws an exception, the workflow will retried after a short delay.
   * <p>
   * Note that if the lambda activity is configured to have retries, the exception will only be caught
   * after the activity has been retried the maximum number of times.
   * <p>
   * This method is meant to be used for calling temporal activities.
   */
  private <INPUT, OUTPUT> OUTPUT runMandatoryActivityWithOutput(final Function<INPUT, OUTPUT> mapper, final INPUT input) {
    try {
      return mapper.apply(input);
    } catch (final Exception e) {
      final Duration sleepDuration = getWorkflowDelay();
      log.error(
          "[ACTIVITY-FAILURE] Connection {} failed to run an activity.({}).  Connection manager workflow will be restarted after a delay of {}.",
          connectionId, input.getClass().getSimpleName(), sleepDuration, e);
      // TODO (https://github.com/airbytehq/airbyte/issues/13773) add tracking/notification

      // Wait a short delay before restarting workflow. This is important if, for example, the failing
      // activity was configured to not have retries.
      // Without this delay, that activity could cause the workflow to loop extremely quickly,
      // overwhelming temporal.
      log.info("Waiting {} before restarting the workflow for connection {}, to prevent spamming temporal with restarts.", sleepDuration,
          connectionId);
      Workflow.sleep(sleepDuration);

      // Add the exception to the span, as it represents a platform failure
      ApmTraceUtils.addExceptionToTrace(e);

      // If a jobId exist set the failure reason
      if (workflowInternalState.getJobId() != null && workflowInternalState.getAttemptNumber() != null) {
        final ConnectionUpdaterInput connectionUpdaterInput = connectionUpdaterInputFromState();
        final FailureReason failureReason =
            FailureHelper.platformFailure(e, workflowInternalState.getJobId(), workflowInternalState.getAttemptNumber());
        reportFailure(connectionUpdaterInput, null, FailureCause.ACTIVITY, Set.of(failureReason));
      } else {
        log.warn("Can't properly fail the job, the next run will clean the state in the EnsureCleanJobStateActivity");
      }

      log.info("Finished wait for connection {}, restarting connection manager workflow", connectionId);

      final ConnectionUpdaterInput newWorkflowInput = TemporalWorkflowUtils.buildStartWorkflowInput(connectionId);

      Workflow.continueAsNew(newWorkflowInput);

      throw new IllegalStateException("This statement should never be reached, as the ConnectionManagerWorkflow for connection "
          + connectionId + " was continued as new.", e);
    }
  }

  private ConnectionUpdaterInput connectionUpdaterInputFromState() {
    return new ConnectionUpdaterInput(
        connectionId,
        workflowInternalState.getJobId(),
        null,
        false,
        workflowInternalState.getAttemptNumber(),
        null,
        false,
        false,
        false);
  }

  /**
   * Similar to runMandatoryActivityWithOutput but for methods that don't return.
   */
  private <INPUT> void runMandatoryActivity(final Consumer<INPUT> consumer, final INPUT input) {
    runMandatoryActivityWithOutput((inputInternal) -> {
      consumer.accept(inputInternal);
      return null;
    }, input);
  }

  /**
   * Calculate the duration to wait so the workflow adheres to its schedule. This lets us 'schedule'
   * the next run.
   * <p>
   * This is calculated by {@link ConfigFetchActivity#getTimeToWait(ScheduleRetrieverInput)} and
   * depends on the last successful run and the schedule.
   * <p>
   * Wait time is infinite If the workflow is manual or disabled since we never want to schedule this.
   */
  private Duration getTimeTilScheduledRun(final UUID connectionId) {
    // Scheduling
    final ScheduleRetrieverInput scheduleRetrieverInput = new ScheduleRetrieverInput(connectionId);

    final ScheduleRetrieverOutput scheduleRetrieverOutput = runMandatoryActivityWithOutput(configFetchActivity::getTimeToWait,
        scheduleRetrieverInput);

    return scheduleRetrieverOutput.getTimeToWait();
  }

  private void ensureCleanJobState(final ConnectionUpdaterInput connectionUpdaterInput) {
    if (connectionUpdaterInput.getJobId() != null) {
      log.info("This workflow is already attached to a job, so no need to clean job state.");
      return;
    }

    runMandatoryActivity(jobCreationAndStatusUpdateActivity::ensureCleanJobState, new EnsureCleanJobStateInput(connectionId));
  }

  private void recordMetric(final RecordMetricInput recordMetricInput) {
    runMandatoryActivity(recordMetricActivity::recordWorkflowCountMetric, recordMetricInput);
  }

  /**
   * Unlike `recordMetric` above, we won't fail the attempt if this metric recording fails.
   */
  private void tryRecordCountMetric(final RecordMetricInput recordMetricInput) {
    try {
      recordMetricActivity.recordWorkflowCountMetric(recordMetricInput);
    } catch (final Exception e) {
      logActivityFailure(recordMetricActivity.getClass().getName(), "recordWorkflowCountMetric");
    }
  }

  /**
   * Creates a new job if it is not present in the input. If the jobId is specified in the input of
   * the connectionManagerWorkflow, we will return it. Otherwise we will create a job and return its
   * id.
   */
  private Long getOrCreateJobId(final ConnectionUpdaterInput connectionUpdaterInput) {
    if (connectionUpdaterInput.getJobId() != null) {
      return connectionUpdaterInput.getJobId();
    }

    final JobCreationOutput jobCreationOutput =
        runMandatoryActivityWithOutput(
            jobCreationAndStatusUpdateActivity::createNewJob,
            new JobCreationInput(connectionUpdaterInput.getConnectionId(), !workflowState.isSkipScheduling()));
    connectionUpdaterInput.setJobId(jobCreationOutput.getJobId());

    return jobCreationOutput.getJobId();
  }

  private Map<String, Boolean> getFeatureFlags(final UUID connectionId) {
    final int getFeatureFlagsVersion =
        Workflow.getVersion(GET_FEATURE_FLAGS_TAG, Workflow.DEFAULT_VERSION, GET_FEATURE_FLAGS_CURRENT_VERSION);

    if (getFeatureFlagsVersion < GET_FEATURE_FLAGS_CURRENT_VERSION) {
      return Map.of();
    }

    final FeatureFlagFetchOutput getFlagsOutput =
        runMandatoryActivityWithOutput(featureFlagFetchActivity::getFeatureFlags, new FeatureFlagFetchInput(connectionId));
    return getFlagsOutput.getFeatureFlags();
  }

  /**
   * Create a new attempt for a given jobId.
   *
   * @param jobId - the jobId associated with the new attempt
   *
   * @return The attempt number
   */
  private Integer createAttempt(final long jobId) {
    final AttemptNumberCreationOutput attemptNumberCreationOutput =
        runMandatoryActivityWithOutput(
            jobCreationAndStatusUpdateActivity::createNewAttemptNumber,
            new AttemptCreationInput(
                jobId));
    return attemptNumberCreationOutput.getAttemptNumber();
  }

  private JobRunConfig getJobRunConfig() {
    final Long jobId = workflowInternalState.getJobId();
    final Integer attemptNumber = workflowInternalState.getAttemptNumber();
    return TemporalWorkflowUtils.createJobRunConfig(jobId, attemptNumber);
  }

  /**
   * Generate the input that is needed by the job. It will generate the configuration needed by the
   * job and will generate a different output if the job is a sync or a reset.
   */
  private JobInput getJobInput() {
    final Long jobId = workflowInternalState.getJobId();
    final Integer attemptNumber = workflowInternalState.getAttemptNumber();

    final SyncInputWithAttemptNumber getSyncInputActivitySyncInput = new SyncInputWithAttemptNumber(
        attemptNumber,
        jobId);

    return runMandatoryActivityWithOutput(
        (input) -> {
          try {
            return getSyncInputActivity.getSyncWorkflowInputWithAttemptNumber(input);
          } catch (final Exception e) {
            throw new RuntimeException(e);
          }
        },
        getSyncInputActivitySyncInput);
  }

  /**
   * Generate the input that is needed by the checks that run prior to the sync workflow.
   */
  private SyncJobCheckConnectionInputs getCheckConnectionInput() {
    final Long jobId = workflowInternalState.getJobId();
    final Integer attemptNumber = workflowInternalState.getAttemptNumber();

    final SyncInputWithAttemptNumber getSyncInputActivitySyncInput = new SyncInputWithAttemptNumber(
        attemptNumber,
        jobId);

    final SyncJobCheckConnectionInputs checkConnectionInputs = runMandatoryActivityWithOutput(
        getSyncInputActivity::getCheckConnectionInputs,
        getSyncInputActivitySyncInput);

    return checkConnectionInputs;
  }

  /**
   * Report the job as started in the job tracker and set it as running in the workflow internal
   * state.
   *
   * @param connectionId The connection ID associated with this execution of the workflow.
   */
  private void reportJobStarting(final UUID connectionId) {
    runMandatoryActivity(
        jobCreationAndStatusUpdateActivity::reportJobStart,
        new ReportJobStartInput(
            workflowInternalState.getJobId(), connectionId));

    workflowState.setRunning(true);
  }

  /**
   * Start the child {@link SyncWorkflow}. We are using a child workflow here for two main reason:
   * <p>
   * - Originally the Sync workflow was living by himself and was launch by the scheduler. In order to
   * limit the potential migration issues, we kept the {@link SyncWorkflow} as is and launch it as a
   * child workflow.
   * <p>
   * - The {@link SyncWorkflow} has different requirements than the {@link ConnectionManagerWorkflow}
   * since the latter is a long running workflow, in the future, using a different Node pool would
   * make sense.
   */
  private StandardSyncOutput runChildWorkflow(final JobInput jobInputs) {
    final String taskQueue = TemporalTaskQueueUtils.getTaskQueue(TemporalJobType.SYNC);

    final SyncWorkflow childSync = Workflow.newChildWorkflowStub(SyncWorkflow.class,
        ChildWorkflowOptions.newBuilder()
            .setWorkflowId("sync_" + workflowInternalState.getJobId())
            .setTaskQueue(taskQueue)
            // This will cancel the child workflow when the parent is terminated
            .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL)
            .build());

    return childSync.run(
        jobInputs.getJobRunConfig(),
        jobInputs.getSourceLauncherConfig(),
        jobInputs.getDestinationLauncherConfig(),
        jobInputs.getSyncInput(),
        connectionId);
  }

  private StandardSyncOutput runChildWorkflowV2(final UUID connectionId,
                                                final long jobId,
                                                final int attemptNumber,
                                                final UUID sourceId) {
    final String taskQueue = TemporalTaskQueueUtils.getTaskQueue(TemporalJobType.SYNC);

    final SyncWorkflowV2 childSync = Workflow.newChildWorkflowStub(SyncWorkflowV2.class,
        ChildWorkflowOptions.newBuilder()
            .setWorkflowId("sync_" + workflowInternalState.getJobId())
            .setTaskQueue(taskQueue)
            // This will cancel the child workflow when the parent is terminated
            .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL)
            .build());

    return childSync.run(
        new SyncWorkflowV2Input(
            connectionId,
            jobId,
            attemptNumber,
            sourceId));
  }

  private ConnectorJobOutput runCheckInChildWorkflow(final JobRunConfig jobRunConfig,
                                                     final IntegrationLauncherConfig launcherConfig,
                                                     final StandardCheckConnectionInput checkInput) {
    final String workflowId = "check_" + workflowInternalState.getJobId() + "_" + checkInput.getActorType().value();
    final String taskQueue = TemporalTaskQueueUtils.getTaskQueue(TemporalJobType.SYNC);
    final ConnectorCommandWorkflow childCheck = Workflow.newChildWorkflowStub(
        ConnectorCommandWorkflow.class,
        ChildWorkflowOptions.newBuilder()
            .setWorkflowId(workflowId)
            .setTaskQueue(taskQueue)
            // This will cancel the child workflow when the parent is terminated
            .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL)
            .build());

    log.info("Running legacy check for {} with id {}", checkInput.getActorType(), checkInput.getActorId());
    return childCheck.run(new CheckCommandInput(new CheckConnectionInput(jobRunConfig, launcherConfig, checkInput)));
  }

  private ConnectorJobOutput runCheckWithCommandApiInChildWorkflow(final UUID actorId,
                                                                   final String jobId,
                                                                   final Long attemptId,
                                                                   final String actorType) {
    final String workflowId = "check_" + jobId + "_" + actorType;
    final String taskQueue = TemporalTaskQueueUtils.getTaskQueue(TemporalJobType.SYNC);
    final ConnectorCommandWorkflow childCheck = Workflow.newChildWorkflowStub(
        ConnectorCommandWorkflow.class,
        ChildWorkflowOptions.newBuilder()
            .setWorkflowId(workflowId)
            .setTaskQueue(taskQueue)
            // This will cancel the child workflow when the parent is terminated
            .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL)
            .build());

    log.info("Running command check for {} with id {} with the use of the new command API", actorType, actorId);
    return childCheck.run(new CheckCommandApiInput(new CheckCommandApiInput.CheckConnectionApiInput(
        actorId,
        jobId,
        attemptId)));
  }

  /**
   * Set the internal status as failed and save the failures reasons.
   *
   * @return true if the job failed, false otherwise
   */
  private boolean getFailStatus(final StandardSyncOutput standardSyncOutput) {
    final StandardSyncSummary standardSyncSummary = standardSyncOutput.getStandardSyncSummary();

    if (standardSyncSummary != null && standardSyncSummary.getStatus() == ReplicationStatus.FAILED) {
      workflowInternalState.getFailures().addAll(standardSyncOutput.getFailures());
      final var recordsCommitted = (standardSyncSummary.getTotalStats() != null) ? standardSyncSummary.getTotalStats().getRecordsCommitted() : null;
      workflowInternalState.setPartialSuccess(recordsCommitted != null && recordsCommitted > 0);
      return true;
    }

    return false;
  }

  /**
   * Extracts whether the job was cancelled from the output.
   *
   * @return true if the job was cancelled, false otherwise
   */
  private boolean getCancelledStatus(final StandardSyncOutput standardSyncOutput) {
    final StandardSyncSummary summary = standardSyncOutput.getStandardSyncSummary();
    return summary != null && summary.getStatus() == ReplicationStatus.CANCELLED;
  }

  /*
   * Set a job as cancel and continue to the next job if and continue as a reset if needed
   */
  private void reportCancelledAndContinueWith(final boolean skipSchedulingNextRun, final ConnectionUpdaterInput connectionUpdaterInput) {
    if (workflowInternalState.getJobId() != null && workflowInternalState.getAttemptNumber() != null) {
      reportCancelled(connectionUpdaterInput.getConnectionId());
    }
    resetNewConnectionInput(connectionUpdaterInput);
    connectionUpdaterInput.setSkipScheduling(skipSchedulingNextRun);
    prepareForNextRunAndContinueAsNew(connectionUpdaterInput);
  }

  private void reportCancelled(final UUID connectionId) {
    final Long jobId = workflowInternalState.getJobId();
    final Integer attemptNumber = workflowInternalState.getAttemptNumber();
    final Set<FailureReason> failures = workflowInternalState.getFailures();
    final Boolean partialSuccess = workflowInternalState.getPartialSuccess();

    runMandatoryActivity(jobCreationAndStatusUpdateActivity::jobCancelledWithAttemptNumber,
        new JobCancelledInputWithAttemptNumber(
            jobId,
            attemptNumber,
            connectionId,
            FailureHelper.failureSummaryForCancellation(jobId, attemptNumber, failures, partialSuccess)));
  }

  private void deleteResetJobStreams() {
    runMandatoryActivity(streamResetActivity::deleteStreamResetRecordsForJob,
        new DeleteStreamResetRecordsForJobInput(connectionId, workflowInternalState.getJobId()));
  }

  private Duration getWorkflowRestartDelaySeconds() {
    return workflowConfigActivity.getWorkflowRestartDelaySeconds();
  }

  private void traceConnectionId() {
    if (connectionId != null) {
      ApmTraceUtils.addTagsToTrace(Map.of(CONNECTION_ID_KEY, connectionId));
    }
  }

  private void setConnectionId(final ConnectionUpdaterInput connectionUpdaterInput) {
    connectionId = Objects.requireNonNull(connectionUpdaterInput.getConnectionId());
    ApmTraceUtils.addTagsToTrace(Map.of(CONNECTION_ID_KEY, connectionId));
  }

  private void setConnectionContext(final ConnectionContext ctx) {
    connectionContext = Objects.requireNonNull(ctx);
  }

  private boolean useAttemptCountRetries() {
    // the manager can be null if
    // - the activity failed unexpectedly
    // in which case we fall back to simple attempt count retries (attempt count < 3)
    return retryManager == null;
  }

  private Duration resolveBackoff() {
    if (useAttemptCountRetries()) {
      return Duration.ZERO;
    }

    final Duration backoff = retryManager.getBackoff();

    runAppendToAttemptLogActivity(String.format("Backing off for: %s.", retryManager.getBackoffString()), LogLevel.WARN);

    return backoff;
  }

  private RetryManager hydrateRetryManager() {
    final var result = runActivityWithFallback(
        retryStatePersistenceActivity::hydrateRetryState,
        new HydrateInput(workflowInternalState.getJobId(), connectionId),
        new HydrateOutput(null),
        RetryStatePersistenceActivity.class.getName(),
        "hydrateRetryState");

    return result.getManager();
  }

  private void accumulateFailureAndPersist(final boolean madeProgress) {
    if (useAttemptCountRetries()) {
      return;
    }

    retryManager.incrementFailure(madeProgress);

    runActivityWithFallback(
        retryStatePersistenceActivity::persistRetryState,
        new PersistInput(workflowInternalState.getJobId(), connectionId, retryManager),
        new PersistOutput(false),
        RetryStatePersistenceActivity.class.getName(),
        "persistRetryState");

    runAppendToAttemptLogActivity(
        String.format("Retry State: %s\n Backoff before next attempt: %s", retryManager, retryManager.getBackoffString()),
        LogLevel.INFO);
  }

  private void logActivityFailure(final String className, final String methodName) {
    log.error(String.format(
        "FAILED %s.%s for connection id: %s, job id: %d, attempt: %d",
        className,
        methodName,
        connectionId,
        workflowInternalState.getJobId(),
        workflowInternalState.getAttemptNumber()));
  }

  private void recordActivityFailure(final String className, final String methodName) {
    final var attrs = new MetricAttribute[] {
      new MetricAttribute(MetricTags.ACTIVITY_NAME, className),
      new MetricAttribute(MetricTags.ACTIVITY_METHOD, methodName),
    };
    final var inputCtx = new ConnectionUpdaterInput(connectionId, workflowInternalState.getJobId());

    logActivityFailure(className, methodName);
    tryRecordCountMetric(
        new RecordMetricInput(
            inputCtx,
            Optional.empty(),
            OssMetricsRegistry.ACTIVITY_FAILURE,
            attrs));
  }

  private void hydrateIdsFromPreviousRun(final ConnectionUpdaterInput input, final WorkflowInternalState state) {
    // connection updater input attempt number starts at 1 instead of 0
    // TODO: this check can be removed once that is fixed
    if (input.getAttemptNumber() != null) {
      state.setAttemptNumber(input.getAttemptNumber() - 1);
    }

    state.setJobId(input.getJobId());
  }

  private void initializeWorkflowStateFromInput(final ConnectionUpdaterInput input) {
    // if our previous attempt was a failure, we are still in a run
    if (input.getFromFailure()) {
      workflowState.setRunning(true);
    }
    // workflow state is only ever set in test cases. for production cases, it will always be null.
    if (input.getWorkflowState() != null) {
      // only copy over state change listener and ID to avoid trampling functionality
      workflowState.setId(input.getWorkflowState().getId());
      workflowState.setStateChangedListener(input.getWorkflowState().getStateChangedListener());
    }

    hydrateIdsFromPreviousRun(input, workflowInternalState);
  }

  private boolean runAppendToAttemptLogActivity(final String logMsg, final LogLevel level) {
    final var result = runActivityWithFallback(
        appendToAttemptLogActivity::log,
        new LogInput(workflowInternalState.getJobId(), workflowInternalState.getAttemptNumber(), logMsg, level),
        new LogOutput(false),
        AppendToAttemptLogActivity.class.getName(),
        "log");

    return result.getSuccess();
  }

  /**
   * When you want to run an activity with a fallback value instead of failing the run.
   */
  private <T, U> U runActivityWithFallback(final Function<T, U> activityMethod,
                                           final T input,
                                           final U defaultVal,
                                           final String className,
                                           final String methodName) {
    var result = defaultVal;

    try {
      result = activityMethod.apply(input);
    } catch (final Exception e) {
      log.error(e.getMessage());
      recordActivityFailure(className, methodName);
    }

    return result;
  }

  private void cancelSyncChildWorkflow() {
    if (cancellableSyncWorkflow != null) {
      cancellableSyncWorkflow.cancel();
    }
  }

  private Duration getWorkflowDelay() {
    if (workflowDelay != null) {
      return workflowDelay;
    } else {
      return Duration.ofSeconds(600L);
    }
  }

}
