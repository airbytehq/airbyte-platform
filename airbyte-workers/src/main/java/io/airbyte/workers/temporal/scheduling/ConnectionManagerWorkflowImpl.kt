/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling

import datadog.trace.api.Trace
import io.airbyte.commons.constants.WorkerConstants
import io.airbyte.commons.temporal.TemporalJobType
import io.airbyte.commons.temporal.TemporalTaskQueueUtils.getTaskQueue
import io.airbyte.commons.temporal.TemporalWorkflowUtils
import io.airbyte.commons.temporal.TemporalWorkflowUtils.buildStartWorkflowInput
import io.airbyte.commons.temporal.annotations.TemporalActivityStub
import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.commons.temporal.scheduling.CheckCommandApiInput
import io.airbyte.commons.temporal.scheduling.CheckCommandInput
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow.JobInformation
import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput
import io.airbyte.commons.temporal.scheduling.ConnectorCommandWorkflow
import io.airbyte.commons.temporal.scheduling.SyncWorkflow
import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2
import io.airbyte.commons.temporal.scheduling.SyncWorkflowV2Input
import io.airbyte.commons.temporal.scheduling.retries.RetryManager
import io.airbyte.commons.temporal.scheduling.state.WorkflowInternalState
import io.airbyte.commons.temporal.scheduling.state.WorkflowState
import io.airbyte.commons.temporal.scheduling.state.listener.NoopStateListener
import io.airbyte.config.ActorType
import io.airbyte.config.ConnectionContext
import io.airbyte.config.ConnectorJobOutput
import io.airbyte.config.FailureReason
import io.airbyte.config.StandardCheckConnectionInput
import io.airbyte.config.StandardSyncOutput
import io.airbyte.config.StandardSyncSummary
import io.airbyte.config.WorkloadPriority
import io.airbyte.featureflag.UseCommandCheck
import io.airbyte.featureflag.UseSyncV2
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.ATTEMPT_NUMBER_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.Tags.CONNECTION_ID_KEY
import io.airbyte.metrics.lib.ApmTraceConstants.WORKFLOW_TRACE_OPERATION_NAME
import io.airbyte.metrics.lib.ApmTraceUtils
import io.airbyte.metrics.lib.ApmTraceUtils.addExceptionToTrace
import io.airbyte.metrics.lib.MetricTags
import io.airbyte.persistence.job.models.IntegrationLauncherConfig
import io.airbyte.persistence.job.models.JobRunConfig
import io.airbyte.workers.helper.FailureHelper
import io.airbyte.workers.helper.FailureHelper.failureReasonFromWorkflowAndActivity
import io.airbyte.workers.helper.FailureHelper.platformFailure
import io.airbyte.workers.helpers.ContextConversionHelper.buildDestinationContextFrom
import io.airbyte.workers.helpers.ContextConversionHelper.buildSourceContextFrom
import io.airbyte.workers.helpers.ContextConversionHelper.connectionContextToDestinationContext
import io.airbyte.workers.helpers.ContextConversionHelper.connectionContextToSourceContext
import io.airbyte.workers.models.JobInput
import io.airbyte.workers.models.SyncJobCheckConnectionInputs
import io.airbyte.workers.temporal.activities.GetConnectionContextInput
import io.airbyte.workers.temporal.activities.GetLoadShedBackoffInput
import io.airbyte.workers.temporal.scheduling.SyncCheckConnectionResult.Companion.isOutputFailed
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity.LogInput
import io.airbyte.workers.temporal.scheduling.activities.AppendToAttemptLogActivity.LogOutput
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity
import io.airbyte.workers.temporal.scheduling.activities.AutoDisableConnectionActivity.AutoDisableConnectionActivityInput
import io.airbyte.workers.temporal.scheduling.activities.CheckRunProgressActivity
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity
import io.airbyte.workers.temporal.scheduling.activities.ConfigFetchActivity.ScheduleRetrieverInput
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity.FeatureFlagFetchInput
import io.airbyte.workers.temporal.scheduling.activities.FeatureFlagFetchActivity.FeatureFlagFetchOutput
import io.airbyte.workers.temporal.scheduling.activities.GenerateInputActivity
import io.airbyte.workers.temporal.scheduling.activities.GenerateInputActivity.SyncInputWithAttemptNumber
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptCreationInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptNumberCreationOutput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.AttemptNumberFailureInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.EnsureCleanJobStateInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCancelledInputWithAttemptNumber
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCheckFailureInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobCreationInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobFailureInput
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.JobSuccessInputWithAttemptNumber
import io.airbyte.workers.temporal.scheduling.activities.JobCreationAndStatusUpdateActivity.ReportJobStartInput
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity.FailureCause
import io.airbyte.workers.temporal.scheduling.activities.RecordMetricActivity.RecordMetricInput
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.HydrateInput
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.HydrateOutput
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.PersistInput
import io.airbyte.workers.temporal.scheduling.activities.RetryStatePersistenceActivity.PersistOutput
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivity
import io.airbyte.workers.temporal.scheduling.activities.StreamResetActivity.DeleteStreamResetRecordsForJobInput
import io.airbyte.workers.temporal.scheduling.activities.WorkflowConfigActivity
import io.temporal.api.enums.v1.ParentClosePolicy
import io.temporal.failure.ActivityFailure
import io.temporal.failure.CanceledFailure
import io.temporal.failure.ChildWorkflowFailure
import io.temporal.workflow.CancellationScope
import io.temporal.workflow.ChildWorkflowOptions
import io.temporal.workflow.Workflow
import jakarta.annotation.Nullable
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.time.Duration
import java.util.Map
import java.util.Objects
import java.util.Optional
import java.util.UUID
import java.util.function.Consumer
import java.util.function.Function

/**
 * ConnectionManagerWorkflowImpl.
 */
open class ConnectionManagerWorkflowImpl : ConnectionManagerWorkflow {
  private val workflowState = WorkflowState(UUID.randomUUID(), NoopStateListener())

  private val workflowInternalState = WorkflowInternalState()

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val getSyncInputActivity: GenerateInputActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val jobCreationAndStatusUpdateActivity: JobCreationAndStatusUpdateActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val configFetchActivity: ConfigFetchActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val autoDisableConnectionActivity: AutoDisableConnectionActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val streamResetActivity: StreamResetActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val recordMetricActivity: RecordMetricActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val workflowConfigActivity: WorkflowConfigActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val featureFlagFetchActivity: FeatureFlagFetchActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val checkRunProgressActivity: CheckRunProgressActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val retryStatePersistenceActivity: RetryStatePersistenceActivity? = null

  @TemporalActivityStub(activityOptionsBeanName = "shortActivityOptions")
  private val appendToAttemptLogActivity: AppendToAttemptLogActivity? = null

  private var cancellableSyncWorkflow: CancellationScope? = null

  private var connectionId: UUID? = null

  private var workflowDelay: Duration? = null

  private var retryManager: RetryManager? = null

  private var connectionContext: ConnectionContext? = null

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  @Throws(RetryableException::class)
  override fun run(connectionUpdaterInput: ConnectionUpdaterInput) {
    try {
      if (isTombstone(connectionUpdaterInput.connectionId)) {
        return
      }

            /*
             * Hydrate the connection context (workspace, org, source, dest, etc. ids) as soon as possible.
             */
      val hydratedContext =
        runMandatoryActivityWithOutput(
          Function { input: GetConnectionContextInput -> configFetchActivity?.getConnectionContext(input) },
          GetConnectionContextInput(connectionUpdaterInput.connectionId!!),
        )!!
      setConnectionContext(hydratedContext.connectionContext)

            /*
             * Sleep and periodically check in a loop until we're no longer load shed.
             */
      backoffIfLoadShedEnabled(hydratedContext.connectionContext)

            /*
             * Always ensure that the connection ID is set from the input before performing any additional work.
             * Failure to set the connection ID before performing any work in this workflow could result in
             * additional failures when attempting to handle a failed workflow AND/OR the inability to identify
             * impacted connections when errors do occur.
             */
      setConnectionId(connectionUpdaterInput)

      // Copy over data from the input to workflowState early to minimize gaps with signals
      initializeWorkflowStateFromInput(connectionUpdaterInput)

      // Fetch workflow delay first so that it is set if any subsequent activities fail and need to be
      // re-attempted.
      workflowDelay = this.workflowRestartDelaySeconds

      recordMetric(
        RecordMetricInput(
          connectionUpdaterInput,
          Optional.empty<FailureCause>(),
          OssMetricsRegistry.TEMPORAL_WORKFLOW_ATTEMPT,
          null,
        ),
      )

      try {
        cancellableSyncWorkflow = generateSyncWorkflowRunnable(connectionUpdaterInput)
        cancellableSyncWorkflow!!.run()
      } catch (cf: CanceledFailure) {
        // When a scope is cancelled temporal will throw a CanceledFailure as you can see here:
        // https://github.com/temporalio/sdk-java/blob/master/temporal-sdk/src/main/java/io/temporal/workflow/CancellationScope.java#L72
        // The naming is very misleading, it is not a failure but the expected behavior...
        recordMetric(
          RecordMetricInput(
            connectionUpdaterInput,
            Optional.of<FailureCause>(FailureCause.CANCELED),
            OssMetricsRegistry.TEMPORAL_WORKFLOW_FAILURE,
            null,
          ),
        )
      }

      if (workflowState.isDeleted) {
        if (workflowState.isRunning) {
          log.info(
            "Cancelling jobId '{}' because connection '{}' was deleted",
            Objects.toString(connectionUpdaterInput.jobId, "null"),
            connectionUpdaterInput.connectionId,
          )
          // This call is not needed anymore since this will be cancel using the cancellation state
          reportCancelled(connectionId)
        }

        return
      }

      // this means that the current workflow is being cancelled so that a reset can be run instead.
      if (workflowState.isCancelledForReset) {
        reportCancelledAndContinueWith(true, connectionUpdaterInput)
      }

      // "Cancel" button was pressed on a job
      if (workflowState.isCancelled) {
        reportCancelledAndContinueWith(false, connectionUpdaterInput)
      }
    } catch (e: Exception) {
      log.error("The connection update workflow has failed, will create a new attempt.", e)
      reportFailure(connectionUpdaterInput, null, FailureCause.UNKNOWN)
      prepareForNextRunAndContinueAsNew(connectionUpdaterInput)

      // Add the exception to the span, as it represents a platform failure
      addExceptionToTrace(e)
    }
  }

  private fun isTombstone(connectionId: UUID?): Boolean {
    val checkTombstoneVersion =
      Workflow.getVersion(CHECK_WORKSPACE_TOMBSTONE_TAG, Workflow.DEFAULT_VERSION, CHECK_WORKSPACE_TOMBSTONE_CURRENT_VERSION)
    if (checkTombstoneVersion == Workflow.DEFAULT_VERSION || connectionId == null) {
      return false
    }

    return configFetchActivity!!.isWorkspaceTombstone(connectionId)
  }

  private fun backoffIfLoadShedEnabled(connectionContext: ConnectionContext?) {
    val version =
      Workflow.getVersion(LOAD_SHED_BACK_OFF_TAG, Workflow.DEFAULT_VERSION, LOAD_SHED_BACK_OFF_CURRENT_VERSION)
    if (version == Workflow.DEFAULT_VERSION || connectionContext == null) {
      return
    }

    val scheduleRetrieverInput = GetLoadShedBackoffInput(connectionContext)
    var backoff = configFetchActivity!!.getLoadShedBackoff(scheduleRetrieverInput)
    while (backoff.duration.isPositive) {
      Workflow.sleep(backoff.duration)
      backoff = configFetchActivity.getLoadShedBackoff(scheduleRetrieverInput)
    }
  }

  private fun generateSyncWorkflowRunnable(connectionUpdaterInput: ConnectionUpdaterInput): CancellationScope {
    return Workflow.newCancellationScope(
      Runnable {
        if (connectionUpdaterInput.skipScheduling) {
          workflowState.isSkipScheduling = true
        }
        // Clean the job state by failing any jobs for this connection that are currently non-terminal.
        // This catches cases where the temporal workflow was terminated and restarted while a job was
        // actively running, leaving that job in an orphaned and non-terminal state.
        ensureCleanJobState(connectionUpdaterInput)

        // setup retry manager before scheduling to resolve schedule with backoff
        retryManager = hydrateRetryManager()
        if (retryManager != null) {
          runAppendToAttemptLogActivity(String.format("Retry State: %s", retryManager), AppendToAttemptLogActivity.LogLevel.INFO)
        }

        val timeTilScheduledRun = getTimeTilScheduledRun(connectionUpdaterInput.connectionId)

        val timeToWait: Duration?
        if (connectionUpdaterInput.fromFailure) {
          // note this can fail the job if the backoff is longer than scheduled time to wait
          timeToWait = resolveBackoff()
        } else {
          timeToWait = timeTilScheduledRun
        }

        if (!timeToWait!!.isZero) {
          Workflow.await(timeToWait) { this.shouldInterruptWaiting() }
        }

        workflowState.isDoneWaiting = true

        if (workflowState.isDeleted) {
          log.info("Returning from workflow cancellation scope because workflow deletion was requested.")
          return@Runnable
        }

        if (workflowState.isUpdated) {
          // Act as a return
          prepareForNextRunAndContinueAsNew(connectionUpdaterInput)
        }

        if (workflowState.isCancelled) {
          reportCancelledAndContinueWith(false, connectionUpdaterInput)
        }

        // re-hydrate retry manager on run-start because FFs may have changed
        retryManager = hydrateRetryManager()

        // This var is unused since not feature flags are currently required in this workflow
        // We keep the activity around to get any feature flags that might be needed in the future
        val featureFlags = getFeatureFlags(connectionUpdaterInput.connectionId)

        workflowInternalState.jobId = getOrCreateJobId(connectionUpdaterInput)
        workflowInternalState.attemptNumber = createAttempt(workflowInternalState.jobId!!)

        var jobInputs: JobInput? = null
        val shouldRunCheckInputGeneration = shouldRunCheckInputGeneration()
        if (!shouldRunCheckInputGeneration) {
          jobInputs = this.jobInput
        }

        reportJobStarting(connectionUpdaterInput.connectionId)
        var standardSyncOutput: StandardSyncOutput? = null
        try {
          val shouldRunWithCheckCommandFFValue = featureFlags.get(UseCommandCheck.key)
          val shouldRunWithCheckCommand = if (shouldRunWithCheckCommandFFValue == null) false else shouldRunWithCheckCommandFFValue
          val canUseCheckWithCommandApi =
            Workflow.getVersion(
              CHECK_USING_COMMAND_API_TAG,
              Workflow.DEFAULT_VERSION,
              CHECK_USING_COMMAND_API_VERSION,
            ) > Workflow.DEFAULT_VERSION

          val syncCheckConnectionResult =
            if (shouldRunWithCheckCommand && canUseCheckWithCommandApi) {
              checkConnectionsWithCommandApi(
                connectionContext!!.sourceId,
                connectionContext!!.destinationId,
                workflowInternalState.jobId!!,
                workflowInternalState.attemptNumber!!.toLong(),
                connectionUpdaterInput.resetConnection,
              )
            } else {
              checkConnections(
                this.jobRunConfig,
                jobInputs,
              )
            }
          if (syncCheckConnectionResult.isFailed) {
            val checkFailureOutput = syncCheckConnectionResult.buildFailureOutput()
            workflowState.isFailed = getFailStatus(checkFailureOutput)
            reportFailure(connectionUpdaterInput, checkFailureOutput, FailureCause.CONNECTION)
          } else {
            if (shouldRunCheckInputGeneration) {
              jobInputs = this.jobInput
            }

            val useSyncWorkflowV2: Boolean = featureFlags.getOrDefault(UseSyncV2.key, false)!!
            val canSyncWorkflowV2 =
              Workflow.getVersion(
                USE_SYNC_WORKFLOW_V2_TAG,
                Workflow.DEFAULT_VERSION,
                USE_SYNC_WORKFLOW_V2_VERSION,
              ) > Workflow.DEFAULT_VERSION
            standardSyncOutput =
              if (canSyncWorkflowV2 && useSyncWorkflowV2) {
                runChildWorkflowV2(
                  connectionId!!,
                  workflowInternalState.jobId!!,
                  workflowInternalState.attemptNumber!!,
                  connectionContext!!.sourceId,
                )
              } else {
                runChildWorkflow(jobInputs!!)
              }
            workflowState.isFailed = getFailStatus(standardSyncOutput)
            workflowState.isCancelled = getCancelledStatus(standardSyncOutput)

            if (workflowState.isFailed) {
              reportFailure(connectionUpdaterInput, standardSyncOutput, FailureCause.UNKNOWN)
            } else if (workflowState.isCancelled) {
              reportCancelledAndContinueWith(false, connectionUpdaterInput)
            } else {
              reportSuccess(connectionUpdaterInput, standardSyncOutput)
            }
          }

          prepareForNextRunAndContinueAsNew(connectionUpdaterInput)
        } catch (childWorkflowFailure: ChildWorkflowFailure) {
          // when we cancel a method, we call the cancel method of the cancellation scope. This will throw an
          // exception since we expect it, we just
          // silently ignore it.
          if (childWorkflowFailure.cause is CanceledFailure) {
            log.debug("Ignoring canceled failure as it is handled by the cancellation scope.")
            // do nothing, cancellation handled by cancellationScope
          } else if (childWorkflowFailure.cause is ActivityFailure) {
            val af: ActivityFailure = childWorkflowFailure.cause as ActivityFailure
            // Allows us to classify unhandled failures from the sync workflow.
            workflowInternalState.failures.add(
              failureReasonFromWorkflowAndActivity(
                childWorkflowFailure.workflowType,
                af.cause!!,
                workflowInternalState.jobId,
                workflowInternalState.attemptNumber,
              ),
            )
            addExceptionToTrace(af.cause!!)
            reportFailure(connectionUpdaterInput, standardSyncOutput, FailureCause.ACTIVITY)
            prepareForNextRunAndContinueAsNew(connectionUpdaterInput)
          } else {
            workflowInternalState.failures.add(
              FailureHelper.unknownOriginFailure(
                childWorkflowFailure.cause!!,
                workflowInternalState.jobId,
                workflowInternalState.attemptNumber,
              ),
            )
            addExceptionToTrace(childWorkflowFailure.cause!!)
            reportFailure(connectionUpdaterInput, standardSyncOutput, FailureCause.WORKFLOW)
            prepareForNextRunAndContinueAsNew(connectionUpdaterInput)
          }
        }
      },
    )
  }

  private fun reportSuccess(
    connectionUpdaterInput: ConnectionUpdaterInput,
    standardSyncOutput: StandardSyncOutput?,
  ) {
    workflowState.isSuccess = true

    runMandatoryActivity<JobSuccessInputWithAttemptNumber?>(
      { input: JobSuccessInputWithAttemptNumber? -> jobCreationAndStatusUpdateActivity!!.jobSuccessWithAttemptNumber(input!!) },
      JobSuccessInputWithAttemptNumber(
        workflowInternalState.jobId,
        workflowInternalState.attemptNumber,
        connectionUpdaterInput.connectionId,
        standardSyncOutput,
      ),
    )

    deleteResetJobStreams()

    // Record the success metric
    recordMetric(RecordMetricInput(connectionUpdaterInput, Optional.empty<FailureCause>(), OssMetricsRegistry.TEMPORAL_WORKFLOW_SUCCESS, null))

    resetNewConnectionInput(connectionUpdaterInput)
  }

  private fun reportFailure(
    connectionUpdaterInput: ConnectionUpdaterInput,
    standardSyncOutput: StandardSyncOutput?,
    failureCause: FailureCause,
    failureReasonsOverride: MutableSet<FailureReason> = HashSet<FailureReason>(),
  ) {
    val failureReasons: MutableSet<FailureReason> =
      if (failureReasonsOverride.isEmpty()) workflowInternalState.failures else failureReasonsOverride

    runMandatoryActivity<AttemptNumberFailureInput?>(
      { input: AttemptNumberFailureInput? -> jobCreationAndStatusUpdateActivity!!.attemptFailureWithAttemptNumber(input!!) },
      AttemptNumberFailureInput(
        workflowInternalState.jobId,
        workflowInternalState.attemptNumber,
        connectionUpdaterInput.connectionId,
        standardSyncOutput,
        FailureHelper.failureSummary(failureReasons, workflowInternalState.partialSuccess),
      ),
    )

    // ATTENTION: connectionUpdaterInput.getAttemptNumber() is 1-based (usually)
    // this differs from workflowInternalState.getAttemptNumber() being 0-based.
    // TODO: Don't mix these bases. Bug filed https://github.com/airbytehq/airbyte/issues/27808
    val attemptNumber: Int = connectionUpdaterInput.attemptNumber!!
    ApmTraceUtils.addTagsToTrace(Map.of<String?, Int?>(ATTEMPT_NUMBER_KEY, attemptNumber))

    // This is outside the retry if/else block because we will pass it to our retry manager regardless
    // of retry state.
    // This will be added in a future PR as we develop this feature.
    val madeProgress = checkRunProgress()
    accumulateFailureAndPersist(madeProgress)

    val failureType: FailureReason.FailureType? =
      if (standardSyncOutput != null) {
        if (standardSyncOutput.failures.isEmpty()) {
          null
        } else {
          standardSyncOutput.failures.first().failureType
        }
      } else {
        null
      }
    if (isWithinRetryLimit(attemptNumber) && failureType != FailureReason.FailureType.CONFIG_ERROR) {
      // restart from failure
      connectionUpdaterInput.attemptNumber = attemptNumber + 1
      connectionUpdaterInput.fromFailure = true

      if (madeProgress) {
        recordProgressMetric(connectionUpdaterInput, failureCause, true)
      }
    } else {
      val internalFailureMessage =
        failureReasons
          .stream()
          .findFirst()
          .map(Function { obj: FailureReason? -> obj!!.internalMessage })
          .orElse("Unknown failure reason")
      val failureReason =
        if (failureType == FailureReason.FailureType.CONFIG_ERROR) {
          internalFailureMessage
        } else {
          "Job failed after too many retries for connection " + connectionId
        }
      failJob(connectionUpdaterInput, failureReason)

      val attrs: Array<MetricAttribute> =
        arrayOf(
          MetricAttribute(MetricTags.MADE_PROGRESS, madeProgress.toString()),
        )
      // Record the failure metric
      recordMetric(
        RecordMetricInput(
          connectionUpdaterInput,
          Optional.of<FailureCause>(failureCause),
          OssMetricsRegistry.TEMPORAL_WORKFLOW_FAILURE,
          attrs,
        ),
      )
      // Record whether we made progress
      if (madeProgress) {
        recordProgressMetric(connectionUpdaterInput, failureCause, false)
      }

      resetNewConnectionInput(connectionUpdaterInput)
    }
  }

  private fun failJob(
    input: ConnectionUpdaterInput,
    failureReason: String?,
  ) {
    runAppendToAttemptLogActivity(
      String.format("Failing job: %d, reason: %s", input.jobId, failureReason),
      AppendToAttemptLogActivity.LogLevel.ERROR,
    )

    runMandatoryActivity<JobFailureInput?>(
      { input: JobFailureInput? -> jobCreationAndStatusUpdateActivity!!.jobFailure(input!!) },
      JobFailureInput(
        input.jobId,
        input.attemptNumber,
        input.connectionId,
        failureReason,
      ),
    )

    val autoDisableConnectionActivityInput = AutoDisableConnectionActivityInput()
    autoDisableConnectionActivityInput.connectionId = connectionId
    val output =
      runMandatoryActivityWithOutput(
        Function { input: AutoDisableConnectionActivityInput ->
          autoDisableConnectionActivity?.autoDisableFailingConnection(input)
        },
        autoDisableConnectionActivityInput,
      )!!
    if (output.isDisabled) {
      log.info("Auto-disabled for constantly failing for Connection {}", connectionId)
    }
  }

  private fun recordProgressMetric(
    input: ConnectionUpdaterInput?,
    cause: FailureCause?,
    willRetry: Boolean,
  ) {
    // job id and other attrs get populated by the wrapping activity
    val attrs: Array<MetricAttribute> =
      arrayOf(
        MetricAttribute(MetricTags.WILL_RETRY, willRetry.toString()),
        MetricAttribute(MetricTags.ATTEMPT_NUMBER, workflowInternalState.attemptNumber.toString()),
      )

    tryRecordCountMetric(
      RecordMetricInput(
        input,
        Optional.ofNullable<FailureCause>(cause),
        OssMetricsRegistry.REPLICATION_MADE_PROGRESS,
        attrs,
      ),
    )
  }

  private fun isWithinRetryLimit(attemptNumber: Int): Boolean {
    if (useAttemptCountRetries()) {
      val maxAttempt = configFetchActivity!!.getMaxAttempt().maxAttempt

      return maxAttempt > attemptNumber
    }

    return retryManager!!.shouldRetry()
  }

  private fun checkRunProgress(): Boolean {
    // we don't use `runMandatoryActivity` to prevent an infinite loop (reportFailure ->
    // runMandatoryActivity -> reportFailure...)

    val result =
      runActivityWithFallback<CheckRunProgressActivity.Input?, CheckRunProgressActivity.Output>(
        { input: CheckRunProgressActivity.Input? ->
          checkRunProgressActivity?.checkProgress(input!!)
        },
        CheckRunProgressActivity.Input(
          workflowInternalState.jobId,
          workflowInternalState.attemptNumber,
          connectionId,
        ),
        CheckRunProgressActivity.Output(false),
        CheckRunProgressActivity::class.java.getName(),
        "checkProgress",
      )!!

    return result.madeProgress()!!
  }

  /**
   * Returns whether the new check input generation activity should be called, depending on the
   * presence of workflow versioning. This should be removed once the new activity is fully rolled
   * out.
   */
  private fun shouldRunCheckInputGeneration(): Boolean {
    val generateCheckInputVersion =
      Workflow.getVersion(GENERATE_CHECK_INPUT_TAG, Workflow.DEFAULT_VERSION, GENERATE_CHECK_INPUT_CURRENT_VERSION)
    return generateCheckInputVersion >= GENERATE_CHECK_INPUT_CURRENT_VERSION
  }

  private fun getCheckConnectionInputFromSync(jobInputs: JobInput): SyncJobCheckConnectionInputs {
    val syncInput = jobInputs.syncInput
    val sourceConfig = syncInput!!.sourceConfiguration
    val destinationConfig = syncInput.destinationConfiguration
    val sourceLauncherConfig = jobInputs.sourceLauncherConfig
    val destinationLauncherConfig = jobInputs.destinationLauncherConfig

    val standardCheckInputSource =
      StandardCheckConnectionInput()
        .withActorType(ActorType.SOURCE)
        .withActorId(syncInput.sourceId)
        .withConnectionConfiguration(sourceConfig)
        .withActorContext(connectionContextToSourceContext(syncInput.connectionContext))

    val standardCheckInputDestination =
      StandardCheckConnectionInput()
        .withActorType(ActorType.DESTINATION)
        .withActorId(syncInput.destinationId)
        .withConnectionConfiguration(destinationConfig)
        .withActorContext(connectionContextToDestinationContext(syncInput.connectionContext))

    return SyncJobCheckConnectionInputs(
      sourceLauncherConfig,
      destinationLauncherConfig,
      standardCheckInputSource,
      standardCheckInputDestination,
    )
  }

  private fun checkConnections(
    jobRunConfig: JobRunConfig,
    @Nullable jobInputs: JobInput?,
  ): SyncCheckConnectionResult {
    val checkConnectionResult = SyncCheckConnectionResult(jobRunConfig)

    val jobStateInput =
      JobCheckFailureInput(jobRunConfig.jobId.toLong(), jobRunConfig.attemptId.toInt(), connectionId)
    val isLastJobOrAttemptFailure =
      runMandatoryActivityWithOutput(
        Function { input: JobCheckFailureInput ->
          jobCreationAndStatusUpdateActivity?.isLastJobOrAttemptFailure(input)
        },
        jobStateInput,
      )!!

    if (!isLastJobOrAttemptFailure) {
      log.info("SOURCE CHECK: Skipped, last attempt was not a failure")
      log.info("DESTINATION CHECK: Skipped, last attempt was not a failure")
      return checkConnectionResult
    }

    val checkInputs: SyncJobCheckConnectionInputs
    if (!shouldRunCheckInputGeneration() && jobInputs != null) {
      checkInputs = getCheckConnectionInputFromSync(jobInputs)
    } else {
      checkInputs = this.checkConnectionInput
    }

    val sourceLauncherConfig =
      checkInputs.sourceLauncherConfig!!
        .withPriority(WorkloadPriority.DEFAULT)

    if (isResetJob(sourceLauncherConfig) || checkConnectionResult.isFailed) {
      // reset jobs don't need to connect to any external source, so check connection is unnecessary
      log.info("SOURCE CHECK: Skipped, reset job")
    } else {
      log.info("SOURCE CHECK: Starting")
      val sourceCheckResponse: ConnectorJobOutput
      sourceCheckResponse =
        runCheckInChildWorkflow(
          jobRunConfig,
          sourceLauncherConfig,
          StandardCheckConnectionInput()
            .withActorType(ActorType.SOURCE)
            .withActorId(checkInputs.sourceCheckConnectionInput!!.actorId)
            .withConnectionConfiguration(checkInputs.sourceCheckConnectionInput!!.connectionConfiguration)
            .withResourceRequirements(checkInputs.sourceCheckConnectionInput!!.resourceRequirements)
            .withActorContext(buildSourceContextFrom(jobInputs, checkInputs)),
        )

      if (isOutputFailed(sourceCheckResponse)) {
        checkConnectionResult.setFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        checkConnectionResult.setFailureOutput(sourceCheckResponse)
        log.info("SOURCE CHECK: Failed")
      } else {
        log.info("SOURCE CHECK: Successful")
      }
    }

    if (checkConnectionResult.isFailed) {
      log.info("DESTINATION CHECK: Skipped, source check failed")
    } else {
      val launcherConfig =
        checkInputs.destinationLauncherConfig!!
          .withPriority(WorkloadPriority.DEFAULT)
      log.info("DESTINATION CHECK: Starting")
      val checkDestInput =
        StandardCheckConnectionInput()
          .withActorType(ActorType.DESTINATION)
          .withActorId(checkInputs.destinationCheckConnectionInput!!.actorId)
          .withConnectionConfiguration(checkInputs.destinationCheckConnectionInput!!.connectionConfiguration)
          .withActorContext(buildDestinationContextFrom(jobInputs, checkInputs))

      val shouldPassReqs =
        Workflow.getVersion(
          PASS_DEST_REQS_TO_CHECK_TAG,
          Workflow.DEFAULT_VERSION,
          PASS_DEST_REQS_TO_CHECK_CURRENT_VERSION,
        ) >= PASS_DEST_REQS_TO_CHECK_CURRENT_VERSION
      if (shouldPassReqs) {
        checkDestInput.resourceRequirements = checkInputs.destinationCheckConnectionInput!!.resourceRequirements
      }

      val destinationCheckResponse =
        runCheckInChildWorkflow(
          jobRunConfig,
          launcherConfig,
          checkDestInput,
        )
      if (isOutputFailed(destinationCheckResponse)) {
        checkConnectionResult.setFailureOrigin(FailureReason.FailureOrigin.DESTINATION)
        checkConnectionResult.setFailureOutput(destinationCheckResponse)
        log.info("DESTINATION CHECK: Failed")
      } else {
        log.info("DESTINATION CHECK: Successful")
      }
    }

    return checkConnectionResult
  }

  private fun checkConnectionsWithCommandApi(
    sourceActorId: UUID,
    destinationActorId: UUID,
    jobId: Long,
    attemptId: Long,
    isReset: Boolean,
  ): SyncCheckConnectionResult {
    val checkConnectionResult = SyncCheckConnectionResult(jobId, attemptId.toInt())

    val jobStateInput =
      JobCheckFailureInput(jobId, attemptId.toInt(), connectionId)
    val isLastJobOrAttemptFailure =
      runMandatoryActivityWithOutput(
        Function { input: JobCheckFailureInput ->
          jobCreationAndStatusUpdateActivity?.isLastJobOrAttemptFailure(input)
        },
        jobStateInput,
      )!!

    if (!isLastJobOrAttemptFailure) {
      log.info("SOURCE CHECK: Skipped, last attempt was not a failure")
      log.info("DESTINATION CHECK: Skipped, last attempt was not a failure")
      return checkConnectionResult
    }

    if (isReset) {
      // reset jobs don't need to connect to any external source, so check connection is unnecessary
      log.info("SOURCE CHECK: Skipped, reset job")
    } else {
      log.info("SOURCE CHECK: Starting")
      val sourceCheckResponse: ConnectorJobOutput
      sourceCheckResponse = runCheckWithCommandApiInChildWorkflow(sourceActorId, jobId.toString(), attemptId, ActorType.SOURCE.value())

      if (isOutputFailed(sourceCheckResponse)) {
        checkConnectionResult.setFailureOrigin(FailureReason.FailureOrigin.SOURCE)
        checkConnectionResult.setFailureOutput(sourceCheckResponse)
        log.info("SOURCE CHECK: Failed")
      } else {
        log.info("SOURCE CHECK: Successful")
      }
    }

    if (checkConnectionResult.isFailed) {
      log.info("DESTINATION CHECK: Skipped, source check failed")
    } else {
      val destinationCheckResponse =
        runCheckWithCommandApiInChildWorkflow(
          destinationActorId,
          jobId.toString(),
          attemptId,
          ActorType.DESTINATION.value(),
        )
      if (isOutputFailed(destinationCheckResponse)) {
        checkConnectionResult.setFailureOrigin(FailureReason.FailureOrigin.DESTINATION)
        checkConnectionResult.setFailureOutput(destinationCheckResponse)
        log.info("DESTINATION CHECK: Failed")
      } else {
        log.info("DESTINATION CHECK: Successful")
      }
    }

    return checkConnectionResult
  }

  private fun isResetJob(sourceLauncherConfig: IntegrationLauncherConfig): Boolean =
    WorkerConstants.RESET_JOB_SOURCE_DOCKER_IMAGE_STUB == sourceLauncherConfig.dockerImage

  // reset the ConnectionUpdaterInput back to a default state
  private fun resetNewConnectionInput(connectionUpdaterInput: ConnectionUpdaterInput) {
    connectionUpdaterInput.jobId = null
    connectionUpdaterInput.attemptNumber = 1
    connectionUpdaterInput.fromFailure = false
    connectionUpdaterInput.skipScheduling = false
  }

  override fun submitManualSync() {
    if (workflowState.isRunning) {
      log.info("Can't schedule a manual workflow if a sync is running for connection {}", connectionId)
      return
    }

    workflowState.isSkipScheduling = true
  }

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  override fun cancelJob() {
    traceConnectionId()
    if (!workflowState.isRunning) {
      log.info("Can't cancel a non-running sync for connection {}", connectionId)
      return
    }
    workflowState.isCancelled = true
    cancelSyncChildWorkflow()
  }

  // TODO: Delete when the don't delete in temporal is removed
  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  override fun deleteConnection() {
    traceConnectionId()
    workflowState.isDeleted = true
    log.info("Set as deleted and canceling job for connection {}", connectionId)
    cancelJob()
  }

  override fun connectionUpdated() {
    workflowState.isUpdated = true
  }

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  override fun resetConnection() {
    traceConnectionId()

    // Assumes that the streams_reset has already been populated with streams to reset for this
    // connection
    if (workflowState.isDoneWaiting) {
      workflowState.isCancelledForReset = true
      cancelSyncChildWorkflow()
    } else {
      workflowState.isSkipScheduling = true
    }
  }

  @Trace(operationName = WORKFLOW_TRACE_OPERATION_NAME)
  override fun resetConnectionAndSkipNextScheduling() {
    traceConnectionId()

    if (workflowState.isDoneWaiting) {
      workflowState.isCancelledForReset = true
      workflowState.isSkipSchedulingNextWorkflow = true
      cancelSyncChildWorkflow()
    } else {
      workflowState.isSkipScheduling = true
      workflowState.isSkipSchedulingNextWorkflow = true
    }
  }

  override fun getState(): WorkflowState = workflowState

  override fun getJobInformation(): JobInformation {
    val jobId: Long =
      (if (workflowInternalState.jobId != null) workflowInternalState.jobId else ConnectionManagerWorkflow.Companion.NON_RUNNING_JOB_ID)!!
    val attemptNumber = workflowInternalState.attemptNumber
    return JobInformation(
      jobId,
      if (attemptNumber == null) ConnectionManagerWorkflow.Companion.NON_RUNNING_ATTEMPT_ID else attemptNumber,
    )
  }

  /**
   * return true if the workflow is in a state that require it to continue. If the state is to process
   * an update or delete the workflow, it won't continue with a run of the [SyncWorkflow] but it
   * will: - restart for an update - Update the connection status and terminate the workflow for a
   * delete
   */
  private fun shouldInterruptWaiting(): Boolean =
    workflowState.isSkipScheduling || workflowState.isDeleted || workflowState.isUpdated || workflowState.isCancelled

  private fun prepareForNextRunAndContinueAsNew(connectionUpdaterInput: ConnectionUpdaterInput) {
    // Continue the workflow as new
    workflowInternalState.failures.clear()
    workflowInternalState.partialSuccess = null
    val isDeleted = workflowState.isDeleted
    if (workflowState.isSkipSchedulingNextWorkflow) {
      connectionUpdaterInput.skipScheduling = true
    }
    workflowState.reset()
    if (!isDeleted) {
      Workflow.continueAsNew(connectionUpdaterInput)
    }
  }

  /**
   * This is running a lambda function that takes {@param input} as an input. If the run of the lambda
   * throws an exception, the workflow will retried after a short delay.
   *
   *
   * Note that if the lambda activity is configured to have retries, the exception will only be caught
   * after the activity has been retried the maximum number of times.
   *
   *
   * This method is meant to be used for calling temporal activities.
   */
  private fun <INPUT, OUTPUT> runMandatoryActivityWithOutput(
    mapper: Function<INPUT?, OUTPUT?>,
    input: INPUT?,
  ): OUTPUT? {
    try {
      return mapper.apply(input)
    } catch (e: Exception) {
      val sleepDuration = getWorkflowDelay()
      log.error(
        "[ACTIVITY-FAILURE] Connection {} failed to run an activity.({}).  Connection manager workflow will be restarted after a delay of {}.",
        connectionId,
        input!!.javaClass.getSimpleName(),
        sleepDuration,
        e,
      )

      // TODO (https://github.com/airbytehq/airbyte/issues/13773) add tracking/notification

      // Wait a short delay before restarting workflow. This is important if, for example, the failing
      // activity was configured to not have retries.
      // Without this delay, that activity could cause the workflow to loop extremely quickly,
      // overwhelming temporal.
      log.info(
        "Waiting {} before restarting the workflow for connection {}, to prevent spamming temporal with restarts.",
        sleepDuration,
        connectionId,
      )
      Workflow.sleep(sleepDuration)

      // Add the exception to the span, as it represents a platform failure
      addExceptionToTrace(e)

      // If a jobId exist set the failure reason
      if (workflowInternalState.jobId != null && workflowInternalState.attemptNumber != null) {
        val connectionUpdaterInput = connectionUpdaterInputFromState()
        val failureReason =
          platformFailure(e, workflowInternalState.jobId, workflowInternalState.attemptNumber)
        reportFailure(connectionUpdaterInput, null, FailureCause.ACTIVITY, mutableSetOf(failureReason))
      } else {
        log.warn("Can't properly fail the job, the next run will clean the state in the EnsureCleanJobStateActivity")
      }

      log.info("Finished wait for connection {}, restarting connection manager workflow", connectionId)

      val newWorkflowInput = buildStartWorkflowInput(connectionId)

      Workflow.continueAsNew(newWorkflowInput)

      throw IllegalStateException(
        (
          "This statement should never be reached, as the ConnectionManagerWorkflow for connection " +
            connectionId + " was continued as new."
        ),
        e,
      )
    }
  }

  private fun connectionUpdaterInputFromState(): ConnectionUpdaterInput =
    ConnectionUpdaterInput(
      connectionId,
      workflowInternalState.jobId,
      null,
      false,
      workflowInternalState.attemptNumber,
      null,
      false,
      false,
      false,
    )

  /**
   * Similar to runMandatoryActivityWithOutput but for methods that don't return.
   */
  private fun <INPUT> runMandatoryActivity(
    consumer: Consumer<INPUT?>,
    input: INPUT?,
  ) {
    runMandatoryActivityWithOutput<INPUT?, Any?>({ inputInternal: INPUT? ->
      consumer.accept(inputInternal)
      null
    }, input)
  }

  /**
   * Calculate the duration to wait so the workflow adheres to its schedule. This lets us 'schedule'
   * the next run.
   *
   *
   * This is calculated by [ConfigFetchActivity.getTimeToWait] and
   * depends on the last successful run and the schedule.
   *
   *
   * Wait time is infinite If the workflow is manual or disabled since we never want to schedule this.
   */
  private fun getTimeTilScheduledRun(connectionId: UUID?): Duration? {
    // Scheduling
    val scheduleRetrieverInput = ScheduleRetrieverInput(connectionId)

    val scheduleRetrieverOutput =
      runMandatoryActivityWithOutput(
        { input: ScheduleRetrieverInput? -> configFetchActivity?.getTimeToWait(input!!) },
        scheduleRetrieverInput,
      )!!

    return scheduleRetrieverOutput.timeToWait
  }

  private fun ensureCleanJobState(connectionUpdaterInput: ConnectionUpdaterInput) {
    if (connectionUpdaterInput.jobId != null) {
      log.info("This workflow is already attached to a job, so no need to clean job state.")
      return
    }

    runMandatoryActivity<EnsureCleanJobStateInput?>({ input: EnsureCleanJobStateInput? ->
      jobCreationAndStatusUpdateActivity!!.ensureCleanJobState(
        input!!,
      )
    }, EnsureCleanJobStateInput(connectionId))
  }

  private fun recordMetric(recordMetricInput: RecordMetricInput?) {
    runMandatoryActivity<RecordMetricInput?>({ metricInput: RecordMetricInput? ->
      recordMetricActivity!!.recordWorkflowCountMetric(
        metricInput!!,
      )
    }, recordMetricInput)
  }

  /**
   * Unlike `recordMetric` above, we won't fail the attempt if this metric recording fails.
   */
  private fun tryRecordCountMetric(recordMetricInput: RecordMetricInput) {
    try {
      recordMetricActivity!!.recordWorkflowCountMetric(recordMetricInput)
    } catch (e: Exception) {
      logActivityFailure(recordMetricActivity!!.javaClass.getName(), "recordWorkflowCountMetric")
    }
  }

  /**
   * Creates a new job if it is not present in the input. If the jobId is specified in the input of
   * the connectionManagerWorkflow, we will return it. Otherwise we will create a job and return its
   * id.
   */
  private fun getOrCreateJobId(connectionUpdaterInput: ConnectionUpdaterInput): Long? {
    if (connectionUpdaterInput.jobId != null) {
      return connectionUpdaterInput.jobId
    }

    val jobCreationOutput =
      runMandatoryActivityWithOutput(
        { input: JobCreationInput? -> jobCreationAndStatusUpdateActivity?.createNewJob(input!!) },
        JobCreationInput(connectionUpdaterInput.connectionId, !workflowState.isSkipScheduling),
      )!!
    connectionUpdaterInput.jobId = jobCreationOutput.jobId

    return jobCreationOutput.jobId
  }

  private fun getFeatureFlags(connectionId: UUID?): MutableMap<String?, Boolean?> {
    val getFeatureFlagsVersion =
      Workflow.getVersion(GET_FEATURE_FLAGS_TAG, Workflow.DEFAULT_VERSION, GET_FEATURE_FLAGS_CURRENT_VERSION)

    if (getFeatureFlagsVersion < GET_FEATURE_FLAGS_CURRENT_VERSION) {
      return Map.of<String?, Boolean?>()
    }

    val getFlagsOutput =
      runMandatoryActivityWithOutput<FeatureFlagFetchInput?, FeatureFlagFetchOutput>({ input: FeatureFlagFetchInput? ->
        featureFlagFetchActivity?.getFeatureFlags(input!!)
      }, FeatureFlagFetchInput(connectionId))!!
    return getFlagsOutput.featureFlags!!
  }

  /**
   * Create a new attempt for a given jobId.
   *
   * @param jobId - the jobId associated with the new attempt
   *
   * @return The attempt number
   */
  private fun createAttempt(jobId: Long): Int? {
    val attemptNumberCreationOutput =
      runMandatoryActivityWithOutput<AttemptCreationInput?, AttemptNumberCreationOutput>(
        { input: AttemptCreationInput? -> jobCreationAndStatusUpdateActivity?.createNewAttemptNumber(input!!) },
        AttemptCreationInput(jobId),
      )!!
    return attemptNumberCreationOutput.attemptNumber
  }

  private val jobRunConfig: JobRunConfig
    get() {
      val jobId = workflowInternalState.jobId
      val attemptNumber = workflowInternalState.attemptNumber
      return TemporalWorkflowUtils.createJobRunConfig(jobId!!, attemptNumber!!)
    }

  private val jobInput: JobInput?
    /**
     * Generate the input that is needed by the job. It will generate the configuration needed by the
     * job and will generate a different output if the job is a sync or a reset.
     */
    get() {
      val jobId = workflowInternalState.jobId
      val attemptNumber = workflowInternalState.attemptNumber

      val getSyncInputActivitySyncInput =
        SyncInputWithAttemptNumber(
          attemptNumber!!,
          jobId!!,
        )

      return runMandatoryActivityWithOutput<SyncInputWithAttemptNumber, JobInput>(
        Function { input: SyncInputWithAttemptNumber? ->
          try {
            return@Function getSyncInputActivity!!.getSyncWorkflowInputWithAttemptNumber(input!!)
          } catch (e: Exception) {
            throw RuntimeException(e)
          }
        },
        getSyncInputActivitySyncInput,
      )
    }

  private val checkConnectionInput: SyncJobCheckConnectionInputs
    /**
     * Generate the input that is needed by the checks that run prior to the sync workflow.
     */
    get() {
      val jobId = workflowInternalState.jobId
      val attemptNumber = workflowInternalState.attemptNumber

      val getSyncInputActivitySyncInput =
        SyncInputWithAttemptNumber(
          attemptNumber!!,
          jobId!!,
        )

      val checkConnectionInputs =
        runMandatoryActivityWithOutput(
          { input: SyncInputWithAttemptNumber? -> getSyncInputActivity?.getCheckConnectionInputs(input!!) },
          getSyncInputActivitySyncInput,
        )!!

      return checkConnectionInputs
    }

  /**
   * Report the job as started in the job tracker and set it as running in the workflow internal
   * state.
   *
   * @param connectionId The connection ID associated with this execution of the workflow.
   */
  private fun reportJobStarting(connectionId: UUID?) {
    runMandatoryActivity<ReportJobStartInput?>(
      { reportJobStartInput: ReportJobStartInput? -> jobCreationAndStatusUpdateActivity!!.reportJobStart(reportJobStartInput!!) },
      ReportJobStartInput(
        workflowInternalState.jobId,
        connectionId,
      ),
    )

    workflowState.isRunning = true
  }

  /**
   * Start the child [SyncWorkflow]. We are using a child workflow here for two main reason:
   *
   *
   * - Originally the Sync workflow was living by himself and was launch by the scheduler. In order to
   * limit the potential migration issues, we kept the [SyncWorkflow] as is and launch it as a
   * child workflow.
   *
   *
   * - The [SyncWorkflow] has different requirements than the [ConnectionManagerWorkflow]
   * since the latter is a long running workflow, in the future, using a different Node pool would
   * make sense.
   */
  private fun runChildWorkflow(jobInputs: JobInput): StandardSyncOutput {
    val taskQueue = getTaskQueue(TemporalJobType.SYNC)

    val childSync =
      Workflow.newChildWorkflowStub(
        SyncWorkflow::class.java,
        ChildWorkflowOptions
          .newBuilder()
          .setWorkflowId("sync_" + workflowInternalState.jobId)
          .setTaskQueue(taskQueue) // This will cancel the child workflow when the parent is terminated
          .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL)
          .build(),
      )

    return childSync.run(
      jobInputs.jobRunConfig!!,
      jobInputs.sourceLauncherConfig!!,
      jobInputs.destinationLauncherConfig!!,
      jobInputs.syncInput!!,
      connectionId!!,
    )
  }

  private fun runChildWorkflowV2(
    connectionId: UUID,
    jobId: Long,
    attemptNumber: Int,
    sourceId: UUID,
  ): StandardSyncOutput {
    val taskQueue = getTaskQueue(TemporalJobType.SYNC)

    val childSync =
      Workflow.newChildWorkflowStub(
        SyncWorkflowV2::class.java,
        ChildWorkflowOptions
          .newBuilder()
          .setWorkflowId("sync_" + workflowInternalState.jobId)
          .setTaskQueue(taskQueue) // This will cancel the child workflow when the parent is terminated
          .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL)
          .build(),
      )

    return childSync.run(
      SyncWorkflowV2Input(
        connectionId,
        jobId,
        attemptNumber.toLong(),
        sourceId,
      ),
    )
  }

  private fun runCheckInChildWorkflow(
    jobRunConfig: JobRunConfig,
    launcherConfig: IntegrationLauncherConfig,
    checkInput: StandardCheckConnectionInput,
  ): ConnectorJobOutput {
    val workflowId = "check_" + workflowInternalState.jobId + "_" + checkInput.actorType.value()
    val taskQueue = getTaskQueue(TemporalJobType.SYNC)
    val childCheck =
      Workflow.newChildWorkflowStub(
        ConnectorCommandWorkflow::class.java,
        ChildWorkflowOptions
          .newBuilder()
          .setWorkflowId(workflowId)
          .setTaskQueue(taskQueue) // This will cancel the child workflow when the parent is terminated
          .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL)
          .build(),
      )

    log.info("Running legacy check for {} with id {}", checkInput.actorType, checkInput.actorId)
    return childCheck.run(CheckCommandInput(CheckCommandInput.CheckConnectionInput(jobRunConfig, launcherConfig, checkInput)))
  }

  private fun runCheckWithCommandApiInChildWorkflow(
    actorId: UUID,
    jobId: String,
    attemptId: Long,
    actorType: String?,
  ): ConnectorJobOutput {
    val workflowId = "check_" + jobId + "_" + actorType
    val taskQueue = getTaskQueue(TemporalJobType.SYNC)
    val childCheck =
      Workflow.newChildWorkflowStub(
        ConnectorCommandWorkflow::class.java,
        ChildWorkflowOptions
          .newBuilder()
          .setWorkflowId(workflowId)
          .setTaskQueue(taskQueue) // This will cancel the child workflow when the parent is terminated
          .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_REQUEST_CANCEL)
          .build(),
      )

    log.info("Running command check for {} with id {} with the use of the new command API", actorType, actorId)
    return childCheck.run(
      CheckCommandApiInput(
        CheckCommandApiInput.CheckConnectionApiInput(
          actorId,
          jobId,
          attemptId,
        ),
      ),
    )
  }

  /**
   * Set the internal status as failed and save the failures reasons.
   *
   * @return true if the job failed, false otherwise
   */
  private fun getFailStatus(standardSyncOutput: StandardSyncOutput): Boolean {
    val standardSyncSummary = standardSyncOutput.standardSyncSummary

    if (standardSyncSummary != null && standardSyncSummary.status == StandardSyncSummary.ReplicationStatus.FAILED) {
      workflowInternalState.failures.addAll(standardSyncOutput.failures)
      val recordsCommitted =
        if (standardSyncSummary.totalStats != null) standardSyncSummary.totalStats.recordsCommitted else null
      workflowInternalState.partialSuccess = recordsCommitted != null && recordsCommitted > 0
      return true
    }

    return false
  }

  /**
   * Extracts whether the job was cancelled from the output.
   *
   * @return true if the job was cancelled, false otherwise
   */
  private fun getCancelledStatus(standardSyncOutput: StandardSyncOutput): Boolean {
    val summary = standardSyncOutput.standardSyncSummary
    return summary != null && summary.status == StandardSyncSummary.ReplicationStatus.CANCELLED
  }

    /*
     * Set a job as cancel and continue to the next job if and continue as a reset if needed
     */
  private fun reportCancelledAndContinueWith(
    skipSchedulingNextRun: Boolean,
    connectionUpdaterInput: ConnectionUpdaterInput,
  ) {
    if (workflowInternalState.jobId != null && workflowInternalState.attemptNumber != null) {
      reportCancelled(connectionUpdaterInput.connectionId)
    }
    resetNewConnectionInput(connectionUpdaterInput)
    connectionUpdaterInput.skipScheduling = skipSchedulingNextRun
    prepareForNextRunAndContinueAsNew(connectionUpdaterInput)
  }

  private fun reportCancelled(connectionId: UUID?) {
    val jobId = workflowInternalState.jobId
    val attemptNumber = workflowInternalState.attemptNumber
    val failures: MutableSet<FailureReason> = workflowInternalState.failures
    val partialSuccess = workflowInternalState.partialSuccess

    runMandatoryActivity<JobCancelledInputWithAttemptNumber?>(
      { input: JobCancelledInputWithAttemptNumber? -> jobCreationAndStatusUpdateActivity!!.jobCancelledWithAttemptNumber(input!!) },
      JobCancelledInputWithAttemptNumber(
        jobId,
        attemptNumber,
        connectionId,
        FailureHelper.failureSummaryForCancellation(jobId, attemptNumber, failures, partialSuccess),
      ),
    )
  }

  private fun deleteResetJobStreams() {
    runMandatoryActivity<DeleteStreamResetRecordsForJobInput?>(
      { input: DeleteStreamResetRecordsForJobInput? -> streamResetActivity!!.deleteStreamResetRecordsForJob(input!!) },
      DeleteStreamResetRecordsForJobInput(connectionId, workflowInternalState.jobId),
    )
  }

  private val workflowRestartDelaySeconds: Duration
    get() = workflowConfigActivity!!.getWorkflowRestartDelaySeconds()

  private fun traceConnectionId() {
    if (connectionId != null) {
      ApmTraceUtils.addTagsToTrace(Map.of<String?, UUID?>(CONNECTION_ID_KEY, connectionId))
    }
  }

  private fun setConnectionId(connectionUpdaterInput: ConnectionUpdaterInput) {
    connectionId = Objects.requireNonNull<UUID?>(connectionUpdaterInput.connectionId)
    ApmTraceUtils.addTagsToTrace(Map.of<String?, UUID?>(CONNECTION_ID_KEY, connectionId))
  }

  private fun setConnectionContext(ctx: ConnectionContext?) {
    connectionContext = Objects.requireNonNull<ConnectionContext>(ctx)
  }

  private fun useAttemptCountRetries(): Boolean {
    // the manager can be null if
    // - the activity failed unexpectedly
    // in which case we fall back to simple attempt count retries (attempt count < 3)
    return retryManager == null
  }

  private fun resolveBackoff(): Duration {
    if (useAttemptCountRetries()) {
      return Duration.ZERO
    }

    val backoff = retryManager!!.backoff

    runAppendToAttemptLogActivity(String.format("Backing off for: %s.", retryManager!!.backoffString), AppendToAttemptLogActivity.LogLevel.WARN)

    return backoff
  }

  private fun hydrateRetryManager(): RetryManager? {
    val result =
      runActivityWithFallback<HydrateInput?, HydrateOutput>(
        { input: HydrateInput? -> retryStatePersistenceActivity?.hydrateRetryState(input!!) },
        HydrateInput(workflowInternalState.jobId, connectionId),
        HydrateOutput(null),
        RetryStatePersistenceActivity::class.java.getName(),
        "hydrateRetryState",
      )!!

    return result.manager
  }

  private fun accumulateFailureAndPersist(madeProgress: Boolean) {
    if (useAttemptCountRetries()) {
      return
    }

    retryManager!!.incrementFailure(madeProgress)

    runActivityWithFallback<PersistInput?, PersistOutput?>(
      { input: PersistInput? -> retryStatePersistenceActivity!!.persistRetryState(input!!) },
      PersistInput(workflowInternalState.jobId, connectionId, retryManager),
      PersistOutput(false),
      RetryStatePersistenceActivity::class.java.getName(),
      "persistRetryState",
    )

    runAppendToAttemptLogActivity(
      String.format("Retry State: %s\n Backoff before next attempt: %s", retryManager, retryManager!!.backoffString),
      AppendToAttemptLogActivity.LogLevel.INFO,
    )
  }

  private fun logActivityFailure(
    className: String?,
    methodName: String?,
  ) {
    log.error(
      String.format(
        "FAILED %s.%s for connection id: %s, job id: %d, attempt: %d",
        className,
        methodName,
        connectionId,
        workflowInternalState.jobId,
        workflowInternalState.attemptNumber,
      ),
    )
  }

  private fun recordActivityFailure(
    className: String,
    methodName: String,
  ) {
    val attrs: Array<MetricAttribute> =
      arrayOf(
        MetricAttribute(MetricTags.ACTIVITY_NAME, className),
        MetricAttribute(MetricTags.ACTIVITY_METHOD, methodName),
      )
    val inputCtx = ConnectionUpdaterInput(connectionId, workflowInternalState.jobId)

    logActivityFailure(className, methodName)
    tryRecordCountMetric(
      RecordMetricInput(
        inputCtx,
        Optional.empty<FailureCause>(),
        OssMetricsRegistry.ACTIVITY_FAILURE,
        attrs,
      ),
    )
  }

  private fun hydrateIdsFromPreviousRun(
    input: ConnectionUpdaterInput,
    state: WorkflowInternalState,
  ) {
    // connection updater input attempt number starts at 1 instead of 0
    // TODO: this check can be removed once that is fixed
    if (input.attemptNumber != null) {
      state.attemptNumber = input.attemptNumber!! - 1
    }

    state.jobId = input.jobId
  }

  private fun initializeWorkflowStateFromInput(input: ConnectionUpdaterInput) {
    // if our previous attempt was a failure, we are still in a run
    if (input.fromFailure) {
      workflowState.isRunning = true
    }
    // workflow state is only ever set in test cases. for production cases, it will always be null.
    if (input.workflowState != null) {
      // only copy over state change listener and ID to avoid trampling functionality
      workflowState.id = input.workflowState!!.id
      workflowState.stateChangedListener = input.workflowState!!.stateChangedListener
    }

    hydrateIdsFromPreviousRun(input, workflowInternalState)
  }

  private fun runAppendToAttemptLogActivity(
    logMsg: String?,
    level: AppendToAttemptLogActivity.LogLevel?,
  ): Boolean {
    val result =
      runActivityWithFallback(
        { input: LogInput? -> appendToAttemptLogActivity?.log(input!!) },
        LogInput(workflowInternalState.jobId, workflowInternalState.attemptNumber, logMsg, level),
        LogOutput(false),
        AppendToAttemptLogActivity::class.java.getName(),
        "log",
      )!!

    return result.success!!
  }

  /**
   * When you want to run an activity with a fallback value instead of failing the run.
   */
  private fun <T, U> runActivityWithFallback(
    activityMethod: Function<T?, U?>,
    input: T?,
    defaultVal: U?,
    className: String,
    methodName: String,
  ): U? {
    var result = defaultVal

    try {
      result = activityMethod.apply(input)
    } catch (e: Exception) {
      log.error(e.message)
      recordActivityFailure(className, methodName)
    }

    return result
  }

  private fun cancelSyncChildWorkflow() {
    if (cancellableSyncWorkflow != null) {
      cancellableSyncWorkflow!!.cancel()
    }
  }

  private fun getWorkflowDelay(): Duration? {
    if (workflowDelay != null) {
      return workflowDelay
    } else {
      return Duration.ofSeconds(600L)
    }
  }

  companion object {
    private val log: Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    private const val GENERATE_CHECK_INPUT_TAG = "generate_check_input"
    private const val GENERATE_CHECK_INPUT_CURRENT_VERSION = 1
    private const val CHECK_WORKSPACE_TOMBSTONE_TAG = "check_workspace_tombstone"
    private const val CHECK_WORKSPACE_TOMBSTONE_CURRENT_VERSION = 1
    private const val LOAD_SHED_BACK_OFF_TAG = "load_shed_back_off"
    private const val LOAD_SHED_BACK_OFF_CURRENT_VERSION = 1
    private const val PASS_DEST_REQS_TO_CHECK_TAG = "pass_dest_reqs_to_check"
    private const val PASS_DEST_REQS_TO_CHECK_CURRENT_VERSION = 1

    private const val GET_FEATURE_FLAGS_TAG = "get_feature_flags"
    private const val GET_FEATURE_FLAGS_CURRENT_VERSION = 1

    private const val CHECK_USING_COMMAND_API_TAG = "check_using_command_api"
    private const val CHECK_USING_COMMAND_API_VERSION = 1

    private const val USE_SYNC_WORKFLOW_V2_TAG = "use_sync_workflow_v2"
    private const val USE_SYNC_WORKFLOW_V2_VERSION = 1
  }
}
