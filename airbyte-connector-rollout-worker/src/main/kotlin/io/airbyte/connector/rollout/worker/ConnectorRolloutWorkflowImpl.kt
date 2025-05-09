/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker

import com.google.common.annotations.VisibleForTesting
import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.connector.rollout.shared.Constants
import io.airbyte.connector.rollout.shared.Decision
import io.airbyte.connector.rollout.shared.RolloutProgressionDecider
import io.airbyte.connector.rollout.shared.models.ActionType
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputCleanup
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFind
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputGet
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPause
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPromoteOrRollback
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputStart
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputVerifyDefaultVersion
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutWorkflowInput
import io.airbyte.connector.rollout.worker.activities.CleanupActivity
import io.airbyte.connector.rollout.worker.activities.DoRolloutActivity
import io.airbyte.connector.rollout.worker.activities.FinalizeRolloutActivity
import io.airbyte.connector.rollout.worker.activities.FindRolloutActivity
import io.airbyte.connector.rollout.worker.activities.GetRolloutActivity
import io.airbyte.connector.rollout.worker.activities.PauseRolloutActivity
import io.airbyte.connector.rollout.worker.activities.PromoteOrRollbackActivity
import io.airbyte.connector.rollout.worker.activities.StartRolloutActivity
import io.airbyte.connector.rollout.worker.activities.VerifyDefaultVersionActivity
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.failure.ApplicationFailure
import io.temporal.workflow.Workflow
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

private val logger = KotlinLogging.logger {}

class ConnectorRolloutWorkflowImpl : ConnectorRolloutWorkflow {
  private val defaultActivityOptions =
    ActivityOptions
      .newBuilder()
      .setStartToCloseTimeout(Duration.ofSeconds(600))
      .setRetryOptions(
        RetryOptions
          .newBuilder()
          .setMaximumInterval(Duration.ofSeconds(600))
          .setMaximumAttempts(6)
          .build(),
      ).build()

  private val startRolloutActivity =
    Workflow.newActivityStub(
      StartRolloutActivity::class.java,
      defaultActivityOptions,
    )

  private val findRolloutActivity =
    Workflow.newActivityStub(
      FindRolloutActivity::class.java,
      defaultActivityOptions,
    )

  private val getRolloutActivity =
    Workflow.newActivityStub(
      GetRolloutActivity::class.java,
      defaultActivityOptions,
    )

  private val doRolloutActivity =
    Workflow.newActivityStub(
      DoRolloutActivity::class.java,
      defaultActivityOptions,
    )

  private val finalizeRolloutActivity =
    Workflow.newActivityStub(
      FinalizeRolloutActivity::class.java,
      defaultActivityOptions,
    )

  private val promoteOrRollbackActivity =
    Workflow.newActivityStub(
      PromoteOrRollbackActivity::class.java,
      defaultActivityOptions,
    )

  private val verifyActivityOptions =
    defaultActivityOptions
      .toBuilder()
      .setHeartbeatTimeout(
        Duration.ofSeconds(Constants.VERIFY_ACTIVITY_HEARTBEAT_TIMEOUT_SECONDS.toLong()),
      ).setStartToCloseTimeout(
        Duration.ofSeconds((Constants.VERIFY_ACTIVITY_TIMEOUT_MILLIS / 1000).toLong()),
      ).build()

  private val verifyDefaultVersionActivity =
    Workflow.newActivityStub(
      VerifyDefaultVersionActivity::class.java,
      verifyActivityOptions,
    )

  private val cleanupActivity =
    Workflow.newActivityStub(
      CleanupActivity::class.java,
      defaultActivityOptions,
    )

  private val pauseRolloutActivity =
    Workflow.newActivityStub(
      PauseRolloutActivity::class.java,
      defaultActivityOptions,
    )

  private var isPaused = false
  private var workflowId: String? = null
  private var connectorRollout: ConnectorRolloutOutput? = null

  override fun run(input: ConnectorRolloutWorkflowInput): ConnectorEnumRolloutState {
    workflowId = Workflow.getInfo().workflowId

    // Checkpoint to record the workflow version
    Workflow.getVersion("ChangedActivityInputStart", Workflow.DEFAULT_VERSION, 1)

    // Set the rollout's initial state
    setRollout(input)

    startRollout(
      ConnectorRolloutActivityInputStart(
        input.dockerRepository,
        input.dockerImageTag,
        input.actorDefinitionId,
        input.rolloutId,
        input.updatedBy,
        input.rolloutStrategy,
        input.migratePins,
      ),
    )

    return if (input.rolloutStrategy == ConnectorEnumRolloutStrategy.AUTOMATED) {
      logger.info { "Running an automated workflow for $workflowId" }
      try {
        runAutomated(workflowId!!, input)
      } catch (e: Exception) {
        connectorRollout =
          pauseRollout(
            ConnectorRolloutActivityInputPause(
              input.dockerRepository,
              input.dockerImageTag,
              input.actorDefinitionId,
              input.rolloutId,
              "Paused due to an exception in the automated rollout: ${e.message}",
              null,
              ConnectorEnumRolloutStrategy.AUTOMATED,
            ),
          )
        Workflow.await { rolloutStateIsTerminal() }
        getRolloutState()
      }
    } else {
      runManual(workflowId!!)
    }
  }

  @VisibleForTesting
  fun runManual(workflowId: String): ConnectorEnumRolloutState {
    // End the workflow if we were unable to start the rollout, or we've reached a terminal state
    Workflow.await { rolloutStateIsTerminal() }
    logger.info { "Rollout for $workflowId has reached a terminal state: ${connectorRollout?.state}" }

    return getRolloutState()
  }

  @VisibleForTesting
  fun runAutomated(
    workflowId: String,
    input: ConnectorRolloutWorkflowInput,
  ): ConnectorEnumRolloutState {
    val rollout = getRollout(ConnectorRolloutActivityInputGet(input.dockerRepository, input.dockerImageTag, input.actorDefinitionId, input.rolloutId))

    var nextRolloutStageAt = getCurrentTimeMilli()
    val expirationTime = nextRolloutStageAt.plusSeconds((input.rolloutExpirationSeconds ?: Constants.DEFAULT_ROLLOUT_EXPIRATION_SECONDS).toLong())

    val waitBetweenRolloutsSeconds = (input.waitBetweenRolloutSeconds ?: Constants.DEFAULT_WAIT_BETWEEN_ROLLOUTS_SECONDS)
    val waitBetweenResultPollsSeconds = (input.waitBetweenSyncResultsQueriesSeconds ?: Constants.DEFAULT_WAIT_BETWEEN_SYNC_RESULTS_QUERIES_SECONDS)
    val stepSizePercentage = rollout.initialRolloutPct ?: Constants.DEFAULT_INITIAL_ROLLOUT_PERCENTAGE

    // Continuously manage the rollout until we reach a terminal state, the workflow is paused, or the rollout expires.
    // The loop performs the following steps:
    // 1. If it's time to advance the rollout (based on `nextRolloutTime`), increase the rollout percentage and attempt
    //    to apply it. If this fails, pause the workflow.
    // 2. Wait (`Workflow.sleep`) to give time for sync results to become available, then fetch the latest rollout state.
    // 3. Use the fetched state to make a decision (`Decision`), which may include finalizing or pausing the rollout, or
    //    waiting for more data.
    // When the rollout is paused, manual intervention is required to finalize the workflow.
    while (!rolloutStateIsTerminal() && !isPaused && (Workflow.currentTimeMillis() < expirationTime.toEpochMilli())) {
      val currentTime = getCurrentTimeMilli()
      val targetPercentageToPin = minOf((connectorRollout!!.currentTargetRolloutPct ?: 0) + stepSizePercentage, 100)
      if (currentTime >= nextRolloutStageAt) {
        // In the first iteration, we always progress the rollout.
        // After that, we wait `waitBetweenRolloutsSeconds` before pinning more actors.
        // `waitBetweenRolloutsSeconds` should be longer than `waitBetweenResultPollsSeconds`, allowing us to get the
        // sync status more often than we pin new actors.
        logger.info {
          "Advancing rolloutId = ${rollout.id} " +
            " targetPercentage=$targetPercentageToPin" +
            " rolloutStrategy = ${rollout.rolloutStrategy}"
        }
        nextRolloutStageAt = currentTime.plusSeconds(waitBetweenRolloutsSeconds.toLong())
        try {
          connectorRollout =
            progressRollout(
              ConnectorRolloutActivityInputRollout(
                dockerRepository = input.dockerRepository,
                dockerImageTag = input.dockerImageTag,
                actorDefinitionId = input.actorDefinitionId,
                rolloutId = input.rolloutId,
                actorIds = null,
                // We let the rollout handler validate that the targetPercentage is within bounds
                targetPercentage = targetPercentageToPin,
                rolloutStrategy = ConnectorEnumRolloutStrategy.AUTOMATED,
              ),
            )
        } catch (e: Exception) {
          logger.error { "Failed to advance the rollout. workflowId=$workflowId e=${e.message} e.cause=${e.cause} e=$e" }
          connectorRollout =
            pauseRollout(
              ConnectorRolloutActivityInputPause(
                input.dockerRepository,
                input.dockerImageTag,
                input.actorDefinitionId,
                input.rolloutId,
                "Paused due to an exception while pinning connections: $e",
                null,
                ConnectorEnumRolloutStrategy.AUTOMATED,
              ),
            )
          break
        }
      }

      if (!isPaused) {
        logger.info { "Waiting for data. workflowId=$workflowId" }
        val exitedEarly =
          Workflow.await(Duration.ofSeconds(waitBetweenResultPollsSeconds.toLong())) {
            rolloutStateIsTerminal() ||
              isPaused ||
              (connectorRollout != null && connectorRollout!!.state == ConnectorEnumRolloutState.FINALIZING)
          }
        if (exitedEarly) {
          logger.info { "Workflow is paused, finalizing, or complete. workflowId=$workflowId state=${connectorRollout!!.state} isPaused=$isPaused" }
          break
        }
        logger.info { "Fetching data. workflowId=$workflowId" }

        connectorRollout =
          getRolloutActivity.getRollout(
            ConnectorRolloutActivityInputGet(
              dockerRepository = input.dockerRepository,
              dockerImageTag = input.dockerImageTag,
              actorDefinitionId = input.actorDefinitionId,
              rolloutId = input.rolloutId,
            ),
          )

        val decision = RolloutProgressionDecider().decide(connectorRollout!!)
        logger.info { "Fetched data. connectorRollout=$connectorRollout decision=${decision.name} workflowId=$workflowId" }
        doNext(decision, workflowId, input)
        if (decision == Decision.RELEASE) {
          return getRolloutState()
        }
      }
    }

    if (!rolloutStateIsTerminal() && !isPaused && Workflow.currentTimeMillis() >= expirationTime.toEpochMilli()) {
      logger.info { "Rollout expiration time reached. Pausing rollout. workflowId=$workflowId" }
      connectorRollout =
        pauseRollout(
          ConnectorRolloutActivityInputPause(
            dockerRepository = input.dockerRepository,
            dockerImageTag = input.dockerImageTag,
            actorDefinitionId = input.actorDefinitionId,
            rolloutId = input.rolloutId,
            pausedReason = "Rollout expired without reaching a terminal state.",
            rolloutStrategy = ConnectorEnumRolloutStrategy.AUTOMATED,
          ),
        )
    }

    // We've timed out waiting for results or the rollout is paused. At this point we require a dev to manually finalize the rollout.
    Workflow.await { rolloutStateIsTerminal() }
    return getRolloutState()
  }

  private fun getCurrentTimeMilli(): Instant = Instant.ofEpochMilli(Workflow.currentTimeMillis())

  private fun doNext(
    decision: Decision,
    workflowId: String,
    input: ConnectorRolloutWorkflowInput,
  ) {
    when (decision) {
      Decision.RELEASE -> {
        logger.info { "Finalizing rollout with ${ConnectorRolloutFinalState.SUCCEEDED}. workflowId=$workflowId" }
        connectorRollout =
          finalizeRollout(
            ConnectorRolloutActivityInputFinalize(
              dockerRepository = input.dockerRepository,
              dockerImageTag = input.dockerImageTag,
              actorDefinitionId = input.actorDefinitionId,
              rolloutId = input.rolloutId,
              result = ConnectorRolloutFinalState.SUCCEEDED,
              previousVersionDockerImageTag = input.initialVersionDockerImageTag!!,
              rolloutStrategy = ConnectorEnumRolloutStrategy.AUTOMATED,
            ),
          )
      }
      Decision.ROLLBACK -> {
        logger.info { "Finalizing rollout with ${ConnectorRolloutFinalState.FAILED_ROLLED_BACK}. workflowId=$workflowId" }
        connectorRollout =
          finalizeRollout(
            ConnectorRolloutActivityInputFinalize(
              dockerRepository = input.dockerRepository,
              dockerImageTag = input.dockerImageTag,
              actorDefinitionId = input.actorDefinitionId,
              rolloutId = input.rolloutId,
              result = ConnectorRolloutFinalState.FAILED_ROLLED_BACK,
              previousVersionDockerImageTag = input.initialVersionDockerImageTag!!,
              failedReason = "Rolled back due to failed syncs for the release candidate version",
              rolloutStrategy = ConnectorEnumRolloutStrategy.AUTOMATED,
            ),
          )
      }
      Decision.INSUFFICIENT_DATA -> {
        logger.info { "Not enough data to decide. workflowId=$workflowId" }
      }
      Decision.PAUSE -> {
        connectorRollout =
          pauseRollout(
            ConnectorRolloutActivityInputPause(
              input.dockerRepository,
              input.dockerImageTag,
              input.actorDefinitionId,
              input.rolloutId,
              "Paused due to insufficient successful syncs in the allotted time.",
              null,
              ConnectorEnumRolloutStrategy.AUTOMATED,
            ),
          )
        logger.info { "Pausing the workflow. workflowId=$workflowId" }
      }
    }
  }

  private fun setRollout(input: ConnectorRolloutWorkflowInput) {
    connectorRollout =
      ConnectorRolloutOutput(
        id = input.connectorRollout?.id,
        workflowRunId = input.connectorRollout?.workflowRunId,
        actorDefinitionId = input.connectorRollout?.actorDefinitionId,
        releaseCandidateVersionId = input.connectorRollout?.releaseCandidateVersionId,
        initialVersionId = input.connectorRollout?.initialVersionId,
        // In Workflow.DEFAULT_VERSION, input.connectorRollout doesn't exist, and we only store the `state` variable.
        // Therefore, we require it to be non-null here.
        // Once all DEFAULT_VERSION workflows are finished, we can delete the null branch.
        state = input.connectorRollout?.state ?: ConnectorEnumRolloutState.INITIALIZED,
        initialRolloutPct = input.connectorRollout?.initialRolloutPct,
        currentTargetRolloutPct = input.connectorRollout?.currentTargetRolloutPct,
        finalTargetRolloutPct = input.connectorRollout?.finalTargetRolloutPct,
        hasBreakingChanges = false,
        rolloutStrategy = input.connectorRollout?.rolloutStrategy,
        maxStepWaitTimeMins = input.connectorRollout?.maxStepWaitTimeMins,
        updatedBy = input.connectorRollout?.updatedBy.toString(),
        createdAt = getOffset(input.connectorRollout?.createdAt),
        updatedAt = getOffset(input.connectorRollout?.updatedAt),
        completedAt = getOffset(input.connectorRollout?.completedAt),
        expiresAt = getOffset(input.connectorRollout?.expiresAt),
        errorMsg = input.connectorRollout?.errorMsg,
        failedReason = input.connectorRollout?.failedReason,
        pausedReason = input.connectorRollout?.pausedReason,
        actorSelectionInfo = input.actorSelectionInfo,
        actorSyncs = input.actorSyncs,
      )
  }

  private fun getOffset(timestamp: Long?): OffsetDateTime? =
    if (timestamp == null) {
      null
    } else {
      Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC)
    }

  private fun getRolloutState(): ConnectorEnumRolloutState = connectorRollout?.state ?: ConnectorEnumRolloutState.WORKFLOW_STARTED

  private fun rolloutStateIsTerminal(): Boolean = ConnectorRolloutFinalState.entries.any { it.value() == getRolloutState().value() }

  @VisibleForTesting
  internal fun startRollout(input: ConnectorRolloutActivityInputStart): ConnectorRolloutOutput {
    logger.info { "startRollout: calling startRolloutActivity" }
    val workflowRunId = Workflow.getInfo().firstExecutionRunId
    return try {
      val output = startRolloutActivity.startRollout(workflowRunId, input)
      logger.info { "startRolloutActivity.startRollout" }
      connectorRollout = output
      output
    } catch (e: Exception) {
      val newState = ConnectorEnumRolloutState.CANCELED
      cleanupActivity.cleanup(
        ConnectorRolloutActivityInputCleanup(
          newState = newState,
          dockerRepository = input.dockerRepository,
          dockerImageTag = input.dockerImageTag,
          actorDefinitionId = input.actorDefinitionId,
          rolloutId = input.rolloutId,
          errorMsg = "Failed to start rollout.",
          failureMsg = e.message,
          updatedBy = input.updatedBy,
          rolloutStrategy = input.rolloutStrategy,
        ),
      )
      throw ApplicationFailure.newFailure(
        "Failure starting rollout for $workflowId",
        ConnectorEnumRolloutState.CANCELED.value(),
      )
    }
  }

  override fun findRollout(input: ConnectorRolloutActivityInputFind): List<ConnectorRolloutOutput> {
    logger.info { "getRollout: calling getRolloutActivity" }
    val output = findRolloutActivity.findRollout(input)
    logger.info { "findRolloutActivity.findRollout: $output" }
    return output
  }

  override fun findRolloutValidator(input: ConnectorRolloutActivityInputFind) {
    logger.info { "findRolloutValidator: ${input.dockerRepository}:${input.dockerImageTag}" }
  }

  override fun getRollout(input: ConnectorRolloutActivityInputGet): ConnectorRolloutOutput {
    logger.info { "getRollout: calling getRolloutActivity" }
    val output = getRolloutActivity.getRollout(input)
    logger.info { "getRolloutActivity.getRollout = $output" }
    return output
  }

  override fun getRolloutValidator(input: ConnectorRolloutActivityInputGet) {
    logger.info { "getRolloutValidator: ${input.dockerRepository}:${input.dockerImageTag}" }
  }

  override fun pauseRollout(input: ConnectorRolloutActivityInputPause): ConnectorRolloutOutput {
    logger.info { "pauseRollout: calling doRolloutActivity with input=$input" }
    val output = pauseRolloutActivity.pauseRollout(input)
    isPaused = true
    connectorRollout = output
    logger.info { "pauseRolloutActivity.pauseRollout = $output" }
    return output
  }

  override fun pauseRolloutValidator(input: ConnectorRolloutActivityInputPause) {
    logger.info { "pauseRolloutValidator: ${input.dockerRepository}:${input.dockerImageTag}" }
  }

  override fun progressRollout(input: ConnectorRolloutActivityInputRollout): ConnectorRolloutOutput {
    logger.info { "progressRollout: calling doRolloutActivity with input=$input" }
    val output = doRolloutActivity.doRollout(input)
    connectorRollout = output
    logger.info { "doRolloutActivity.doRollout = $output" }
    return output
  }

  override fun progressRolloutValidator(input: ConnectorRolloutActivityInputRollout) {
    logger.info { "progressRolloutValidator: ${input.dockerRepository}:${input.dockerImageTag}" }
  }

  override fun finalizeRollout(input: ConnectorRolloutActivityInputFinalize): ConnectorRolloutOutput {
    // Start a GH workflow to make the release candidate available as `latest`, if the rollout was successful
    // Delete the release candidate on either success or failure (but not cancellation)
    if (input.result == ConnectorRolloutFinalState.SUCCEEDED || input.result == ConnectorRolloutFinalState.FAILED_ROLLED_BACK) {
      if (connectorRollout?.state == ConnectorEnumRolloutState.FINALIZING) {
        logger.info { "finalizeRollout: already promoted/rolled back, skipping; if you need to re-run the GHA please do so manually " }
      } else {
        logger.info { "finalizeRollout: calling promoteOrRollback" }
        val output =
          promoteOrRollbackActivity.promoteOrRollback(
            ConnectorRolloutActivityInputPromoteOrRollback(
              dockerRepository = input.dockerRepository,
              technicalName = input.dockerRepository.substringAfter("airbyte/"),
              dockerImageTag = input.dockerImageTag,
              action =
                when (input.result) {
                  ConnectorRolloutFinalState.SUCCEEDED -> ActionType.PROMOTE
                  ConnectorRolloutFinalState.FAILED_ROLLED_BACK -> ActionType.ROLLBACK
                  else -> throw IllegalArgumentException("Unrecognized status: $input.result")
                },
              rolloutId = input.rolloutId,
              rolloutStrategy = input.rolloutStrategy,
            ),
          )
        connectorRollout = output
      }
    }

    // Verify the release candidate is the default version
    if (input.result == ConnectorRolloutFinalState.SUCCEEDED) {
      logger.info { "finalizeRollout: calling verifyDefaultVersionActivity" }
      val defaultVersionOutput =
        verifyDefaultVersionActivity.getAndVerifyDefaultVersion(
          ConnectorRolloutActivityInputVerifyDefaultVersion(
            dockerRepository = input.dockerRepository,
            dockerImageTag = input.dockerImageTag,
            actorDefinitionId = input.actorDefinitionId,
            rolloutId = input.rolloutId,
            previousVersionDockerImageTag = input.previousVersionDockerImageTag,
            rolloutStrategy = input.rolloutStrategy,
          ),
        )
      if (!defaultVersionOutput.isReleased) {
        // If the default version is not the release candidate, the rollout was superseded and we consider it canceled
        input.result = ConnectorRolloutFinalState.CANCELED
        input.errorMsg = "Default version is not the release candidate; rollout was superseded"
      }
    }

    // Mark the rollout as finalized, and unpin all actors that were pinned to the release candidate if appropriate
    logger.info { "finalizeRollout: calling finalizeRolloutActivity" }
    val rolloutResult = finalizeRolloutActivity.finalizeRollout(input)
    logger.info { "finalizeRolloutActivity.finalizeRollout rolloutResult = $rolloutResult" }
    connectorRollout = rolloutResult
    return rolloutResult
  }

  override fun finalizeRolloutValidator(input: ConnectorRolloutActivityInputFinalize) {
    logger.info { "finalizeRolloutValidator: ${input.dockerRepository}:${input.dockerImageTag}" }
  }
}
