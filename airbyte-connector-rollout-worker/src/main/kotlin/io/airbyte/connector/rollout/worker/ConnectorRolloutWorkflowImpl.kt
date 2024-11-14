/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker

import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.connector.rollout.shared.Constants
import io.airbyte.connector.rollout.shared.models.ActionType
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputCleanup
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFind
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputGet
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPromoteOrRollback
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputStart
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputVerifyDefaultVersion
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.airbyte.connector.rollout.worker.activities.CleanupActivity
import io.airbyte.connector.rollout.worker.activities.DoRolloutActivity
import io.airbyte.connector.rollout.worker.activities.FinalizeRolloutActivity
import io.airbyte.connector.rollout.worker.activities.FindRolloutActivity
import io.airbyte.connector.rollout.worker.activities.GetRolloutActivity
import io.airbyte.connector.rollout.worker.activities.PromoteOrRollbackActivity
import io.airbyte.connector.rollout.worker.activities.StartRolloutActivity
import io.airbyte.connector.rollout.worker.activities.VerifyDefaultVersionActivity
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.failure.ApplicationFailure
import io.temporal.workflow.Workflow
import java.lang.reflect.Field
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
          .setMaximumAttempts(1)
          .setDoNotRetry("org.openapitools.client.infrastructure.ClientException")
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
    defaultActivityOptions.toBuilder()
      .setHeartbeatTimeout(
        Duration.ofSeconds(Constants.VERIFY_ACTIVITY_HEARTBEAT_TIMEOUT_SECONDS.toLong()),
      )
      .setStartToCloseTimeout(
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

  private var startRolloutFailed = false
  private var connectorRollout: ConnectorRolloutOutput? = null

  override fun run(input: ConnectorRolloutActivityInputStart): ConnectorEnumRolloutState {
    val workflowId = Workflow.getInfo().workflowId

    // Checkpoint to record the workflow version
    Workflow.getVersion("ChangedActivityInputStart", Workflow.DEFAULT_VERSION, 1)

    setRollout(input)

    // End the workflow if we were unable to start the rollout, or we've reached a terminal state
    Workflow.await { startRolloutFailed || rolloutStateIsTerminal() }
    if (startRolloutFailed) {
      throw ApplicationFailure.newFailure(
        "Failure starting rollout for $workflowId",
        ConnectorEnumRolloutState.CANCELED.value(),
      )
    }
    logger.info { "Rollout for $workflowId has reached a terminal state: ${connectorRollout?.state}" }

    return getRolloutState()
  }

  private fun setRollout(input: ConnectorRolloutActivityInputStart) {
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
        initialRolloutPct = input.connectorRollout?.initialRolloutPct?.toInt(),
        currentTargetRolloutPct = input.connectorRollout?.currentTargetRolloutPct?.toInt(),
        finalTargetRolloutPct = input.connectorRollout?.finalTargetRolloutPct?.toInt(),
        hasBreakingChanges = false,
        rolloutStrategy = input.connectorRollout?.rolloutStrategy,
        maxStepWaitTimeMins = input.connectorRollout?.maxStepWaitTimeMins?.toInt(),
        updatedBy = input.connectorRollout?.updatedBy.toString(),
        createdAt = getOffset(input.connectorRollout?.createdAt),
        updatedAt = getOffset(input.connectorRollout?.updatedAt),
        completedAt = getOffset(input.connectorRollout?.completedAt),
        expiresAt = getOffset(input.connectorRollout?.expiresAt),
        errorMsg = input.connectorRollout?.errorMsg,
        failedReason = input.connectorRollout?.failedReason,
      )
  }

  private fun getOffset(timestamp: Long?): OffsetDateTime? {
    return if (timestamp == null) {
      null
    } else {
      Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC)
    }
  }

  private fun getRolloutState(): ConnectorEnumRolloutState {
    return connectorRollout?.state ?: ConnectorEnumRolloutState.INITIALIZED
  }

  private fun rolloutStateIsTerminal(): Boolean {
    return ConnectorRolloutFinalState.entries.any { it.value() == getRolloutState().value() }
  }

  override fun startRollout(input: ConnectorRolloutActivityInputStart): ConnectorRolloutOutput {
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
      startRolloutFailed = true
      throw e
    }
  }

  override fun startRolloutValidator(input: ConnectorRolloutActivityInputStart) {
    logger.info { "startRolloutValidator: ${input.dockerRepository}:${input.dockerImageTag}" }
    require(!(input.dockerRepository == null || input.dockerImageTag == null || input.actorDefinitionId == null || input.rolloutId == null)) {
      "Cannot start rollout; invalid input: ${mapAttributesToString(input)}"
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
    require(!(input.dockerRepository == null || input.dockerImageTag == null || input.actorDefinitionId == null)) {
      "Cannot find rollout; invalid input: ${mapAttributesToString(input)}"
    }
  }

  override fun getRollout(input: ConnectorRolloutActivityInputGet): ConnectorRolloutOutput {
    logger.info { "getRollout: calling getRolloutActivity" }
    val output = getRolloutActivity.getRollout(input)
    logger.info { "getRolloutActivity.getRollout = $output" }
    return output
  }

  override fun getRolloutValidator(input: ConnectorRolloutActivityInputGet) {
    logger.info { "getRolloutValidator: ${input.dockerRepository}:${input.dockerImageTag}" }
    require(!(input.dockerRepository == null || input.dockerImageTag == null || input.actorDefinitionId == null || input.rolloutId == null)) {
      "Cannot get rollout; invalid input: ${mapAttributesToString(input)}"
    }
  }

  override fun doRollout(input: ConnectorRolloutActivityInputRollout): ConnectorRolloutOutput {
    logger.info { "doRollout: calling doRolloutActivity" }
    val output = doRolloutActivity.doRollout(input)
    connectorRollout = output
    logger.info { "doRolloutActivity.doRollout = $output" }
    return output
  }

  override fun doRolloutValidator(input: ConnectorRolloutActivityInputRollout) {
    logger.info { "doRolloutValidator: ${input.dockerRepository}:${input.dockerImageTag}" }
    require(!(input.dockerRepository == null || input.dockerImageTag == null || input.actorDefinitionId == null || input.rolloutId == null)) {
      "Cannot do rollout; invalid input: ${mapAttributesToString(input)}"
    }
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

    // Unpin all actors that were pinned to the release candidate
    logger.info { "finalizeRollout: calling finalizeRolloutActivity" }
    val rolloutResult = finalizeRolloutActivity.finalizeRollout(input)
    logger.info { "finalizeRolloutActivity.finalizeRollout rolloutResult = $rolloutResult" }
    connectorRollout = rolloutResult
    return rolloutResult
  }

  override fun finalizeRolloutValidator(input: ConnectorRolloutActivityInputFinalize) {
    logger.info { "finalizeRolloutValidator: ${input.dockerRepository}:${input.dockerImageTag}" }
    require(
      !(
        input.dockerRepository == null ||
          input.dockerImageTag == null ||
          input.actorDefinitionId == null ||
          input.rolloutId == null ||
          input.result == null
      ),
    ) {
      "Cannot do rollout; invalid input: ${mapAttributesToString(input)}"
    }
  }

  companion object {
    fun mapAttributesToString(obj: Any): String {
      val result = StringBuilder()
      val fields: Array<Field> = obj.javaClass.declaredFields
      for (field in fields) {
        try {
          val value = field.get(obj)
          result
            .append("${field.name}=")
            .append(value?.toString() ?: "null")
            .append(" ")
        } catch (e: IllegalAccessException) {
          logger.error(e) { "Error mapping attributes to string: ${e.message}" }
        }
      }
      return result.toString().trim()
    }
  }
}
