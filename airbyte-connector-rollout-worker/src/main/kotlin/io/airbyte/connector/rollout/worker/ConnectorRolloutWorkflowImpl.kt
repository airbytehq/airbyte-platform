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

private val logger = KotlinLogging.logger {}

class ConnectorRolloutWorkflowImpl : ConnectorRolloutWorkflow {
  private val defaultActivityOptions =
    ActivityOptions
      .newBuilder()
      .setStartToCloseTimeout(Duration.ofSeconds(10))
      .setRetryOptions(
        RetryOptions
          .newBuilder()
          .setMaximumInterval(Duration.ofSeconds(20))
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
    defaultActivityOptions
      .toBuilder()
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
  private var state = ConnectorEnumRolloutState.INITIALIZED

  override fun run(input: ConnectorRolloutActivityInputStart): ConnectorEnumRolloutState {
    val workflowId = "${input.dockerRepository}:${input.dockerImageTag}:${input.actorDefinitionId.toString().substring(0, 8)}"
    logger.info { "Initialized rollout for $workflowId" }
    // End the workflow if we were unable to start the rollout, or we've reached a terminal state
    Workflow.await { startRolloutFailed || ConnectorRolloutFinalState.entries.any { it.value() == state.value() } }
    if (startRolloutFailed) {
      throw ApplicationFailure.newFailure(
        "Failure starting rollout for $workflowId",
        ConnectorEnumRolloutState.CANCELED_ROLLED_BACK.value(),
      )
    }
    logger.info { "Rollout for $workflowId has reached a terminal state: $state" }
    return state
  }

  override fun startRollout(input: ConnectorRolloutActivityInputStart): ConnectorRolloutOutput {
    logger.info { "startRollout: calling startRolloutActivity" }
    val workflowRunId = Workflow.getInfo().firstExecutionRunId
    return try {
      val output = startRolloutActivity.startRollout(workflowRunId, input)
      logger.info { "startRolloutActivity.startRollout" }
      state = output.state
      output
    } catch (e: Exception) {
      val newState = ConnectorEnumRolloutState.CANCELED_ROLLED_BACK
      cleanupActivity.cleanup(
        ConnectorRolloutActivityInputCleanup(
          newState = newState,
          dockerRepository = input.dockerRepository,
          dockerImageTag = input.dockerImageTag,
          actorDefinitionId = input.actorDefinitionId,
          rolloutId = input.rolloutId,
          errorMsg = "Failed to start rollout.",
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
    logger.info { "getRolloutActivity.getRollout pinned_actors = ${output.actorIds}" }
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
    state = output.state
    logger.info { "doRolloutActivity.doRollout pinned_connections = ${output.actorIds}" }
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
      if (state == ConnectorEnumRolloutState.FINALIZING) {
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
            ),
          )
        state = output.state
      }
    }

    // Verify the release candidate is the default version
    if (input.result == ConnectorRolloutFinalState.SUCCEEDED) {
      logger.info { "finalizeRollout: calling verifyDefaultVersionActivity" }
      verifyDefaultVersionActivity.verifyDefaultVersion(
        ConnectorRolloutActivityInputVerifyDefaultVersion(
          dockerRepository = input.dockerRepository,
          dockerImageTag = input.dockerImageTag,
          actorDefinitionId = input.actorDefinitionId,
          rolloutId = input.rolloutId,
        ),
      )
    }

    // Unpin all actors that were pinned to the release candidate
    logger.info { "finalizeRollout: calling finalizeRolloutActivity" }
    val rolloutResult = finalizeRolloutActivity.finalizeRollout(input)
    logger.info { "finalizeRolloutActivity.finalizeRollout rolloutResult = $rolloutResult" }
    state = rolloutResult.state
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
