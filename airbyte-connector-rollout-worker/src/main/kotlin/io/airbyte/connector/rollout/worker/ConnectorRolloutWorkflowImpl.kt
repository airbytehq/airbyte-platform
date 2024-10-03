/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker

import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.connector.rollout.shared.models.ActionType
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFind
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputGet
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPromoteOrRollback
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputStart
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputVerifyDefaultVersion
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
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
  private val activityOptions =
    ActivityOptions.newBuilder()
      .setStartToCloseTimeout(Duration.ofSeconds(10))
      .setRetryOptions(
        RetryOptions.newBuilder()
          .setMaximumInterval(Duration.ofSeconds(20))
          .setMaximumAttempts(1)
          .setDoNotRetry("org.openapitools.client.infrastructure.ClientException")
          .build(),
      )
      .build()

  private val startRolloutActivity =
    Workflow.newActivityStub(
      StartRolloutActivity::class.java,
      activityOptions,
    )

  private val findRolloutActivity =
    Workflow.newActivityStub(
      FindRolloutActivity::class.java,
      activityOptions,
    )

  private val getRolloutActivity =
    Workflow.newActivityStub(
      GetRolloutActivity::class.java,
      activityOptions,
    )

  private val doRolloutActivity =
    Workflow.newActivityStub(
      DoRolloutActivity::class.java,
      activityOptions,
    )

  private val finalizeRolloutActivity =
    Workflow.newActivityStub(
      FinalizeRolloutActivity::class.java,
      activityOptions,
    )

  private val promoteOrRollbackActivity =
    Workflow.newActivityStub(
      PromoteOrRollbackActivity::class.java,
      activityOptions,
    )

  private val verifyDefaultVersionActivity =
    Workflow.newActivityStub(
      VerifyDefaultVersionActivity::class.java,
      activityOptions,
    )

  private var succeeded = false
  private var canceled = false
  private var errored = false
  private var failed = false
  private var startRolloutFailed = false

  override fun run(input: ConnectorRolloutActivityInputStart): ConnectorEnumRolloutState {
    val workflowId = "${input.dockerRepository}:${input.dockerImageTag}:${input.actorDefinitionId.toString().substring(0, 8)}"
    logger.info { "Initialized rollout for $workflowId" }
    Workflow.await { startRolloutFailed || failed || errored || canceled || succeeded }
    return when {
      startRolloutFailed -> throw ApplicationFailure.newFailure(
        "Failure starting rollout for $workflowId",
        ConnectorEnumRolloutState.ERRORED.value(),
      )
      failed -> ConnectorEnumRolloutState.FAILED_ROLLED_BACK
      canceled -> ConnectorEnumRolloutState.CANCELED_ROLLED_BACK
      else -> ConnectorEnumRolloutState.SUCCEEDED
    }
  }

  override fun startRollout(input: ConnectorRolloutActivityInputStart): ConnectorRolloutOutput {
    logger.info { "startRollout: calling startRolloutActivity" }
    val workflowRunId = Workflow.getInfo().firstExecutionRunId
    return try {
      val output = startRolloutActivity.startRollout(workflowRunId, input)
      logger.info { "startRolloutActivity.startRollout" }
      output
    } catch (e: Exception) {
      startRolloutFailed = true
      if (e.cause != null) {
        throw e.cause!!
      } else {
        throw e
      }
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
      logger.info { "finalizeRollout: calling promoteOrRollback" }
      promoteOrRollbackActivity.promoteOrRollback(
        ConnectorRolloutActivityInputPromoteOrRollback(
          dockerRepository = input.dockerRepository,
          technicalName = input.dockerRepository.substringAfter("airbyte/"),
          dockerImageTag = input.dockerImageTag,
          action =
            if (input.result == ConnectorRolloutFinalState.SUCCEEDED) {
              ActionType.PROMOTE
            } else if (input.result == ConnectorRolloutFinalState.FAILED_ROLLED_BACK) {
              ActionType.ROLLBACK
            } else {
              throw IllegalArgumentException("Unrecognized status: $input.result")
            },
          rolloutId = input.rolloutId,
        ),
      )
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
    when (input.result) {
      ConnectorRolloutFinalState.SUCCEEDED -> succeeded = true
      ConnectorRolloutFinalState.FAILED_ROLLED_BACK -> failed = true
      ConnectorRolloutFinalState.CANCELED_ROLLED_BACK -> canceled = true
      else -> errored = true
    }
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
          result.append("${field.name}=")
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
