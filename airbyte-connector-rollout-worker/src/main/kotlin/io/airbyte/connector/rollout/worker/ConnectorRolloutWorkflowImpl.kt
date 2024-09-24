/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.worker

import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.connector.rollout.worker.activities.DoRolloutActivity
import io.airbyte.connector.rollout.worker.activities.FinalizeRolloutActivity
import io.airbyte.connector.rollout.worker.activities.FindRolloutActivity
import io.airbyte.connector.rollout.worker.activities.GetRolloutActivity
import io.airbyte.connector.rollout.worker.activities.StartRolloutActivity
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputFind
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputGet
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputStart
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutOutput
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.failure.ApplicationFailure
import io.temporal.workflow.Workflow
import org.slf4j.LoggerFactory
import java.lang.reflect.Field
import java.time.Duration

class ConnectorRolloutWorkflowImpl : ConnectorRolloutWorkflow {
  private val log = LoggerFactory.getLogger(ConnectorRolloutWorkflowImpl::class.java)
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

  private var succeeded = false
  private var canceled = false
  private var errored = false
  private var failed = false
  private var startRolloutFailed = false

  override fun run(input: ConnectorRolloutActivityInputStart): ConnectorEnumRolloutState {
    val workflowId = "${input.dockerRepository}:${input.dockerImageTag}:${input.actorDefinitionId.toString().substring(0, 8)}"
    log.info("Initialized rollout for $workflowId")
    Workflow.await { startRolloutFailed || failed || errored || canceled || succeeded }
    return when {
      startRolloutFailed -> throw ApplicationFailure.newFailure(
        "Failure starting rollout for $workflowId",
        ConnectorEnumRolloutState.ERRORED.value(),
      )
      failed -> throw ApplicationFailure.newFailure(
        "Failure during rollout for $workflowId",
        ConnectorEnumRolloutState.FAILED_ROLLED_BACK.value(),
      )
      canceled -> throw ApplicationFailure.newFailure(
        "Rollout for $workflowId was canceled.",
        ConnectorEnumRolloutState.CANCELED_ROLLED_BACK.value(),
      )
      else -> ConnectorEnumRolloutState.SUCCEEDED
    }
  }

  override fun startRollout(input: ConnectorRolloutActivityInputStart): ConnectorRolloutOutput {
    log.info("startRollout: calling startRolloutActivity")
    val workflowRunId = Workflow.getInfo().firstExecutionRunId
    return try {
      val output = startRolloutActivity.startRollout(workflowRunId, input)
      log.info("startRolloutActivity.startRollout")
      output
    } catch (e: Exception) {
      startRolloutFailed = true
      throw e
    }
  }

  override fun startRolloutValidator(input: ConnectorRolloutActivityInputStart) {
    log.info("startRolloutValidator: ${input.dockerRepository}:${input.dockerImageTag}")
    require(!(input.dockerRepository == null || input.dockerImageTag == null || input.actorDefinitionId == null || input.rolloutId == null)) {
      "Cannot start rollout; invalid input: ${mapAttributesToString(input)}"
    }
  }

  override fun findRollout(input: ConnectorRolloutActivityInputFind): List<ConnectorRolloutOutput> {
    log.info("getRollout: calling getRolloutActivity")
    val output = findRolloutActivity.findRollout(input)
    log.info("findRolloutActivity.findRollout: $output")
    return output
  }

  override fun findRolloutValidator(input: ConnectorRolloutActivityInputFind) {
    log.info("findRolloutValidator: ${input.dockerRepository}:${input.dockerImageTag}")
    require(!(input.dockerRepository == null || input.dockerImageTag == null || input.actorDefinitionId == null)) {
      "Cannot find rollout; invalid input: ${mapAttributesToString(input)}"
    }
  }

  override fun getRollout(input: ConnectorRolloutActivityInputGet): ConnectorRolloutOutput {
    log.info("getRollout: calling getRolloutActivity")
    val output = getRolloutActivity.getRollout(input)
    log.info("getRolloutActivity.getRollout pinned_actors = ${output.actorIds}")
    return output
  }

  override fun getRolloutValidator(input: ConnectorRolloutActivityInputGet) {
    log.info("getRolloutValidator: ${input.dockerRepository}:${input.dockerImageTag}")
    require(!(input.dockerRepository == null || input.dockerImageTag == null || input.actorDefinitionId == null || input.rolloutId == null)) {
      "Cannot get rollout; invalid input: ${mapAttributesToString(input)}"
    }
  }

  override fun doRollout(input: ConnectorRolloutActivityInputRollout): ConnectorRolloutOutput {
    log.info("doRollout: calling doRolloutActivity")
    val output = doRolloutActivity.doRollout(input)
    log.info("doRolloutActivity.doRollout pinned_connections = ${output.actorIds}")
    return output
  }

  override fun doRolloutValidator(input: ConnectorRolloutActivityInputRollout) {
    log.info("doRolloutValidator: ${input.dockerRepository}:${input.dockerImageTag}")
    require(!(input.dockerRepository == null || input.dockerImageTag == null || input.actorDefinitionId == null || input.rolloutId == null)) {
      "Cannot do rollout; invalid input: ${mapAttributesToString(input)}"
    }
  }

  override fun finalizeRollout(input: ConnectorRolloutActivityInputFinalize): ConnectorRolloutOutput {
    log.info("finalizeRollout: calling finalizeRolloutActivity")
    val rolloutResult = finalizeRolloutActivity.finalizeRollout(input)
    log.info("finalizeRolloutActivity.finalizeRollout rolloutResult = $rolloutResult")
    when (input.result) {
      ConnectorRolloutFinalState.SUCCEEDED -> succeeded = true
      ConnectorRolloutFinalState.FAILED_ROLLED_BACK -> failed = true
      ConnectorRolloutFinalState.CANCELED_ROLLED_BACK -> canceled = true
      else -> errored = true
    }
    return rolloutResult
  }

  override fun finalizeRolloutValidator(input: ConnectorRolloutActivityInputFinalize) {
    log.info("finalizeRolloutValidator: ${input.dockerRepository}:${input.dockerImageTag}")
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
          LoggerFactory.getLogger(ConnectorRolloutWorkflowImpl::class.java)
            .error("Error mapping attributes to string: ${e.message}")
        }
      }
      return result.toString().trim()
    }
  }
}
