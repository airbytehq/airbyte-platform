/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.client

import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.connector.rollout.shared.Constants
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPause
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputStart
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutWorkflowInput
import io.airbyte.connector.rollout.worker.ConnectorRolloutWorkflow
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.client.WorkflowUpdateException
import io.temporal.client.WorkflowUpdateStage
import jakarta.inject.Inject
import jakarta.inject.Singleton
import java.util.UUID

private val logger = KotlinLogging.logger {}

@Singleton
class ConnectorRolloutClient
  @Inject
  constructor(
    private val workflowClient: WorkflowClientWrapper,
  ) {
    init {
      logger.info { "ConnectorRolloutService constructor" }
    }

    private fun getWorkflowId(
      name: String,
      version: String,
      actorDefinitionId: UUID,
    ): String {
      return "$name:$version:${actorDefinitionId.toString().substring(0, 8)}"
    }

    private fun <I, T> executeUpdate(
      input: I,
      workflowId: String,
      action: (ConnectorRolloutWorkflow, I) -> T,
    ): T {
      return try {
        val workflowStub = workflowClient.getClient().newWorkflowStub(ConnectorRolloutWorkflow::class.java, workflowId)
        logger.info { "Executing workflow action for $workflowId" }
        action(workflowStub, input)
      } catch (e: WorkflowUpdateException) {
        logger.error { "Error executing workflow action: $e" }
        throw e
      }
    }

    fun startRollout(input: ConnectorRolloutWorkflowInput) {
      logger.info { "ConnectorRolloutService.startWorkflow with input: id=${input.rolloutId} rolloutStrategy=${input.rolloutStrategy}" }
      if (input.rolloutId == null) {
        throw RuntimeException("Rollout ID is required to start a rollout workflow")
      }

      val workflowId = getWorkflowId(input.dockerRepository, input.dockerImageTag, input.connectorRollout!!.actorDefinitionId)
      val workflowStub =
        workflowClient.getClient().newWorkflowStub(
          ConnectorRolloutWorkflow::class.java,
          WorkflowOptions.newBuilder()
            .setWorkflowId(workflowId)
            .setTaskQueue(Constants.TASK_QUEUE)
            .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_FAIL)
            .build(),
        )

      logger.info { "Starting workflow $workflowId" }
      val workflowExecution = WorkflowClient.start(workflowStub::run, input)
      logger.info { "Workflow $workflowId initialized with ID: ${workflowExecution.workflowId}" }

      if (input.rolloutStrategy == ConnectorEnumRolloutStrategy.MANUAL) {
        val connectorRolloutActivityInputStart =
          ConnectorRolloutActivityInputStart(
            input.dockerRepository,
            input.dockerImageTag,
            input.actorDefinitionId,
            input.rolloutId,
            input.updatedBy,
            input.rolloutStrategy,
            input.migratePins,
          )

        executeUpdate(connectorRolloutActivityInputStart, workflowId) { stub, i -> stub.startRollout(i) }
        logger.info { "Rollout $workflowId started with ID: ${workflowExecution.workflowId}" }
      }
    }

    fun doRollout(input: ConnectorRolloutActivityInputRollout): ConnectorRolloutOutput {
      val workflowId = getWorkflowId(input.dockerRepository, input.dockerImageTag, input.actorDefinitionId)
      return executeUpdate(input, workflowId) { stub, i -> stub.progressRollout(i) }
    }

    fun pauseRollout(input: ConnectorRolloutActivityInputPause): ConnectorRolloutOutput {
      val workflowId = getWorkflowId(input.dockerRepository, input.dockerImageTag, input.actorDefinitionId)
      return executeUpdate(input, workflowId) { stub, i -> stub.pauseRollout(i) }
    }

    fun finalizeRollout(input: ConnectorRolloutActivityInputFinalize) {
      val workflowId = getWorkflowId(input.dockerRepository, input.dockerImageTag, input.actorDefinitionId)
      logger.info { "Rollout $workflowId starting `finalizeRollout` update: $workflowId" }
      val workflow =
        workflowClient.getClient().newWorkflowStub(
          ConnectorRolloutWorkflow::class.java,
          workflowId,
        )
      logger.info { "Rollout $workflowId starting `finalizeRollout` workflow: $workflow" }
      // Send the `update` request async so we don't block waiting for the GHA to run and the default version to become available
      WorkflowStub.fromTyped(workflow).startUpdate(
        "finalizeRollout",
        WorkflowUpdateStage.ACCEPTED,
        ConnectorRolloutOutput::class.java,
        input,
      )
    }
  }
