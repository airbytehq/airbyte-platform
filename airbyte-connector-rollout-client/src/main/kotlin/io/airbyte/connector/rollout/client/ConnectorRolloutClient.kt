/*
 * Copyright (c) 2020-2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.client

import io.airbyte.config.ConnectorEnumRolloutState
import io.airbyte.connector.rollout.worker.ConnectorRolloutWorkflow
import io.airbyte.connector.rollout.worker.Constants
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputFind
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputGet
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutActivityInputStart
import io.airbyte.connector.rollout.worker.models.ConnectorRolloutOutput
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowUpdateException
import jakarta.inject.Inject
import jakarta.inject.Named
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.util.UUID

@Singleton
class ConnectorRolloutClient
  @Inject
  constructor(
    @Named("connectorRolloutClientWorkflowClient") private val client: WorkflowClient,
  ) {
    companion object {
      private val log = LoggerFactory.getLogger(ConnectorRolloutClient::class.java)
    }

    init {
      log.info("ConnectorRolloutService constructor")
    }

    private fun getWorkflowId(
      name: String,
      version: String,
      actorDefinitionId: UUID,
    ): String {
      return "$name:$version:${actorDefinitionId.toString().substring(0, 8)}"
    }

    private fun <I, T> executeWorkflow(
      input: I,
      workflowId: String,
      action: (ConnectorRolloutWorkflow, I) -> T,
    ): T {
      return try {
        val workflowStub = client.newWorkflowStub(ConnectorRolloutWorkflow::class.java, workflowId)
        log.info("Executing workflow action for $workflowId")
        action(workflowStub, input)
      } catch (e: WorkflowUpdateException) {
        log.error("Error executing workflow action: $e")
        throw e
      }
    }

    fun startWorkflow(input: ConnectorRolloutActivityInputStart): ConnectorRolloutOutput {
      log.info("ConnectorRolloutService.startWorkflow with input: $input")
      if (input.rolloutId == null) {
        throw RuntimeException("Rollout ID is required to start a rollout workflow")
      }

      val workflowId = getWorkflowId(input.dockerRepository, input.dockerImageTag, input.actorDefinitionId)
      val workflowStub =
        client.newWorkflowStub(
          ConnectorRolloutWorkflow::class.java,
          WorkflowOptions.newBuilder()
            .setWorkflowId(workflowId)
            .setTaskQueue(Constants.TASK_QUEUE)
            .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_FAIL)
            .build(),
        )

      log.info("Starting workflow $workflowId")
      val workflowExecution: WorkflowExecution = WorkflowClient.start(workflowStub::run, input)
      log.info("Workflow $workflowId initialized with ID: ${workflowExecution.workflowId}")
      val startOutput = executeWorkflow(input, workflowId) { stub, i -> stub.startRollout(i) }
      log.info("Rollout $workflowId started with ID: ${workflowExecution.workflowId}")
      return startOutput
    }

    fun findRollout(input: ConnectorRolloutActivityInputFind): List<ConnectorRolloutOutput> {
      val workflowId = getWorkflowId(input.dockerRepository, input.dockerImageTag, input.actorDefinitionId)
      return executeWorkflow(input, workflowId) { stub, i -> stub.findRollout(i) }
    }

    fun getRollout(input: ConnectorRolloutActivityInputGet): ConnectorRolloutOutput {
      val workflowId = getWorkflowId(input.dockerRepository, input.dockerImageTag, input.actorDefinitionId)
      return executeWorkflow(input, workflowId) { stub, i -> stub.getRollout(i) }
    }

    fun doRollout(input: ConnectorRolloutActivityInputRollout): ConnectorRolloutOutput {
      val workflowId = getWorkflowId(input.dockerRepository, input.dockerImageTag, input.actorDefinitionId)
      return executeWorkflow(input, workflowId) { stub, i -> stub.doRollout(i) }
    }

    fun finalizeRollout(input: ConnectorRolloutActivityInputFinalize): ConnectorRolloutOutput {
      val workflowId = getWorkflowId(input.dockerRepository, input.dockerImageTag, input.actorDefinitionId)
      return executeWorkflow(input, workflowId) { stub, i -> stub.finalizeRollout(i) }
    }

    fun getWorkflowResult(input: ConnectorRolloutActivityInputFinalize): ConnectorEnumRolloutState {
      val workflowId = getWorkflowId(input.dockerRepository, input.dockerImageTag, input.actorDefinitionId)
      val workflowStubUntyped = client.newUntypedWorkflowStub(workflowId)
      log.info("Getting workflow result for ID: $workflowId")
      return workflowStubUntyped.getResult(ConnectorEnumRolloutState::class.java)
    }
  }
