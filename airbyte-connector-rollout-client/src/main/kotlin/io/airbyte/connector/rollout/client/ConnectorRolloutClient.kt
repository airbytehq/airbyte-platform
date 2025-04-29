/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.connector.rollout.client

import io.airbyte.config.ConnectorEnumRolloutStrategy
import io.airbyte.config.ConnectorRollout
import io.airbyte.config.ConnectorRolloutFinalState
import io.airbyte.connector.rollout.shared.Constants
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputFinalize
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputPause
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutActivityInputRollout
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutOutput
import io.airbyte.connector.rollout.shared.models.ConnectorRolloutWorkflowInput
import io.airbyte.connector.rollout.worker.ConnectorRolloutWorkflow
import io.github.oshai.kotlinlogging.KotlinLogging
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.enums.v1.WorkflowIdConflictPolicy
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest
import io.temporal.api.workflowservice.v1.TerminateWorkflowExecutionRequest
import io.temporal.client.WorkflowClient
import io.temporal.client.WorkflowOptions
import io.temporal.client.WorkflowStub
import io.temporal.client.WorkflowUpdateException
import io.temporal.client.WorkflowUpdateStage
import io.temporal.common.RetryOptions
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
      tag: String?,
    ): String {
      var workflowId = "$name:$version:${actorDefinitionId.toString().substring(0, 8)}"
      if (tag != null) {
        workflowId = workflowId + ":$tag"
      }
      return workflowId
    }

    private fun <I, T> executeUpdate(
      input: I,
      workflowId: String,
      action: (ConnectorRolloutWorkflow, I) -> T,
    ): T =
      try {
        val workflowStub = workflowClient.getClient().newWorkflowStub(ConnectorRolloutWorkflow::class.java, workflowId)
        logger.info { "Executing workflow action for $workflowId" }
        action(workflowStub, input)
      } catch (e: WorkflowUpdateException) {
        logger.error { "Error executing workflow action: $e" }
        throw e
      }

    fun startRolloutWorkflow(
      connectorRollout: ConnectorRollout,
      dockerRepository: String,
      dockerImageTag: String,
      actorDefinitionId: UUID,
      defaultDockerImageTag: String?,
      migratePins: Boolean?,
      waitBetweenRolloutSeconds: Int?,
      waitBetweenSyncResultsQueriesSeconds: Int?,
      rolloutExpirationSeconds: Int?,
      updatedBy: UUID?,
      rolloutStrategy: ConnectorEnumRolloutStrategy,
    ) {
      logger.info {
        "ConnectorRolloutService.startWorkflow with input: id=${connectorRollout.id} rolloutStrategy=$rolloutStrategy"
      }

      val workflowId = getWorkflowId(dockerRepository, dockerImageTag, connectorRollout.actorDefinitionId, connectorRollout.tag)

      // If a workflow already exists, terminate it and start a new one.
      // This is a valid operation because we rely on Postgres constraints to decide whether we're allowed to start a new rollout.
      // Once we've gotten here, Postgres has allowed us to create a new rollout.
      // Theoretically there should not be a running workflow if we're starting a new rollout, so this should always be a noop.
      // In the event that we are in this state, an alternative would be to only start a new workflow if there isn't an existing
      // one, but if we've gotten into this state the workflow may be unhealthy so termination is safer. Connector rollout workflows
      // recover their state from postgres and all activities are idempotent, so we can always terminate and restart.
      //
      // Note: any connectors pinned to the release candidate will not be unpinned when we terminate via this code path.
      val workflowStub =
        workflowClient.getClient().newWorkflowStub(
          ConnectorRolloutWorkflow::class.java,
          WorkflowOptions
            .newBuilder()
            .setWorkflowId(workflowId)
            .setTaskQueue(Constants.TASK_QUEUE)
            .setWorkflowIdConflictPolicy(WorkflowIdConflictPolicy.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING)
            .setRetryOptions(RetryOptions.newBuilder().setMaximumAttempts(1).build())
            .build(),
        )

      logger.info { "Starting workflow $workflowId" }
      val workflowExecution =
        WorkflowClient.start(
          workflowStub::run,
          ConnectorRolloutWorkflowInput(
            dockerRepository,
            dockerImageTag,
            actorDefinitionId,
            connectorRollout.id,
            updatedBy,
            rolloutStrategy,
            defaultDockerImageTag,
            connectorRollout,
            null,
            null,
            migratePins,
            waitBetweenRolloutSeconds,
            waitBetweenSyncResultsQueriesSeconds,
            rolloutExpirationSeconds,
          ),
        )
      logger.info { "Workflow for connectorRollout ${connectorRollout.id} $workflowId initialized with ID: ${workflowExecution.workflowId}" }
    }

    private fun startWorkflowIfNotExists(
      workflowId: String,
      connectorRollout: ConnectorRollout,
      dockerRepository: String,
      dockerImageTag: String,
      actorDefinitionId: UUID,
      defaultDockerImageTag: String? = null,
      migratePins: Boolean? = true,
      waitBetweenRolloutSeconds: Int? = null,
      waitBetweenSyncResultsQueriesSeconds: Int? = null,
      rolloutExpirationSeconds: Int? = null,
      updatedBy: UUID? = null,
      rolloutStrategy: ConnectorEnumRolloutStrategy,
    ) {
      try {
        val workflowExecution = WorkflowExecution.newBuilder().setWorkflowId(workflowId).build()
        val request =
          DescribeWorkflowExecutionRequest
            .newBuilder()
            .setNamespace(workflowClient.getClient().options.namespace)
            .setExecution(workflowExecution)
            .build()
        val response =
          workflowClient
            .getClient()
            .workflowServiceStubs
            .blockingStub()
            .describeWorkflowExecution(request)

        when (val status = response.workflowExecutionInfo.status) {
          WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING,
          WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
          -> {
            logger.info { "Workflow $workflowId for connectorRollout.id ${connectorRollout.id} already running with status: $status" }
            return
          }

          WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED,
          WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED,
          WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CANCELED,
          WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TERMINATED,
          WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
          WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED,
          -> {
            logger.info { "Workflow $workflowId for connectorRollout.id ${connectorRollout.id} is restarting from status $status" }
            startRolloutWorkflow(
              connectorRollout,
              dockerRepository,
              dockerImageTag,
              actorDefinitionId,
              defaultDockerImageTag,
              migratePins,
              waitBetweenRolloutSeconds,
              waitBetweenSyncResultsQueriesSeconds,
              rolloutExpirationSeconds,
              updatedBy,
              rolloutStrategy,
            )
          }

          WorkflowExecutionStatus.UNRECOGNIZED,
          -> {
            throw RuntimeException("Unrecognized workflow execution status for $workflowId connectorRollout.id=${connectorRollout.id}")
          }
        }
      } catch (e: StatusRuntimeException) {
        if (e.status.code == Status.Code.NOT_FOUND) {
          logger.info { "Workflow $workflowId for connectorRollout.id ${connectorRollout.id} was not found and is starting" }
          startRolloutWorkflow(
            connectorRollout,
            dockerRepository,
            dockerImageTag,
            actorDefinitionId,
            defaultDockerImageTag,
            migratePins,
            waitBetweenRolloutSeconds,
            waitBetweenSyncResultsQueriesSeconds,
            rolloutExpirationSeconds,
            updatedBy,
            rolloutStrategy,
          )
        } else {
          throw e
        }
      }
    }

    fun doRollout(
      connectorRollout: ConnectorRollout,
      dockerRepository: String,
      dockerImageTag: String,
      actorDefinitionId: UUID,
      actorIds: List<UUID>,
      targetPercentage: Int?,
      updatedBy: UUID?,
      rolloutStrategy: ConnectorEnumRolloutStrategy,
    ) {
      val workflowId = getWorkflowId(dockerRepository, dockerImageTag, actorDefinitionId, connectorRollout.tag)

      startWorkflowIfNotExists(
        workflowId,
        connectorRollout,
        dockerRepository,
        dockerImageTag,
        actorDefinitionId,
        updatedBy = updatedBy,
        rolloutStrategy = rolloutStrategy,
      )

      val workflow =
        workflowClient.getClient().newWorkflowStub(
          ConnectorRolloutWorkflow::class.java,
          workflowId,
        )
      logger.info { "Rollout starting `doRollout`. connectorRollout.id=${connectorRollout.id} $workflowId" }
      // Send the `update` request async so we don't block waiting for the actors to be pinned
      WorkflowStub.fromTyped(workflow).startUpdate(
        "progressRollout",
        WorkflowUpdateStage.ACCEPTED,
        ConnectorRolloutOutput::class.java,
        ConnectorRolloutActivityInputRollout(
          dockerRepository,
          dockerImageTag,
          actorDefinitionId,
          connectorRollout.id,
          actorIds,
          targetPercentage,
          updatedBy,
          rolloutStrategy,
        ),
      )
    }

    fun pauseRollout(
      connectorRollout: ConnectorRollout,
      dockerRepository: String,
      dockerImageTag: String,
      actorDefinitionId: UUID,
      pausedReason: String,
      updatedBy: UUID?,
      rolloutStrategy: ConnectorEnumRolloutStrategy,
    ): ConnectorRolloutOutput {
      val workflowId = getWorkflowId(dockerRepository, dockerImageTag, actorDefinitionId, connectorRollout.tag)

      startWorkflowIfNotExists(
        workflowId,
        connectorRollout,
        dockerRepository,
        dockerImageTag,
        actorDefinitionId,
        updatedBy = updatedBy,
        rolloutStrategy = rolloutStrategy,
      )

      return executeUpdate(
        ConnectorRolloutActivityInputPause(
          dockerRepository,
          dockerImageTag,
          actorDefinitionId,
          connectorRollout.id,
          pausedReason,
          updatedBy,
          rolloutStrategy,
        ),
        workflowId,
      ) { stub, i -> stub.pauseRollout(i) }
    }

    fun finalizeRollout(
      connectorRollout: ConnectorRollout,
      dockerRepository: String,
      dockerImageTag: String,
      actorDefinitionId: UUID,
      defaultDockerImageTag: String,
      finalState: ConnectorRolloutFinalState,
      errorMsg: String? = null,
      failedReason: String? = null,
      updatedBy: UUID? = null,
      rolloutStrategy: ConnectorEnumRolloutStrategy,
      retainPinsOnCancellation: Boolean = true,
    ) {
      val workflowId = getWorkflowId(dockerRepository, dockerImageTag, actorDefinitionId, connectorRollout.tag)

      startWorkflowIfNotExists(
        workflowId,
        connectorRollout,
        dockerRepository,
        dockerImageTag,
        actorDefinitionId,
        updatedBy = updatedBy,
        rolloutStrategy = rolloutStrategy,
      )

      logger.info { "Rollout starting `finalizeRollout` update. connectorRollout.id=${connectorRollout.id} $workflowId" }
      val workflow =
        workflowClient.getClient().newWorkflowStub(
          ConnectorRolloutWorkflow::class.java,
          workflowId,
        )

      // Send the `update` request async so we don't block waiting for the GHA to run and the default version to become available
      WorkflowStub.fromTyped(workflow).startUpdate(
        "finalizeRollout",
        WorkflowUpdateStage.ACCEPTED,
        ConnectorRolloutOutput::class.java,
        ConnectorRolloutActivityInputFinalize(
          dockerRepository,
          dockerImageTag,
          actorDefinitionId,
          connectorRollout.id,
          defaultDockerImageTag,
          finalState,
          errorMsg,
          failedReason,
          updatedBy,
          rolloutStrategy,
          retainPinsOnCancellation,
        ),
      )
    }

    fun cancelRollout(
      connectorRollout: ConnectorRollout,
      dockerRepository: String,
      dockerImageTag: String,
      actorDefinitionId: UUID,
      errorMsg: String? = null,
      failedReason: String? = null,
    ) {
      val workflowId = getWorkflowId(dockerRepository, dockerImageTag, actorDefinitionId, connectorRollout.tag)

      val request =
        TerminateWorkflowExecutionRequest
          .newBuilder()
          .setNamespace(workflowClient.getClient().options.namespace)
          .setWorkflowExecution(
            WorkflowExecution.newBuilder().setWorkflowId(workflowId).build(),
          ).setReason(
            "Manual cancellation. connectorRollout.id=${connectorRollout.id} workflow=$workflowId errorMsg=$errorMsg failedReason=$failedReason",
          ).build()

      try {
        workflowClient
          .getClient()
          .workflowServiceStubs
          .blockingStub()
          .terminateWorkflowExecution(request)
        logger
          .info { "Workflow terminated. connectorRollout.id=${connectorRollout.id} workflow=$workflowId" }
      } catch (e: Throwable) {
        logger.error(e) { "Unable to terminate workflow. connectorRollout.id=${connectorRollout.id} workflow=$workflowId" }
      }
    }
  }
