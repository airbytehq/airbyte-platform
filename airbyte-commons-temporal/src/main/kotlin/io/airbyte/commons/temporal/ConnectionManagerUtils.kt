/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.airbyte.commons.temporal.exception.DeletedWorkflowException
import io.airbyte.commons.temporal.exception.UnreachableWorkflowException
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow
import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput
import io.airbyte.commons.temporal.scheduling.state.WorkflowState
import io.airbyte.metrics.MetricAttribute
import io.airbyte.metrics.MetricClient
import io.airbyte.metrics.OssMetricsRegistry
import io.airbyte.metrics.lib.MetricTags
import io.github.oshai.kotlinlogging.KotlinLogging
import io.temporal.api.common.v1.WorkflowExecution
import io.temporal.api.enums.v1.WorkflowExecutionStatus
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest
import io.temporal.client.WorkflowClient
import io.temporal.workflow.Functions
import io.temporal.workflow.Functions.Proc1
import io.temporal.workflow.Functions.TemporalFunctionalInterfaceMarker
import jakarta.inject.Singleton
import java.util.Optional
import java.util.UUID
import java.util.function.Function

/**
 * Utility functions for connection manager workflows.
 */
@Singleton
class ConnectionManagerUtils(
  private val workflowClientWrapped: WorkflowClientWrapped,
  private val metricClient: MetricClient,
) {
  /**
   * Send a cancellation to the workflow. It will swallow any exception and won't check if the
   * workflow is already deleted when being cancel.
   */
  fun deleteWorkflowIfItExist(connectionId: UUID) {
    try {
      log.info { "Deleting workflow $connectionId" }
      val connectionManagerWorkflow =
        workflowClientWrapped.newWorkflowStub(ConnectionManagerWorkflow::class.java, getConnectionManagerName(connectionId))
      connectionManagerWorkflow.deleteConnection()
    } catch (e: Exception) {
      log.warn("The workflow is not reachable when trying to cancel it, for the connection $connectionId", e)
    }
  }

  /**
   * Attempts to send a signal to the existing ConnectionManagerWorkflow for the provided connection.
   *
   * If the workflow is unreachable, this will restart the workflow and send the signal in a single
   * batched request. Batching is used to avoid race conditions between starting the workflow and
   * executing the signal.
   *
   * @param connectionId the connection ID to execute this operation for
   * @param signalMethod a function that takes in a connection manager workflow and executes a signal
   * method on it, with no arguments
   * @return the healthy connection manager workflow that was signaled
   * @throws DeletedWorkflowException if the connection manager workflow was deleted
   */
  @Throws(DeletedWorkflowException::class)
  fun signalWorkflowAndRepairIfNecessary(
    connectionId: UUID,
    signalMethod: Function<ConnectionManagerWorkflow, Functions.Proc>,
  ): ConnectionManagerWorkflow = signalWorkflowAndRepairIfNecessary(connectionId, signalMethod, Optional.empty<Any>())

  // This method unifies the logic of the above two, by using the optional signalArgument parameter to
  // indicate if an argument is being provided to the signal or not.
  // Keeping this private and only exposing the above methods outside this class provides a strict
  // type enforcement for external calls, and means this method can assume consistent type
  // implementations for both cases.
  @Throws(DeletedWorkflowException::class)
  private fun <T> signalWorkflowAndRepairIfNecessary(
    connectionId: UUID,
    signalMethod: Function<ConnectionManagerWorkflow, out TemporalFunctionalInterfaceMarker>,
    signalArgument: Optional<T>,
  ): ConnectionManagerWorkflow {
    try {
      val connectionManagerWorkflow = getConnectionManagerWorkflow(connectionId)
      log.info("Retrieved existing connection manager workflow for connection {}. Executing signal.", connectionId)
      // retrieve the signal from the lambda
      val signal = signalMethod.apply(connectionManagerWorkflow)
      // execute the signal
      if (signalArgument.isPresent) {
        (signal as Proc1<T>).apply(signalArgument.get())
      } else {
        (signal as Functions.Proc).apply()
      }
      return connectionManagerWorkflow
    } catch (e: UnreachableWorkflowException) {
      metricClient.count(
        OssMetricsRegistry.WORFLOW_UNREACHABLE,
        1L,
        MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()),
      )
      log.error(
        String.format(
          "Failed to retrieve ConnectionManagerWorkflow for connection %s. " +
            "Repairing state by creating new workflow and starting with the signal.",
          connectionId,
        ),
        e,
      )

      // in case there is an existing workflow in a bad state, attempt to terminate it first before
      // starting a new workflow
      safeTerminateWorkflow(connectionId, "Terminating workflow in unreachable state before starting a new workflow for this connection")

      val connectionManagerWorkflow = newConnectionManagerWorkflowStub(connectionId)
      val startWorkflowInput = TemporalWorkflowUtils.buildStartWorkflowInput(connectionId)

      val batchRequest = workflowClientWrapped.newSignalWithStartRequest()
      batchRequest.add(
        { connectionUpdaterInput: ConnectionUpdaterInput -> connectionManagerWorkflow.run(connectionUpdaterInput) },
        startWorkflowInput,
      )

      // retrieve the signal from the lambda
      val signal = signalMethod.apply(connectionManagerWorkflow)
      // add signal to batch request
      if (signalArgument.isPresent) {
        batchRequest.add(signal as Proc1<T>, signalArgument.get())
      } else {
        batchRequest.add(signal as Functions.Proc)
      }

      workflowClientWrapped.signalWithStart(batchRequest)
      log.info("Connection manager workflow for connection {} has been started and signaled.", connectionId)

      return connectionManagerWorkflow
    }
  }

  fun safeTerminateWorkflow(
    workflowId: String,
    reason: String,
  ) {
    log.info("Attempting to terminate existing workflow for workflowId {}.", workflowId)
    try {
      workflowClientWrapped.terminateWorkflow(workflowId, reason)
    } catch (e: Exception) {
      log.warn(
        "Could not terminate temporal workflow due to the following error; " +
          "this may be because there is currently no running workflow for this connection.",
        e,
      )
    }
  }

  // todo (cgardens) - what makes this safe

  /**
   * Terminate a temporal workflow and throw a useful exception.
   *
   * @param connectionId connection id
   * @param reason reason for terminating the workflow
   */
  fun safeTerminateWorkflow(
    connectionId: UUID,
    reason: String,
  ) {
    safeTerminateWorkflow(getConnectionManagerName(connectionId), reason)
  }

  // todo (cgardens) - what does no signal mean in this context?

  /**
   * Start a connection manager workflow for a connection.
   *
   * @param connectionId connection id
   * @return new connection manager workflow
   */
  fun startConnectionManagerNoSignal(connectionId: UUID): ConnectionManagerWorkflow {
    val connectionManagerWorkflow = newConnectionManagerWorkflowStub(connectionId)
    val input = TemporalWorkflowUtils.buildStartWorkflowInput(connectionId)
    WorkflowClient.start({ connectionUpdaterInput: ConnectionUpdaterInput -> connectionManagerWorkflow.run(connectionUpdaterInput) }, input)

    return connectionManagerWorkflow
  }

  /**
   * Attempts to retrieve the connection manager workflow for the provided connection.
   *
   * @param connectionId the ID of the connection whose workflow should be retrieved
   * @return the healthy ConnectionManagerWorkflow
   * @throws DeletedWorkflowException if the workflow was deleted, according to the workflow state
   * @throws UnreachableWorkflowException if the workflow is in an unreachable state
   */
  @Throws(DeletedWorkflowException::class, UnreachableWorkflowException::class)
  fun getConnectionManagerWorkflow(connectionId: UUID): ConnectionManagerWorkflow {
    val connectionManagerWorkflow: ConnectionManagerWorkflow
    val workflowState: WorkflowState
    val workflowExecutionStatus: WorkflowExecutionStatus
    try {
      connectionManagerWorkflow =
        workflowClientWrapped.newWorkflowStub(ConnectionManagerWorkflow::class.java, getConnectionManagerName(connectionId))
      workflowState = connectionManagerWorkflow.getState()
      workflowExecutionStatus = getConnectionManagerWorkflowStatus(connectionId)
    } catch (e: Exception) {
      throw UnreachableWorkflowException(
        String.format("Failed to retrieve ConnectionManagerWorkflow for connection %s due to the following error:", connectionId),
        e,
      )
    }

    if (WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED == workflowExecutionStatus) {
      if (workflowState.isDeleted) {
        throw DeletedWorkflowException(
          String.format(
            "The connection manager workflow for connection %s is deleted, so no further operations cannot be performed on it.",
            connectionId,
          ),
        )
      }

      // A non-deleted workflow being in a COMPLETED state is unexpected, and should be corrected
      throw UnreachableWorkflowException(
        String.format("ConnectionManagerWorkflow for connection %s is unreachable due to having COMPLETED status.", connectionId),
      )
    }

    return connectionManagerWorkflow
  }

  fun getWorkflowState(connectionId: UUID): Optional<WorkflowState> {
    try {
      val connectionManagerWorkflow =
        workflowClientWrapped.newWorkflowStub(
          ConnectionManagerWorkflow::class.java,
          getConnectionManagerName(connectionId),
        )
      return Optional.of(connectionManagerWorkflow.getState())
    } catch (e: Exception) {
      log.error("Exception thrown while checking workflow state for connection id {}", connectionId, e)
      return Optional.empty()
    }
  }

  fun isWorkflowStateRunning(connectionId: UUID): Boolean = getWorkflowState(connectionId).map(WorkflowState::isRunning).orElse(false)

  /**
   * Get status of a connection manager workflow.
   *
   * @param connectionId connection id
   * @return workflow execution status
   */
  private fun getConnectionManagerWorkflowStatus(connectionId: UUID): WorkflowExecutionStatus {
    val describeWorkflowExecutionRequest =
      DescribeWorkflowExecutionRequest
        .newBuilder()
        .setExecution(
          WorkflowExecution
            .newBuilder()
            .setWorkflowId(getConnectionManagerName(connectionId))
            .build(),
        ).setNamespace(workflowClientWrapped.namespace)
        .build()

    val describeWorkflowExecutionResponse =
      workflowClientWrapped.blockingDescribeWorkflowExecution(describeWorkflowExecutionRequest)

    return describeWorkflowExecutionResponse.workflowExecutionInfo.status
  }

  /**
   * Get the job id for a connection is a workflow is running for it. Otherwise, throws.
   *
   * @param connectionId connection id
   * @return current job id
   */
  fun getCurrentJobId(connectionId: UUID): Long {
    try {
      val connectionManagerWorkflow = getConnectionManagerWorkflow(connectionId)
      return connectionManagerWorkflow.getJobInformation().jobId
    } catch (e: Exception) {
      return ConnectionManagerWorkflow.NON_RUNNING_JOB_ID
    }
  }

  private fun newConnectionManagerWorkflowStub(connectionId: UUID): ConnectionManagerWorkflow =
    workflowClientWrapped.newWorkflowStub(
      ConnectionManagerWorkflow::class.java,
      TemporalWorkflowUtils.buildWorkflowOptions(TemporalJobType.CONNECTION_UPDATER, getConnectionManagerName(connectionId)),
    )

  fun getConnectionManagerName(connectionId: UUID): String = "connection_manager_$connectionId"

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
