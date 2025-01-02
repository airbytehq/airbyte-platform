/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import io.airbyte.commons.temporal.exception.DeletedWorkflowException;
import io.airbyte.commons.temporal.exception.UnreachableWorkflowException;
import io.airbyte.commons.temporal.scheduling.ConnectionManagerWorkflow;
import io.airbyte.commons.temporal.scheduling.ConnectionUpdaterInput;
import io.airbyte.commons.temporal.scheduling.state.WorkflowState;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClient;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse;
import io.temporal.client.BatchRequest;
import io.temporal.client.WorkflowClient;
import io.temporal.workflow.Functions.Proc;
import io.temporal.workflow.Functions.Proc1;
import io.temporal.workflow.Functions.TemporalFunctionalInterfaceMarker;
import jakarta.inject.Singleton;
import java.lang.invoke.MethodHandles;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility functions for connection manager workflows.
 */
@Singleton
public class ConnectionManagerUtils {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final WorkflowClientWrapped workflowClientWrapped;
  private final MetricClient metricClient;

  public ConnectionManagerUtils(final WorkflowClientWrapped workflowClientWrapped, final MetricClient metricClient) {
    this.workflowClientWrapped = workflowClientWrapped;
    this.metricClient = metricClient;
  }

  /**
   * Send a cancellation to the workflow. It will swallow any exception and won't check if the
   * workflow is already deleted when being cancel.
   */
  public void deleteWorkflowIfItExist(final UUID connectionId) {
    try {
      final ConnectionManagerWorkflow connectionManagerWorkflow =
          workflowClientWrapped.newWorkflowStub(ConnectionManagerWorkflow.class, getConnectionManagerName(connectionId));
      connectionManagerWorkflow.deleteConnection();
    } catch (final Exception e) {
      log.warn("The workflow is not reachable when trying to cancel it", e);
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
   *        method on it, with no arguments
   * @return the healthy connection manager workflow that was signaled
   * @throws DeletedWorkflowException if the connection manager workflow was deleted
   */
  public ConnectionManagerWorkflow signalWorkflowAndRepairIfNecessary(final UUID connectionId,
                                                                      final Function<ConnectionManagerWorkflow, Proc> signalMethod)
      throws DeletedWorkflowException {
    return signalWorkflowAndRepairIfNecessary(connectionId, signalMethod, Optional.empty());
  }

  // This method unifies the logic of the above two, by using the optional signalArgument parameter to
  // indicate if an argument is being provided to the signal or not.
  // Keeping this private and only exposing the above methods outside this class provides a strict
  // type enforcement for external calls, and means this method can assume consistent type
  // implementations for both cases.
  @SuppressWarnings("LineLength")
  private <T> ConnectionManagerWorkflow signalWorkflowAndRepairIfNecessary(final UUID connectionId,
                                                                           final Function<ConnectionManagerWorkflow, ? extends TemporalFunctionalInterfaceMarker> signalMethod,
                                                                           final Optional<T> signalArgument)
      throws DeletedWorkflowException {
    try {
      final ConnectionManagerWorkflow connectionManagerWorkflow = getConnectionManagerWorkflow(connectionId);
      log.info("Retrieved existing connection manager workflow for connection {}. Executing signal.", connectionId);
      // retrieve the signal from the lambda
      final TemporalFunctionalInterfaceMarker signal = signalMethod.apply(connectionManagerWorkflow);
      // execute the signal
      if (signalArgument.isPresent()) {
        ((Proc1<T>) signal).apply(signalArgument.get());
      } else {
        ((Proc) signal).apply();
      }
      return connectionManagerWorkflow;
    } catch (final UnreachableWorkflowException e) {
      metricClient.count(OssMetricsRegistry.WORFLOW_UNREACHABLE, 1,
          new MetricAttribute(MetricTags.CONNECTION_ID, connectionId.toString()));
      log.error(
          String.format(
              "Failed to retrieve ConnectionManagerWorkflow for connection %s. "
                  + "Repairing state by creating new workflow and starting with the signal.",
              connectionId),
          e);

      // in case there is an existing workflow in a bad state, attempt to terminate it first before
      // starting a new workflow
      safeTerminateWorkflow(connectionId, "Terminating workflow in unreachable state before starting a new workflow for this connection");

      final ConnectionManagerWorkflow connectionManagerWorkflow = newConnectionManagerWorkflowStub(connectionId);
      final ConnectionUpdaterInput startWorkflowInput = TemporalWorkflowUtils.buildStartWorkflowInput(connectionId);

      final BatchRequest batchRequest = workflowClientWrapped.newSignalWithStartRequest();
      batchRequest.add(connectionManagerWorkflow::run, startWorkflowInput);

      // retrieve the signal from the lambda
      final TemporalFunctionalInterfaceMarker signal = signalMethod.apply(connectionManagerWorkflow);
      // add signal to batch request
      if (signalArgument.isPresent()) {
        batchRequest.add((Proc1<T>) signal, signalArgument.get());
      } else {
        batchRequest.add((Proc) signal);
      }

      workflowClientWrapped.signalWithStart(batchRequest);
      log.info("Connection manager workflow for connection {} has been started and signaled.", connectionId);

      return connectionManagerWorkflow;
    }
  }

  void safeTerminateWorkflow(final String workflowId, final String reason) {
    log.info("Attempting to terminate existing workflow for workflowId {}.", workflowId);
    try {
      workflowClientWrapped.terminateWorkflow(workflowId, reason);
    } catch (final Exception e) {
      log.warn(
          "Could not terminate temporal workflow due to the following error; "
              + "this may be because there is currently no running workflow for this connection.",
          e);
    }
  }

  /**
   * Terminate a temporal workflow and throw a useful exception.
   *
   * @param connectionId connection id
   * @param reason reason for terminating the workflow
   */
  // todo (cgardens) - what makes this safe
  public void safeTerminateWorkflow(final UUID connectionId, final String reason) {
    safeTerminateWorkflow(getConnectionManagerName(connectionId), reason);
  }

  /**
   * Start a connection manager workflow for a connection.
   *
   * @param connectionId connection id
   * @return new connection manager workflow
   */
  // todo (cgardens) - what does no signal mean in this context?
  public ConnectionManagerWorkflow startConnectionManagerNoSignal(final UUID connectionId) {
    final ConnectionManagerWorkflow connectionManagerWorkflow = newConnectionManagerWorkflowStub(connectionId);
    final ConnectionUpdaterInput input = TemporalWorkflowUtils.buildStartWorkflowInput(connectionId);
    WorkflowClient.start(connectionManagerWorkflow::run, input);

    return connectionManagerWorkflow;
  }

  /**
   * Attempts to retrieve the connection manager workflow for the provided connection.
   *
   * @param connectionId the ID of the connection whose workflow should be retrieved
   * @return the healthy ConnectionManagerWorkflow
   * @throws DeletedWorkflowException if the workflow was deleted, according to the workflow state
   * @throws UnreachableWorkflowException if the workflow is in an unreachable state
   */
  public ConnectionManagerWorkflow getConnectionManagerWorkflow(final UUID connectionId)
      throws DeletedWorkflowException, UnreachableWorkflowException {

    final ConnectionManagerWorkflow connectionManagerWorkflow;
    final WorkflowState workflowState;
    final WorkflowExecutionStatus workflowExecutionStatus;
    try {
      connectionManagerWorkflow = workflowClientWrapped.newWorkflowStub(ConnectionManagerWorkflow.class, getConnectionManagerName(connectionId));
      workflowState = connectionManagerWorkflow.getState();
      workflowExecutionStatus = getConnectionManagerWorkflowStatus(connectionId);
    } catch (final Exception e) {
      throw new UnreachableWorkflowException(
          String.format("Failed to retrieve ConnectionManagerWorkflow for connection %s due to the following error:", connectionId),
          e);
    }

    if (WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED.equals(workflowExecutionStatus)) {
      if (workflowState.isDeleted()) {
        throw new DeletedWorkflowException(String.format(
            "The connection manager workflow for connection %s is deleted, so no further operations cannot be performed on it.",
            connectionId));
      }

      // A non-deleted workflow being in a COMPLETED state is unexpected, and should be corrected
      throw new UnreachableWorkflowException(
          String.format("ConnectionManagerWorkflow for connection %s is unreachable due to having COMPLETED status.", connectionId));
    }

    return connectionManagerWorkflow;
  }

  Optional<WorkflowState> getWorkflowState(final UUID connectionId) {
    try {
      final ConnectionManagerWorkflow connectionManagerWorkflow = workflowClientWrapped.newWorkflowStub(ConnectionManagerWorkflow.class,
          getConnectionManagerName(connectionId));
      return Optional.of(connectionManagerWorkflow.getState());
    } catch (final Exception e) {
      log.error("Exception thrown while checking workflow state for connection id {}", connectionId, e);
      return Optional.empty();
    }
  }

  boolean isWorkflowStateRunning(final UUID connectionId) {
    return getWorkflowState(connectionId).map(WorkflowState::isRunning).orElse(false);
  }

  /**
   * Get status of a connection manager workflow.
   *
   * @param connectionId connection id
   * @return workflow execution status
   */
  private WorkflowExecutionStatus getConnectionManagerWorkflowStatus(final UUID connectionId) {
    final DescribeWorkflowExecutionRequest describeWorkflowExecutionRequest = DescribeWorkflowExecutionRequest.newBuilder()
        .setExecution(WorkflowExecution.newBuilder()
            .setWorkflowId(getConnectionManagerName(connectionId))
            .build())
        .setNamespace(workflowClientWrapped.getNamespace()).build();

    final DescribeWorkflowExecutionResponse describeWorkflowExecutionResponse =
        workflowClientWrapped.blockingDescribeWorkflowExecution(describeWorkflowExecutionRequest);

    return describeWorkflowExecutionResponse.getWorkflowExecutionInfo().getStatus();
  }

  /**
   * Get the job id for a connection is a workflow is running for it. Otherwise, throws.
   *
   * @param connectionId connection id
   * @return current job id
   */
  public long getCurrentJobId(final UUID connectionId) {
    try {
      final ConnectionManagerWorkflow connectionManagerWorkflow = getConnectionManagerWorkflow(connectionId);
      return connectionManagerWorkflow.getJobInformation().getJobId();
    } catch (final Exception e) {
      return ConnectionManagerWorkflow.NON_RUNNING_JOB_ID;
    }
  }

  private ConnectionManagerWorkflow newConnectionManagerWorkflowStub(final UUID connectionId) {
    return workflowClientWrapped.newWorkflowStub(ConnectionManagerWorkflow.class,
        TemporalWorkflowUtils.buildWorkflowOptions(TemporalJobType.CONNECTION_UPDATER, getConnectionManagerName(connectionId)));
  }

  public String getConnectionManagerName(final UUID connectionId) {
    return "connection_manager_" + connectionId;
  }

}
