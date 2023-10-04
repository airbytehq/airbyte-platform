/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;

import datadog.trace.api.Trace;
import io.airbyte.api.client.generated.AttemptApi;
import io.airbyte.api.client.generated.JobsApi;
import io.airbyte.api.client.invoker.generated.ApiException;
import io.airbyte.api.client.model.generated.ConnectionIdRequestBody;
import io.airbyte.api.client.model.generated.ConnectionJobRequestBody;
import io.airbyte.api.client.model.generated.CreateNewAttemptNumberRequest;
import io.airbyte.api.client.model.generated.FailAttemptRequest;
import io.airbyte.api.client.model.generated.JobCreate;
import io.airbyte.api.client.model.generated.JobFailureRequest;
import io.airbyte.api.client.model.generated.JobInfoRead;
import io.airbyte.api.client.model.generated.JobSuccessWithAttemptNumberRequest;
import io.airbyte.api.client.model.generated.PersistCancelJobRequestBody;
import io.airbyte.api.client.model.generated.ReportJobStartRequest;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.workers.context.AttemptContext;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * JobCreationAndStatusUpdateActivityImpl.
 */
@Slf4j
@Singleton
@Requires(env = WorkerMode.CONTROL_PLANE)
public class JobCreationAndStatusUpdateActivityImpl implements JobCreationAndStatusUpdateActivity {

  private final JobsApi jobsApi;
  private final AttemptApi attemptApi;

  public JobCreationAndStatusUpdateActivityImpl(final JobsApi jobsApi,
                                                final AttemptApi attemptApi) {
    this.jobsApi = jobsApi;
    this.attemptApi = attemptApi;
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public JobCreationOutput createNewJob(final JobCreationInput input) {
    new AttemptContext(input.getConnectionId(), null, null).addTagsToTrace();
    try {
      final JobInfoRead jobInfoRead = jobsApi.createJob(new JobCreate().connectionId(input.getConnectionId()));
      return new JobCreationOutput(jobInfoRead.getJob().getId());
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      log.error("Unable to create job for connection {}", input.getConnectionId(), e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public AttemptNumberCreationOutput createNewAttemptNumber(final AttemptCreationInput input) throws RetryableException {
    new AttemptContext(null, input.getJobId(), null).addTagsToTrace();

    try {
      final long jobId = input.getJobId();
      final var response = attemptApi.createNewAttemptNumber(new CreateNewAttemptNumberRequest().jobId(jobId));
      return new AttemptNumberCreationOutput(response.getAttemptNumber());
    } catch (final Exception e) {
      ApmTraceUtils.addExceptionToTrace(e);
      log.error("createNewAttemptNumber for job {} failed with exception: {}", input.getJobId(), e.getMessage(), e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void jobSuccessWithAttemptNumber(final JobSuccessInputWithAttemptNumber input) {
    new AttemptContext(input.getConnectionId(), input.getJobId(), input.getAttemptNumber()).addTagsToTrace();

    try {
      final var request = new JobSuccessWithAttemptNumberRequest()
          .jobId(input.getJobId())
          .attemptNumber(input.getAttemptNumber())
          .connectionId(input.getConnectionId())
          .standardSyncOutput(input.getStandardSyncOutput());
      jobsApi.jobSuccessWithAttemptNumber(request);
    } catch (final ApiException e) {
      ApmTraceUtils.addExceptionToTrace(e);
      log.error("jobSuccessWithAttemptNumber for job {} failed with exception: {}", input.getJobId(), e.getMessage(), e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void jobFailure(final JobFailureInput input) {
    new AttemptContext(input.getConnectionId(), input.getJobId(), input.getAttemptNumber()).addTagsToTrace();

    try {
      final var request = new JobFailureRequest()
          .attemptNumber(input.getAttemptNumber())
          .connectionId(input.getConnectionId())
          .jobId(input.getJobId())
          .reason(input.getReason());
      jobsApi.jobFailure(request);
    } catch (final ApiException e) {
      log.error("jobFailure for job {} attempt {} failed with exception: {}", input.getJobId(), input.getAttemptNumber(), e.getMessage(), e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void attemptFailureWithAttemptNumber(final AttemptNumberFailureInput input) {
    new AttemptContext(input.getConnectionId(), input.getJobId(), input.getAttemptNumber()).addTagsToTrace();

    try {
      final var req = new FailAttemptRequest()
          .attemptNumber(input.getAttemptNumber())
          .jobId(input.getJobId())
          .failureSummary(input.getAttemptFailureSummary())
          .standardSyncOutput(input.getStandardSyncOutput());

      attemptApi.failAttempt(req);
    } catch (final ApiException e) {
      log.error("attemptFailureWithAttemptNumber for job {} failed with exception: {}", input.getJobId(), e.getMessage(), e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void jobCancelledWithAttemptNumber(final JobCancelledInputWithAttemptNumber input) {
    new AttemptContext(input.getConnectionId(), input.getJobId(), input.getAttemptNumber()).addTagsToTrace();

    try {
      final var req = new PersistCancelJobRequestBody()
          .connectionId(input.getConnectionId())
          .jobId(input.getJobId())
          .attemptNumber(input.getAttemptNumber())
          .attemptFailureSummary(input.getAttemptFailureSummary());

      jobsApi.persistJobCancellation(req);
    } catch (final ApiException e) {
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void reportJobStart(final ReportJobStartInput input) {
    new AttemptContext(input.getConnectionId(), input.getJobId(), null).addTagsToTrace();

    try {
      jobsApi.reportJobStart(new ReportJobStartRequest().connectionId(input.getConnectionId()).jobId(input.getJobId()));
    } catch (final ApiException e) {
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void ensureCleanJobState(final EnsureCleanJobStateInput input) {
    new AttemptContext(input.getConnectionId(), null, null).addTagsToTrace();
    try {
      jobsApi.failNonTerminalJobs(new ConnectionIdRequestBody().connectionId(input.getConnectionId()));
    } catch (final ApiException e) {
      throw new RetryableException(e);
    }
  }

  /**
   * This method is used to determine if the current job is the last job or attempt failure.
   *
   * @param input - JobCheckFailureInput.
   * @return - boolean.
   */
  @Override
  public boolean isLastJobOrAttemptFailure(final JobCheckFailureInput input) {
    // If there has been a previous attempt, that means it failed. We don't create subsequent attempts
    // on success.
    final var isNotFirstAttempt = input.getAttemptId() > 0;
    if (isNotFirstAttempt) {
      return true;
    }

    try {
      final var didSucceed = jobsApi.didPreviousJobSucceed(
          new ConnectionJobRequestBody()
              .connectionId(input.getConnectionId())
              .jobId(input.getJobId()))
          .getValue();
      // Treat anything other than an explicit success as a failure.
      return !didSucceed;
    } catch (final ApiException e) {
      throw new RetryableException(e);
    }
  }

}
