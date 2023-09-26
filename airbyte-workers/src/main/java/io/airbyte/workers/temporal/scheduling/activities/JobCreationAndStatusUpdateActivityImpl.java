/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.config.JobConfig.ConfigType.SYNC;
import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;

import com.google.common.annotations.VisibleForTesting;
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
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.UseNewIsLastJobOrAttemptFailure;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.errorreporter.JobErrorReporter;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.persistence.job.tracker.JobTracker;
import io.airbyte.workers.context.AttemptContext;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * JobCreationAndStatusUpdateActivityImpl.
 */
@Slf4j
@Singleton
@Requires(env = WorkerMode.CONTROL_PLANE)
public class JobCreationAndStatusUpdateActivityImpl implements JobCreationAndStatusUpdateActivity {

  private final JobPersistence jobPersistence;
  private final JobNotifier jobNotifier;
  private final JobTracker jobTracker;
  private final JobErrorReporter jobErrorReporter;
  private final JobCreationAndStatusUpdateHelper jobCreationAndStatusUpdateHelper;
  private final JobsApi jobsApi;
  private final AttemptApi attemptApi;
  private final FeatureFlagClient ffClient;

  public JobCreationAndStatusUpdateActivityImpl(final JobPersistence jobPersistence,
                                                final JobNotifier jobNotifier,
                                                final JobTracker jobTracker,
                                                final ConfigRepository configRepository,
                                                final JobErrorReporter jobErrorReporter,
                                                final JobsApi jobsApi,
                                                final AttemptApi attemptApi,
                                                final FeatureFlagClient ffClient) {
    this.jobPersistence = jobPersistence;
    this.jobNotifier = jobNotifier;
    this.jobTracker = jobTracker;
    this.jobErrorReporter = jobErrorReporter;
    this.jobsApi = jobsApi;
    this.attemptApi = attemptApi;
    this.ffClient = ffClient;
    this.jobCreationAndStatusUpdateHelper = new JobCreationAndStatusUpdateHelper(
        jobPersistence,
        configRepository,
        jobNotifier,
        jobTracker);
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
   * isLastJobOrAttemptFailureOld.
   */
  public boolean isLastJobOrAttemptFailureOld(final JobCheckFailureInput input) {
    final int limit = 2;
    boolean lastAttemptCheck = false;
    boolean lastJobCheck = false;

    final Set<ConfigType> configTypes = new HashSet<>();
    configTypes.add(SYNC);

    try {
      final List<Job> jobList = jobPersistence.listJobsIncludingId(configTypes, input.getConnectionId().toString(), input.getJobId(), limit);
      final Optional<Job> optionalActiveJob = jobList.stream().filter(job -> job.getId() == input.getJobId()).findFirst();
      if (optionalActiveJob.isPresent()) {
        lastAttemptCheck = jobCreationAndStatusUpdateHelper.checkActiveJobPreviousAttempt(optionalActiveJob.get(), input.getAttemptId());
      }

      final OptionalLong previousJobId =
          jobCreationAndStatusUpdateHelper.getPreviousJobId(input.getJobId(), jobList.stream().map(Job::getId).toList());
      if (previousJobId.isPresent()) {
        final Optional<Job> optionalPreviousJob = jobList.stream().filter(job -> job.getId() == previousJobId.getAsLong()).findFirst();
        if (optionalPreviousJob.isPresent()) {
          lastJobCheck = optionalPreviousJob.get().getStatus().equals(io.airbyte.persistence.job.models.JobStatus.FAILED);
        }
      }

      return lastJobCheck || lastAttemptCheck;
    } catch (final IOException e) {
      throw new RetryableException(e);
    }
  }

  @Override
  public boolean isLastJobOrAttemptFailure(final JobCheckFailureInput input) {
    if (!ffClient.boolVariation(UseNewIsLastJobOrAttemptFailure.INSTANCE, new Connection(input.getConnectionId()))) {
      return isLastJobOrAttemptFailureOld(input);
    }

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

  @VisibleForTesting
  List<ReleaseStage> getJobToReleaseStages(final Job job) throws IOException {
    return jobCreationAndStatusUpdateHelper.getJobToReleaseStages(job);
  }

  @VisibleForTesting
  static List<ReleaseStage> orderByReleaseStageAsc(final List<ReleaseStage> releaseStages) {
    return JobCreationAndStatusUpdateHelper.orderByReleaseStageAsc(releaseStages);
  }

}
