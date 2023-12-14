/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper.SYNC_CONFIG_SET;

import io.airbyte.api.model.generated.BooleanRead;
import io.airbyte.api.model.generated.InternalOperationResult;
import io.airbyte.api.model.generated.JobFailureRequest;
import io.airbyte.api.model.generated.JobSuccessWithAttemptNumberRequest;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.JobStatus;
import io.airbyte.commons.server.errors.BadRequestException;
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.AttemptSyncConfig;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.errorreporter.AttemptConfigReportingContext;
import io.airbyte.persistence.job.errorreporter.JobErrorReporter;
import io.airbyte.persistence.job.errorreporter.SyncJobReportingContext;
import io.airbyte.persistence.job.models.Attempt;
import io.airbyte.persistence.job.models.Job;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AttemptHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
public class JobsHandler {

  private static final Logger log = LoggerFactory.getLogger(JobsHandler.class);
  private final JobPersistence jobPersistence;
  private final JobCreationAndStatusUpdateHelper jobCreationAndStatusUpdateHelper;
  private final JobNotifier jobNotifier;
  private final JobErrorReporter jobErrorReporter;

  public JobsHandler(final JobPersistence jobPersistence,
                     final JobCreationAndStatusUpdateHelper jobCreationAndStatusUpdateHelper,
                     final JobNotifier jobNotifier,
                     final JobErrorReporter jobErrorReporter) {
    this.jobPersistence = jobPersistence;
    this.jobCreationAndStatusUpdateHelper = jobCreationAndStatusUpdateHelper;
    this.jobNotifier = jobNotifier;
    this.jobErrorReporter = jobErrorReporter;
  }

  /**
   * Mark job as failure.
   *
   * @param input - the request object.
   * @return - the result of the operation.
   */
  public InternalOperationResult jobFailure(final JobFailureRequest input) {
    try {
      final long jobId = input.getJobId();
      jobPersistence.failJob(jobId);
      final Job job = jobPersistence.getJob(jobId);

      jobNotifier.failJob(input.getReason(), job);
      jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.JOB_FAILED_BY_RELEASE_STAGE, job);

      final UUID connectionId = UUID.fromString(job.getScope());
      if (!connectionId.equals(input.getConnectionId())) {
        log.warn("inconsistent connectionId for jobId '{}' (input:'{}', db:'{}')", jobId, input.getConnectionId(), connectionId);
        MetricClientFactory.getMetricClient().count(OssMetricsRegistry.INCONSISTENT_ACTIVITY_INPUT, 1);
      }

      final JobSyncConfig jobSyncConfig = job.getConfig().getSync();
      final UUID destinationDefinitionVersionId;
      final UUID sourceDefinitionVersionId;
      if (jobSyncConfig == null) {
        final JobResetConnectionConfig resetConfig = job.getConfig().getResetConnection();
        // In a reset, we run a fake source
        sourceDefinitionVersionId = null;
        destinationDefinitionVersionId = resetConfig != null ? resetConfig.getDestinationDefinitionVersionId() : null;
      } else {
        sourceDefinitionVersionId = jobSyncConfig.getSourceDefinitionVersionId();
        destinationDefinitionVersionId = jobSyncConfig.getDestinationDefinitionVersionId();
      }
      final SyncJobReportingContext jobContext = new SyncJobReportingContext(jobId, sourceDefinitionVersionId, destinationDefinitionVersionId);
      reportIfLastFailedAttempt(job, connectionId, jobContext);
      jobCreationAndStatusUpdateHelper.trackCompletion(job, JobStatus.FAILED);
      return new InternalOperationResult().succeeded(true);
    } catch (final IOException e) {
      jobCreationAndStatusUpdateHelper.trackCompletionForInternalFailure(input.getJobId(), input.getConnectionId(), input.getAttemptNumber(),
          JobStatus.FAILED, e);
      throw new RuntimeException(e);
    }
  }

  private void reportIfLastFailedAttempt(Job job, UUID connectionId, SyncJobReportingContext jobContext) {
    Optional<Attempt> lastFailedAttempt = job.getLastFailedAttempt();
    if (lastFailedAttempt.isPresent()) {
      Attempt attempt = lastFailedAttempt.get();
      Optional<AttemptFailureSummary> failureSummaryOpt = attempt.getFailureSummary();

      if (failureSummaryOpt.isPresent()) {
        AttemptFailureSummary failureSummary = failureSummaryOpt.get();
        AttemptConfigReportingContext attemptConfig = null;

        Optional<AttemptSyncConfig> syncConfigOpt = attempt.getSyncConfig();
        if (syncConfigOpt.isPresent()) {
          AttemptSyncConfig syncConfig = syncConfigOpt.get();
          attemptConfig = new AttemptConfigReportingContext(
              syncConfig.getSourceConfiguration(),
              syncConfig.getDestinationConfiguration(),
              syncConfig.getState());
        }

        jobErrorReporter.reportSyncJobFailure(connectionId, failureSummary, jobContext, attemptConfig);
      }
    }
  }

  /**
   * Report a job and a given attempt as successful.
   */
  public InternalOperationResult jobSuccessWithAttemptNumber(final JobSuccessWithAttemptNumberRequest input) {
    try {
      final long jobId = input.getJobId();
      final int attemptNumber = input.getAttemptNumber();

      if (input.getStandardSyncOutput() != null) {
        final JobOutput jobOutput = new JobOutput().withSync(Jsons.convertValue(input.getStandardSyncOutput(), StandardSyncOutput.class));
        jobPersistence.writeOutput(jobId, attemptNumber, jobOutput);
      } else {
        log.warn("The job {} doesn't have any output for the attempt {}", jobId, attemptNumber);
      }
      jobPersistence.succeedAttempt(jobId, attemptNumber);
      final Job job = jobPersistence.getJob(jobId);
      jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.ATTEMPT_SUCCEEDED_BY_RELEASE_STAGE, job);

      jobNotifier.successJob(job);
      jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.JOB_SUCCEEDED_BY_RELEASE_STAGE, job);
      jobCreationAndStatusUpdateHelper.trackCompletion(job, JobStatus.SUCCEEDED);

      return new InternalOperationResult().succeeded(true);
    } catch (final IOException e) {
      jobCreationAndStatusUpdateHelper.trackCompletionForInternalFailure(input.getJobId(), input.getConnectionId(), input.getAttemptNumber(),
          JobStatus.SUCCEEDED, e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Fail non terminal jobs.
   *
   * @param connectionId - the connection id.
   * @throws IOException - exception.
   *
   */
  public void failNonTerminalJobs(final UUID connectionId) throws IOException {
    jobCreationAndStatusUpdateHelper.failNonTerminalJobs(connectionId);
  }

  /**
   * Report a job as started.
   */
  public InternalOperationResult reportJobStart(final Long jobId) throws IOException {
    jobCreationAndStatusUpdateHelper.reportJobStart(jobId);
    return new InternalOperationResult().succeeded(true);
  }

  /**
   * Did previous job succeed.
   *
   * @param connectionId - the connection id.
   * @param jobId - the job id.
   * @return - the result of the operation.
   * @throws IOException - exception.
   */
  public BooleanRead didPreviousJobSucceed(final UUID connectionId, final long jobId) throws IOException {
    // This DB call is a lift-n-shift from activity code to move database access out of the worker. It
    // is knowingly brittle and awkward. By setting pageSize to 2 this should just fetch the latest and
    // preceding job, but technically can fetch a much longer list.
    final List<Job> jobs = jobPersistence.listJobsIncludingId(SYNC_CONFIG_SET, connectionId.toString(), jobId, 2);

    final boolean previousJobSucceeded = jobCreationAndStatusUpdateHelper.findPreviousJob(jobs, jobId)
        .map(jobCreationAndStatusUpdateHelper::didJobSucceed)
        .orElse(false);

    return new BooleanRead().value(previousJobSucceeded);
  }

  /**
   * Records the cancellation of a job and its ongoing attempt. Kicks off notification and other
   * post-processing.
   */
  public void persistJobCancellation(final UUID connectionId, final long jobId, final int attemptNumber, final Object rawFailureSummary) {
    AttemptFailureSummary failureSummary = null;
    if (rawFailureSummary != null) {
      try {
        failureSummary = Jsons.convertValue(rawFailureSummary, AttemptFailureSummary.class);
      } catch (final Exception e) {
        throw new BadRequestException("Unable to parse failureSummary.", e);
      }
    }

    try {
      // fail attempt
      jobPersistence.failAttempt(jobId, attemptNumber);
      jobPersistence.writeAttemptFailureSummary(jobId, attemptNumber, failureSummary);
      // persist cancellation
      jobPersistence.cancelJob(jobId);
      // post process
      final var job = jobPersistence.getJob(jobId);
      jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.JOB_CANCELLED_BY_RELEASE_STAGE, job);
      jobNotifier.failJob("Job was cancelled", job);
      jobCreationAndStatusUpdateHelper.trackCompletion(job, JobStatus.FAILED);
    } catch (final IOException e) {
      jobCreationAndStatusUpdateHelper.trackCompletionForInternalFailure(jobId, connectionId, attemptNumber,
          JobStatus.FAILED, e);
      throw new RuntimeException(e);
    }
  }

}
