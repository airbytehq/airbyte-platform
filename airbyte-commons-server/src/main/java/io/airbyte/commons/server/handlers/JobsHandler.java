/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import io.airbyte.commons.server.handlers.helpers.ConnectionTimelineEventHelper;
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper;
import io.airbyte.config.Attempt;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.AttemptSyncConfig;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.featureflag.MergeStreamStatWithMetadata;
import io.airbyte.metrics.MetricClient;
import io.airbyte.metrics.OssMetricsRegistry;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.errorreporter.AttemptConfigReportingContext;
import io.airbyte.persistence.job.errorreporter.JobErrorReporter;
import io.airbyte.persistence.job.errorreporter.SyncJobReportingContext;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
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
  private final ConnectionTimelineEventHelper connectionTimelineEventHelper;
  private final FeatureFlagClient featureFlagClient;
  private final MetricClient metricClient;

  public JobsHandler(final JobPersistence jobPersistence,
                     final JobCreationAndStatusUpdateHelper jobCreationAndStatusUpdateHelper,
                     final JobNotifier jobNotifier,
                     final JobErrorReporter jobErrorReporter,
                     final ConnectionTimelineEventHelper connectionTimelineEventHelper,
                     final FeatureFlagClient featureFlagClient,
                     final MetricClient metricClient) {
    this.jobPersistence = jobPersistence;
    this.jobCreationAndStatusUpdateHelper = jobCreationAndStatusUpdateHelper;
    this.jobNotifier = jobNotifier;
    this.jobErrorReporter = jobErrorReporter;
    this.connectionTimelineEventHelper = connectionTimelineEventHelper;
    this.featureFlagClient = featureFlagClient;
    this.metricClient = metricClient;
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

      final List<JobPersistence.AttemptStats> attemptStats = new ArrayList<>();
      for (final Attempt attempt : job.getAttempts()) {
        final boolean mergeStatsWithStreamMetadata = featureFlagClient.boolVariation(MergeStreamStatWithMetadata.INSTANCE,
            new Connection(input.getConnectionId()));
        if (mergeStatsWithStreamMetadata) {
          attemptStats.add(jobPersistence.getAttemptStatsWithStreamMetadata(jobId, attempt.getAttemptNumber()));
        } else {
          attemptStats.add(jobPersistence.getAttemptStats(jobId, attempt.getAttemptNumber()));
        }
      }
      if (job.getConfigType().equals(JobConfig.ConfigType.SYNC)) {
        jobNotifier.failJob(job, attemptStats);
      }
      connectionTimelineEventHelper.logJobFailureEventInConnectionTimeline(job, input.getConnectionId(), attemptStats);

      jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.JOB_FAILED_BY_RELEASE_STAGE, job, input);

      final UUID connectionId = UUID.fromString(job.getScope());
      if (!connectionId.equals(input.getConnectionId())) {
        log.warn("inconsistent connectionId for jobId '{}' (input:'{}', db:'{}')", jobId, input.getConnectionId(), connectionId);
        metricClient.count(OssMetricsRegistry.INCONSISTENT_ACTIVITY_INPUT);
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

  private void reportIfLastFailedAttempt(final Job job, final UUID connectionId, final SyncJobReportingContext jobContext) {
    final Optional<Attempt> lastFailedAttempt = job.getLastFailedAttempt();
    if (lastFailedAttempt.isPresent()) {
      final Attempt attempt = lastFailedAttempt.get();
      final Optional<AttemptFailureSummary> failureSummaryOpt = attempt.getFailureSummary();

      if (failureSummaryOpt.isPresent()) {
        final AttemptFailureSummary failureSummary = failureSummaryOpt.get();
        AttemptConfigReportingContext attemptConfig = null;

        final Optional<AttemptSyncConfig> syncConfigOpt = attempt.getSyncConfig();
        if (syncConfigOpt.isPresent()) {
          final AttemptSyncConfig syncConfig = syncConfigOpt.get();
          attemptConfig = new AttemptConfigReportingContext(
              syncConfig.getSourceConfiguration(),
              syncConfig.getDestinationConfiguration(),
              syncConfig.getState());
        }

        jobErrorReporter.reportSyncJobFailure(connectionId, failureSummary, jobContext, attemptConfig);
        log.info("Successfully reported failure for job id '{}' connectionId: '{}'", job.getId(), connectionId);
      } else {
        log.info("Failure summary is missing, skipping reporting for jobId '{}', connectionId '{}'", job.getId(), connectionId);
      }
    } else {
      log.info("Last failed attempt is missing, skipping reporting for jobId '{}', connectionId '{}'", job.getId(), connectionId);
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

      final List<JobPersistence.AttemptStats> attemptStats = new ArrayList<>();
      for (final Attempt attempt : job.getAttempts()) {
        final boolean mergeStatsWithStreamMetadata = featureFlagClient.boolVariation(MergeStreamStatWithMetadata.INSTANCE,
            new Connection(input.getConnectionId()));
        if (mergeStatsWithStreamMetadata) {
          attemptStats.add(jobPersistence.getAttemptStatsWithStreamMetadata(jobId, attempt.getAttemptNumber()));
        } else {
          attemptStats.add(jobPersistence.getAttemptStats(jobId, attempt.getAttemptNumber()));
        }
      }
      if (job.getConfigType().equals(JobConfig.ConfigType.SYNC)) {
        jobNotifier.successJob(job, attemptStats);
      }
      connectionTimelineEventHelper.logJobSuccessEventInConnectionTimeline(job, input.getConnectionId(), attemptStats);
      jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.JOB_SUCCEEDED_BY_RELEASE_STAGE, job, input);
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
      final List<JobPersistence.AttemptStats> attemptStats = new ArrayList<>();
      for (final Attempt attempt : job.getAttempts()) {
        attemptStats.add(jobPersistence.getAttemptStats(jobId, attempt.getAttemptNumber()));
      }
      jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.JOB_CANCELLED_BY_RELEASE_STAGE, job);
      jobCreationAndStatusUpdateHelper.trackCompletion(job, JobStatus.FAILED);
    } catch (final IOException e) {
      jobCreationAndStatusUpdateHelper.trackCompletionForInternalFailure(jobId, connectionId, attemptNumber,
          JobStatus.FAILED, e);
      throw new RuntimeException(e);
    }
  }

}
