/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper.SYNC_CONFIG_SET;

import io.airbyte.api.model.generated.BooleanRead;
import io.airbyte.api.model.generated.InternalOperationResult;
import io.airbyte.api.model.generated.JobSuccessWithAttemptNumberRequest;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.JobStatus;
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.config.JobOutput;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.models.Job;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.List;
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

  public JobsHandler(final JobPersistence jobPersistence,
                     final JobCreationAndStatusUpdateHelper jobCreationAndStatusUpdateHelper,
                     final JobNotifier jobNotifier) {
    this.jobPersistence = jobPersistence;
    this.jobCreationAndStatusUpdateHelper = jobCreationAndStatusUpdateHelper;
    this.jobNotifier = jobNotifier;
  }

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
      throw new RetryableException(e);
    }
  }

  public void failNonTerminalJobs(final UUID connectionId) throws IOException {
    jobCreationAndStatusUpdateHelper.failNonTerminalJobs(connectionId);
  }

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

}
