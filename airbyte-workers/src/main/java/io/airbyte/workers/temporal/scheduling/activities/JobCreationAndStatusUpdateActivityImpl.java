/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.temporal.scheduling.activities;

import static io.airbyte.config.JobConfig.ConfigType.SYNC;
import static io.airbyte.metrics.lib.ApmTraceConstants.ACTIVITY_TRACE_OPERATION_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import datadog.trace.api.Trace;
import io.airbyte.commons.server.JobStatus;
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.commons.temporal.config.WorkerMode;
import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.commons.version.Version;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.Configs.WorkerEnvironment;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobOutput;
import io.airbyte.config.JobResetConnectionConfig;
import io.airbyte.config.JobSyncConfig;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOperation;
import io.airbyte.config.helpers.LogClientSingleton;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.ConfigNotFoundException;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.config.persistence.StreamResetPersistence;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.JobCreator;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.errorreporter.JobErrorReporter;
import io.airbyte.persistence.job.errorreporter.SyncJobReportingContext;
import io.airbyte.persistence.job.factory.OAuthConfigSupplier;
import io.airbyte.persistence.job.factory.SyncJobFactory;
import io.airbyte.persistence.job.models.Attempt;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.persistence.job.tracker.JobTracker;
import io.airbyte.persistence.job.tracker.JobTracker.JobState;
import io.airbyte.protocol.models.StreamDescriptor;
import io.airbyte.validation.json.JsonValidationException;
import io.airbyte.workers.context.AttemptContext;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

/**
 * JobCreationAndStatusUpdateActivityImpl.
 */
@SuppressWarnings("ParameterName")
@Slf4j
@Singleton
@Requires(env = WorkerMode.CONTROL_PLANE)
public class JobCreationAndStatusUpdateActivityImpl implements JobCreationAndStatusUpdateActivity {

  private final SyncJobFactory jobFactory;
  private final JobPersistence jobPersistence;
  private final Path workspaceRoot;
  private final WorkerEnvironment workerEnvironment;
  private final LogConfigs logConfigs;
  private final JobNotifier jobNotifier;
  private final JobTracker jobTracker;
  private final ConfigRepository configRepository;
  private final JobCreator jobCreator;
  private final StreamResetPersistence streamResetPersistence;
  private final JobErrorReporter jobErrorReporter;
  private final OAuthConfigSupplier oAuthConfigSupplier;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final JobCreationAndStatusUpdateHelper jobCreationAndStatusUpdateHelper;

  public JobCreationAndStatusUpdateActivityImpl(final SyncJobFactory jobFactory,
                                                final JobPersistence jobPersistence,
                                                @Named("workspaceRoot") final Path workspaceRoot,
                                                final WorkerEnvironment workerEnvironment,
                                                final LogConfigs logConfigs,
                                                final JobNotifier jobNotifier,
                                                final JobTracker jobTracker,
                                                final ConfigRepository configRepository,
                                                final JobCreator jobCreator,
                                                final StreamResetPersistence streamResetPersistence,
                                                final JobErrorReporter jobErrorReporter,
                                                final OAuthConfigSupplier oAuthConfigSupplier,
                                                final ActorDefinitionVersionHelper actorDefinitionVersionHelper) {
    this.jobFactory = jobFactory;
    this.jobPersistence = jobPersistence;
    this.workspaceRoot = workspaceRoot;
    this.workerEnvironment = workerEnvironment;
    this.logConfigs = logConfigs;
    this.jobNotifier = jobNotifier;
    this.jobTracker = jobTracker;
    this.configRepository = configRepository;
    this.jobCreator = jobCreator;
    this.streamResetPersistence = streamResetPersistence;
    this.jobErrorReporter = jobErrorReporter;
    this.oAuthConfigSupplier = oAuthConfigSupplier;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
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
      // Fail non-terminal jobs first to prevent this activity from repeatedly trying to create a new job
      // and failing, potentially resulting in the workflow ending up in a quarantined state.
      // Another non-terminal job is not expected to exist at this point in the normal case, but this
      // could happen in special edge cases for example when migrating to this from the old scheduler.
      jobCreationAndStatusUpdateHelper.failNonTerminalJobs(input.getConnectionId());

      final StandardSync standardSync = configRepository.getStandardSync(input.getConnectionId());
      final List<StreamDescriptor> streamsToReset = streamResetPersistence.getStreamResets(input.getConnectionId());
      log.info("Found the following streams to reset for connection {}: {}", input.getConnectionId(), streamsToReset);

      if (!streamsToReset.isEmpty()) {
        final DestinationConnection destination = configRepository.getDestinationConnection(standardSync.getDestinationId());

        final JsonNode destinationConfiguration = oAuthConfigSupplier.injectDestinationOAuthParameters(
            destination.getDestinationDefinitionId(),
            destination.getDestinationId(),
            destination.getWorkspaceId(),
            destination.getConfiguration());
        destination.setConfiguration(destinationConfiguration);

        final StandardDestinationDefinition destinationDef =
            configRepository.getStandardDestinationDefinition(destination.getDestinationDefinitionId());
        final ActorDefinitionVersion destinationVersion =
            actorDefinitionVersionHelper.getDestinationVersion(destinationDef, destination.getWorkspaceId(), destination.getDestinationId());
        final String destinationImageName = destinationVersion.getDockerRepository() + ":" + destinationVersion.getDockerImageTag();

        final List<StandardSyncOperation> standardSyncOperations = Lists.newArrayList();
        for (final var operationId : standardSync.getOperationIds()) {
          final StandardSyncOperation standardSyncOperation = configRepository.getStandardSyncOperation(operationId);
          standardSyncOperations.add(standardSyncOperation);
        }

        final Optional<Long> jobIdOptional =
            jobCreator.createResetConnectionJob(
                destination,
                standardSync,
                destinationVersion,
                destinationImageName,
                new Version(destinationVersion.getProtocolVersion()),
                destinationDef.getCustom(),
                standardSyncOperations, streamsToReset);

        final long jobId = jobIdOptional.isEmpty()
            ? jobPersistence.getLastReplicationJob(standardSync.getConnectionId()).orElseThrow(() -> new RuntimeException("No job available")).getId()
            : jobIdOptional.get();

        return new JobCreationOutput(jobId);
      } else {
        final long jobId = jobFactory.create(input.getConnectionId());

        log.info("New job created, with id: " + jobId);
        final Job job = jobPersistence.getJob(jobId);
        jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.JOB_CREATED_BY_RELEASE_STAGE, job);

        return new JobCreationOutput(jobId);
      }
    } catch (final JsonValidationException | ConfigNotFoundException | IOException e) {
      log.error("createNewJob for connection {} failed with exception: {}", input.getConnectionId(), e.getMessage(), e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public AttemptNumberCreationOutput createNewAttemptNumber(final AttemptCreationInput input) throws RetryableException {
    new AttemptContext(null, input.getJobId(), null).addTagsToTrace();

    try {
      final long jobId = input.getJobId();
      final Job job = jobPersistence.getJob(jobId);

      final Path jobRoot = TemporalUtils.getJobRoot(workspaceRoot, String.valueOf(jobId), job.getAttemptsCount());
      final Path logFilePath = jobRoot.resolve(LogClientSingleton.LOG_FILENAME);
      final int persistedAttemptNumber = jobPersistence.createAttempt(jobId, logFilePath);
      jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.ATTEMPT_CREATED_BY_RELEASE_STAGE, job);
      jobCreationAndStatusUpdateHelper.emitAttemptCreatedEvent(job, persistedAttemptNumber);

      LogClientSingleton.getInstance().setJobMdc(workerEnvironment, logConfigs, jobRoot);
      return new AttemptNumberCreationOutput(persistedAttemptNumber);
    } catch (final IOException e) {
      log.error("createNewAttemptNumber for job {} failed with exception: {}", input.getJobId(), e.getMessage(), e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void jobSuccessWithAttemptNumber(final JobSuccessInputWithAttemptNumber input) {
    new AttemptContext(input.getConnectionId(), input.getJobId(), input.getAttemptNumber()).addTagsToTrace();

    try {
      final long jobId = input.getJobId();
      final int attemptNumber = input.getAttemptNumber();

      if (input.getStandardSyncOutput() != null) {
        final JobOutput jobOutput = new JobOutput().withSync(input.getStandardSyncOutput());
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
    } catch (final IOException e) {
      jobCreationAndStatusUpdateHelper.trackCompletionForInternalFailure(input.getJobId(), input.getConnectionId(), input.getAttemptNumber(),
          JobStatus.SUCCEEDED, e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void jobFailure(final JobFailureInput input) {
    new AttemptContext(input.getConnectionId(), input.getJobId(), input.getAttemptNumber()).addTagsToTrace();

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
      job.getLastFailedAttempt().flatMap(Attempt::getFailureSummary)
          .ifPresent(failureSummary -> jobErrorReporter.reportSyncJobFailure(connectionId, failureSummary, jobContext));
      jobCreationAndStatusUpdateHelper.trackCompletion(job, JobStatus.FAILED);
    } catch (final IOException e) {
      jobCreationAndStatusUpdateHelper.trackCompletionForInternalFailure(input.getJobId(), input.getConnectionId(), input.getAttemptNumber(),
          JobStatus.FAILED, e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void attemptFailureWithAttemptNumber(final AttemptNumberFailureInput input) {
    new AttemptContext(input.getConnectionId(), input.getJobId(), input.getAttemptNumber()).addTagsToTrace();

    try {
      final int attemptNumber = input.getAttemptNumber();
      final long jobId = input.getJobId();
      final AttemptFailureSummary failureSummary = input.getAttemptFailureSummary();

      jobCreationAndStatusUpdateHelper.traceFailures(failureSummary);

      jobPersistence.failAttempt(jobId, attemptNumber);
      jobPersistence.writeAttemptFailureSummary(jobId, attemptNumber, failureSummary);

      if (input.getStandardSyncOutput() != null) {
        final JobOutput jobOutput = new JobOutput().withSync(input.getStandardSyncOutput());
        jobPersistence.writeOutput(jobId, attemptNumber, jobOutput);
      }

      final Job job = jobPersistence.getJob(jobId);
      jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.ATTEMPT_FAILED_BY_RELEASE_STAGE, job);
      jobCreationAndStatusUpdateHelper.trackFailures(failureSummary);
    } catch (final IOException e) {
      log.error("attemptFailureWithAttemptNumber for job {} failed with exception: {}", input.getJobId(), e.getMessage(), e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void jobCancelledWithAttemptNumber(final JobCancelledInputWithAttemptNumber input) {
    new AttemptContext(input.getConnectionId(), input.getJobId(), input.getAttemptNumber()).addTagsToTrace();

    try {
      final long jobId = input.getJobId();
      final int attemptNumber = input.getAttemptNumber();
      jobPersistence.failAttempt(jobId, attemptNumber);
      jobPersistence.writeAttemptFailureSummary(jobId, attemptNumber, input.getAttemptFailureSummary());
      jobPersistence.cancelJob(jobId);

      final Job job = jobPersistence.getJob(jobId);
      jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.JOB_CANCELLED_BY_RELEASE_STAGE, job);
      jobNotifier.failJob("Job was cancelled", job);
      jobCreationAndStatusUpdateHelper.trackCompletion(job, JobStatus.FAILED);
    } catch (final IOException e) {
      jobCreationAndStatusUpdateHelper.trackCompletionForInternalFailure(input.getJobId(), input.getConnectionId(), input.getAttemptNumber(),
          JobStatus.FAILED, e);
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void reportJobStart(final ReportJobStartInput input) {
    new AttemptContext(input.getConnectionId(), input.getJobId(), null).addTagsToTrace();

    try {
      final Job job = jobPersistence.getJob(input.getJobId());
      jobTracker.trackSync(job, JobState.STARTED);
    } catch (final IOException e) {
      throw new RetryableException(e);
    }
  }

  @Trace(operationName = ACTIVITY_TRACE_OPERATION_NAME)
  @Override
  public void ensureCleanJobState(final EnsureCleanJobStateInput input) {
    new AttemptContext(input.getConnectionId(), null, null).addTagsToTrace();
    jobCreationAndStatusUpdateHelper.failNonTerminalJobs(input.getConnectionId());
  }

  @Override
  public boolean isLastJobOrAttemptFailure(final JobCheckFailureInput input) {
    final int limit = 2;
    boolean lastAttemptCheck = false;
    boolean lastJobCheck = false;

    final Set<JobConfig.ConfigType> configTypes = new HashSet<>();
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

  @VisibleForTesting
  List<ReleaseStage> getJobToReleaseStages(final Job job) throws IOException {
    return jobCreationAndStatusUpdateHelper.getJobToReleaseStages(job);
  }

  @VisibleForTesting
  static List<ReleaseStage> orderByReleaseStageAsc(final List<ReleaseStage> releaseStages) {
    return JobCreationAndStatusUpdateHelper.orderByReleaseStageAsc(releaseStages);
  }

}
