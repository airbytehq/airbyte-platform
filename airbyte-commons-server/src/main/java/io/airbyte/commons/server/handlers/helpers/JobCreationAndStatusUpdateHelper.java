/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers.helpers;

import static io.airbyte.config.JobConfig.ConfigType.SYNC;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.FAILURE_ORIGINS_KEY;
import static io.airbyte.metrics.lib.ApmTraceConstants.Tags.FAILURE_TYPES_KEY;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.airbyte.commons.enums.Enums;
import io.airbyte.commons.server.JobStatus;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.FailureReason;
import io.airbyte.config.FailureReason.FailureOrigin;
import io.airbyte.config.FailureReason.FailureType;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.Metadata;
import io.airbyte.config.ReleaseStage;
import io.airbyte.config.persistence.ConfigRepository;
import io.airbyte.metrics.lib.ApmTraceUtils;
import io.airbyte.metrics.lib.MetricAttribute;
import io.airbyte.metrics.lib.MetricClientFactory;
import io.airbyte.metrics.lib.MetricTags;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.JobNotifier;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.persistence.job.models.Attempt;
import io.airbyte.persistence.job.models.Job;
import io.airbyte.persistence.job.tracker.JobTracker;
import io.airbyte.persistence.job.tracker.JobTracker.JobState;
import io.micronaut.core.util.CollectionUtils;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper class to handle and track job creation and status updates.
 */
@Slf4j
@Singleton
public class JobCreationAndStatusUpdateHelper {

  private static final String JOB_ID_METADATA_KEY = "jobId";
  private static final String ATTEMPT_NUMBER_METADATA_KEY = "attemptNumber";

  private static final int MAX_ATTEMPTS = 3;
  private static final Map<ReleaseStage, Integer> RELEASE_STAGE_ORDER = Map.of(
      ReleaseStage.CUSTOM, 1,
      ReleaseStage.ALPHA, 2,
      ReleaseStage.BETA, 3,
      ReleaseStage.GENERALLY_AVAILABLE, 4);
  private static final Comparator<ReleaseStage> RELEASE_STAGE_COMPARATOR = Comparator.comparingInt(RELEASE_STAGE_ORDER::get);
  public static final Set<ConfigType> SYNC_CONFIG_SET = Set.of(SYNC);

  private final JobPersistence jobPersistence;
  private final ConfigRepository configRepository;
  private final JobNotifier jobNotifier;
  private final JobTracker jobTracker;

  public JobCreationAndStatusUpdateHelper(final JobPersistence jobPersistence,
                                          final ConfigRepository configRepository,
                                          final JobNotifier jobNotifier,
                                          final JobTracker jobTracker) {
    this.jobPersistence = jobPersistence;
    this.configRepository = configRepository;
    this.jobNotifier = jobNotifier;
    this.jobTracker = jobTracker;
  }

  @VisibleForTesting
  public static List<ReleaseStage> orderByReleaseStageAsc(final List<ReleaseStage> releaseStages) {
    return releaseStages.stream()
        .filter(stage -> stage != null)
        .sorted(RELEASE_STAGE_COMPARATOR)
        .toList();
  }

  /**
   * Extract the attempt number from an attempt. If the number is anonymous (not 0,1,2,3) for some
   * reason return null. We don't want to accidentally have high cardinality here because of a bug.
   *
   * @param attemptNumber - attemptNumber to parse
   * @return extract attempt number or null
   */
  private static String parseAttemptNumberOrNull(final int attemptNumber) {
    if (attemptNumber > MAX_ATTEMPTS) {
      return null;
    } else {
      return Integer.toString(attemptNumber);
    }
  }

  public Optional<Job> findPreviousJob(final List<Job> jobs, final long targetJobId) {
    final Optional<Job> targetJob = jobs.stream()
        .filter(j -> j.getId() == targetJobId)
        .findFirst();

    // Target job not found or list is empty.
    if (targetJob.isEmpty()) {
      return Optional.empty();
    }

    return jobs.stream()
        .filter(job -> !Objects.equals(job.getId(), targetJobId)) // Not our target job.
        .filter(job -> job.getCreatedAtInSecond() < targetJob.get().getCreatedAtInSecond()) // Precedes target job.
        .reduce((a, b) -> a.getCreatedAtInSecond() > b.getCreatedAtInSecond() ? a : b); // Get latest.
  }

  public boolean didJobSucceed(final Job job) {
    return job.getStatus().equals(io.airbyte.persistence.job.models.JobStatus.SUCCEEDED);
  }

  public void failNonTerminalJobs(final UUID connectionId) throws IOException {
    final List<Job> jobs = jobPersistence.listJobsForConnectionWithStatuses(
        connectionId,
        Job.REPLICATION_TYPES,
        io.airbyte.persistence.job.models.JobStatus.NON_TERMINAL_STATUSES);

    for (final Job job : jobs) {
      final long jobId = job.getId();

      // fail all non-terminal attempts
      for (final Attempt attempt : job.getAttempts()) {
        if (Attempt.isAttemptInTerminalState(attempt)) {
          continue;
        }

        final int attemptNumber = attempt.getAttemptNumber();
        log.info("Failing non-terminal attempt {} for non-terminal job {}", attemptNumber, jobId);
        jobPersistence.failAttempt(jobId, attemptNumber);
        jobPersistence.writeAttemptFailureSummary(jobId, attemptNumber, failureSummaryForTemporalCleaningJobState(jobId, attemptNumber));
      }

      log.info("Failing non-terminal job {}", jobId);
      jobPersistence.failJob(jobId);

      final Job failedJob = jobPersistence.getJob(jobId);
      jobNotifier.failJob("Failing job in order to start from clean job state for new temporal workflow run.", failedJob);
      trackCompletion(failedJob, JobStatus.FAILED);
    }
  }

  private String parseIsJobRunningOnCustomConnectorForMetrics(final Job job) {
    if (job.getConfig() == null || job.getConfig().getSync() == null) {
      return "null";
    }
    if (job.getConfig().getSync().getIsSourceCustomConnector() == null
        || job.getConfig().getSync().getIsDestinationCustomConnector() == null) {
      return "null";
    }
    return String.valueOf(job.getConfig().getSync().getIsSourceCustomConnector() || job.getConfig().getSync().getIsDestinationCustomConnector());
  }

  private void emitAttemptEvent(final OssMetricsRegistry metric, final Job job, final int attemptNumber) throws IOException {
    emitAttemptEvent(metric, job, attemptNumber, Collections.emptyList());
  }

  private void emitAttemptEvent(final OssMetricsRegistry metric,
                                final Job job,
                                final int attemptNumber,
                                final List<MetricAttribute> additionalAttributes)
      throws IOException {
    final List<ReleaseStage> releaseStages = getJobToReleaseStages(job);
    final var releaseStagesOrdered = orderByReleaseStageAsc(releaseStages);
    final var connectionId = job.getScope() == null ? null : UUID.fromString(job.getScope());
    final var geography = configRepository.getGeographyForConnection(connectionId);

    final List<MetricAttribute> baseMetricAttributes = List.of(
        new MetricAttribute(MetricTags.GEOGRAPHY, geography == null ? null : geography.toString()),
        new MetricAttribute(MetricTags.ATTEMPT_NUMBER, parseAttemptNumberOrNull(attemptNumber)),
        new MetricAttribute(MetricTags.MIN_CONNECTOR_RELEASE_STATE, MetricTags.getReleaseStage(getOrNull(releaseStagesOrdered, 0))),
        new MetricAttribute(MetricTags.MAX_CONNECTOR_RELEASE_STATE, MetricTags.getReleaseStage(getOrNull(releaseStagesOrdered, 1))),
        new MetricAttribute(MetricTags.IS_CUSTOM_CONNECTOR_SYNC, parseIsJobRunningOnCustomConnectorForMetrics(job)));

    final MetricAttribute[] allMetricAttributes = Stream.concat(baseMetricAttributes.stream(), additionalAttributes.stream())
        .toList()
        .toArray(new MetricAttribute[baseMetricAttributes.size() + additionalAttributes.size()]);
    MetricClientFactory.getMetricClient().count(metric, 1, allMetricAttributes);
  }

  public void emitAttemptCreatedEvent(final Job job, final int attemptNumber) throws IOException {
    emitAttemptEvent(OssMetricsRegistry.ATTEMPTS_CREATED, job, attemptNumber);
  }

  /**
   * Get the value at an index or null if the index is out of bounds.
   *
   * @param list list to extract from
   * @param index index to extra
   * @param <T> type of the value in the list
   * @return extract value at index or null if index is out of bounds.
   */
  private static <T> T getOrNull(final List<T> list, final int index) {
    Preconditions.checkNotNull(list);
    if (list.size() > index) {
      return list.get(index);
    } else {
      return null;
    }
  }

  private void emitAttemptCompletedEvent(final Job job, final Attempt attempt) throws IOException {
    final Optional<String> failureOrigin = attempt.getFailureSummary().flatMap(summary -> summary.getFailures()
        .stream()
        .map(FailureReason::getFailureOrigin)
        .filter(Objects::nonNull)
        .map(FailureOrigin::name)
        .findFirst());

    final Optional<String> failureType = attempt.getFailureSummary().flatMap(summary -> summary.getFailures()
        .stream()
        .map(FailureReason::getFailureType)
        .filter(Objects::nonNull)
        .map(MetricTags::getFailureType)
        .findFirst());

    final List<MetricAttribute> additionalAttributes = List.of(
        new MetricAttribute(MetricTags.ATTEMPT_OUTCOME, attempt.getStatus().toString()),
        new MetricAttribute(MetricTags.FAILURE_ORIGIN, failureOrigin.orElse(null)),
        new MetricAttribute(MetricTags.FAILURE_TYPE, failureType.orElse(null)),
        new MetricAttribute(MetricTags.ATTEMPT_QUEUE, attempt.getProcessingTaskQueue()));

    emitAttemptEvent(OssMetricsRegistry.ATTEMPTS_COMPLETED, job, attempt.getAttemptNumber(), additionalAttributes);
  }

  @VisibleForTesting
  public List<ReleaseStage> getJobToReleaseStages(final Job job) throws IOException {
    if (job == null || job.getConfig() == null || job.getConfig().getConfigType() == null) {
      return Collections.emptyList();
    }

    final List<UUID> actorDefVersionIds = switch (job.getConfig().getConfigType()) {
      case SYNC -> List.of(job.getConfig().getSync().getDestinationDefinitionVersionId(), job.getConfig().getSync().getSourceDefinitionVersionId());
      case RESET_CONNECTION -> List.of(job.getConfig().getResetConnection().getDestinationDefinitionVersionId());
      default -> throw new IllegalArgumentException("Unexpected config type: " + job.getConfigType());
    };

    return configRepository.getActorDefinitionVersions(actorDefVersionIds).stream().map(ActorDefinitionVersion::getReleaseStage).toList();
  }

  public void emitJobToReleaseStagesMetric(final OssMetricsRegistry metric, final Job job) throws IOException {
    final var releaseStages = getJobToReleaseStages(job);
    if (releaseStages.isEmpty()) {
      return;
    }

    for (final ReleaseStage stage : releaseStages) {
      if (stage != null) {
        MetricClientFactory.getMetricClient().count(metric, 1,
            new MetricAttribute(MetricTags.RELEASE_STAGE, MetricTags.getReleaseStage(stage)));
      }
    }
  }

  public void trackCompletion(final Job job, final JobStatus status) throws IOException {
    emitAttemptCompletedEventIfAttemptPresent(job);
    jobTracker.trackSync(job, Enums.convertTo(status, JobState.class));
  }

  private void emitAttemptCompletedEventIfAttemptPresent(final Job job) throws IOException {
    if (job == null) {
      return;
    }

    final Optional<Attempt> lastAttempt = job.getLastAttempt();
    if (lastAttempt.isPresent()) {
      emitAttemptCompletedEvent(job, lastAttempt.get());
    }
  }

  public void trackCompletionForInternalFailure(final Long jobId,
                                                final UUID connectionId,
                                                final Integer attemptId,
                                                final JobStatus status,
                                                final Exception e) {
    jobTracker.trackSyncForInternalFailure(jobId, connectionId, attemptId, Enums.convertTo(status, JobState.class), e);
  }

  /**
   * Adds the failure origins to the APM trace.
   *
   * @param failureSummary The {@link AttemptFailureSummary} containing the failure reason(s).
   */
  public void traceFailures(final AttemptFailureSummary failureSummary) {
    if (failureSummary != null) {
      if (CollectionUtils.isNotEmpty(failureSummary.getFailures())) {
        ApmTraceUtils.addTagsToTrace(Map.of(
            FAILURE_TYPES_KEY,
            failureSummary.getFailures()
                .stream()
                .map(FailureReason::getFailureType)
                .map(MetricTags::getFailureType)
                .collect(Collectors.joining(",")),
            FAILURE_ORIGINS_KEY,
            failureSummary.getFailures()
                .stream()
                .map(FailureReason::getFailureOrigin)
                .map(FailureOrigin::name)
                .collect(Collectors.joining(","))));
      }
    } else {
      ApmTraceUtils.addTagsToTrace(Map.of(
          FAILURE_TYPES_KEY, MetricTags.getFailureType(null),
          FAILURE_ORIGINS_KEY, FailureOrigin.UNKNOWN.value()));
    }
  }

  /**
   * Records a metric for each failure reason.
   *
   * @param failureSummary The {@link AttemptFailureSummary} containing the failure reason(s).
   */
  public void trackFailures(final AttemptFailureSummary failureSummary) {
    if (failureSummary != null) {
      for (final FailureReason reason : failureSummary.getFailures()) {
        MetricClientFactory.getMetricClient().count(OssMetricsRegistry.ATTEMPT_FAILED_BY_FAILURE_ORIGIN, 1,
            new MetricAttribute(MetricTags.FAILURE_ORIGIN, MetricTags.getFailureOrigin(reason.getFailureOrigin())),
            new MetricAttribute(MetricTags.FAILURE_TYPE, MetricTags.getFailureType(reason.getFailureType())));
      }
    } else {
      MetricClientFactory.getMetricClient().count(OssMetricsRegistry.ATTEMPT_FAILED_BY_FAILURE_ORIGIN, 1,
          new MetricAttribute(MetricTags.FAILURE_ORIGIN, FailureOrigin.UNKNOWN.value()),
          new MetricAttribute(MetricTags.FAILURE_TYPE, MetricTags.getFailureType(null)));
    }
  }

  /**
   * Create attempt failure summary.
   *
   * @param jobId job id
   * @param attemptNumber attempt number
   * @return attempt failure summary
   */
  private AttemptFailureSummary failureSummaryForTemporalCleaningJobState(final Long jobId, final Integer attemptNumber) {
    final FailureReason failureReason = new FailureReason()
        .withFailureOrigin(FailureOrigin.AIRBYTE_PLATFORM)
        .withFailureType(FailureType.SYSTEM_ERROR)
        .withInternalMessage(
            "Setting attempt to FAILED because the temporal workflow for this connection was restarted, and existing job state was cleaned.")
        .withExternalMessage("An internal Airbyte error has occurred. This sync will need to be retried.")
        .withTimestamp(System.currentTimeMillis())
        .withMetadata(jobAndAttemptMetadata(jobId, attemptNumber));
    return new AttemptFailureSummary().withFailures(List.of(failureReason));
  }

  private static Metadata jobAndAttemptMetadata(final Long jobId, final Integer attemptNumber) {
    return new Metadata()
        .withAdditionalProperty(JOB_ID_METADATA_KEY, jobId)
        .withAdditionalProperty(ATTEMPT_NUMBER_METADATA_KEY, attemptNumber);
  }

  /**
   * Report a job as started.
   */
  public void reportJobStart(final Long jobId) throws IOException {
    final Job job = jobPersistence.getJob(jobId);
    jobTracker.trackSync(job, JobState.STARTED);
  }

}
