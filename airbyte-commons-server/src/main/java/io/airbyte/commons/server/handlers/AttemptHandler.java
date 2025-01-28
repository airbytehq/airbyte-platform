/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import static io.airbyte.commons.logging.LogMdcHelperKt.DEFAULT_LOG_FILENAME;

import com.google.common.annotations.VisibleForTesting;
import io.airbyte.api.model.generated.AttemptInfoRead;
import io.airbyte.api.model.generated.AttemptStats;
import io.airbyte.api.model.generated.CreateNewAttemptNumberResponse;
import io.airbyte.api.model.generated.InternalOperationResult;
import io.airbyte.api.model.generated.SaveAttemptSyncConfigRequestBody;
import io.airbyte.api.model.generated.SaveStatsRequestBody;
import io.airbyte.api.model.generated.SaveStreamAttemptMetadataRequestBody;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.converters.JobConverter;
import io.airbyte.commons.server.errors.BadRequestException;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.commons.server.errors.UnprocessableContentException;
import io.airbyte.commons.server.handlers.helpers.JobCreationAndStatusUpdateHelper;
import io.airbyte.commons.temporal.TemporalUtils;
import io.airbyte.config.ActorDefinitionVersion;
import io.airbyte.config.AttemptFailureSummary;
import io.airbyte.config.ConfiguredAirbyteCatalog;
import io.airbyte.config.DestinationConnection;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig;
import io.airbyte.config.JobConfigProxy;
import io.airbyte.config.JobOutput;
import io.airbyte.config.RefreshStream;
import io.airbyte.config.StandardDestinationDefinition;
import io.airbyte.config.StandardSync;
import io.airbyte.config.StandardSyncOutput;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncMode;
import io.airbyte.config.SyncStats;
import io.airbyte.config.persistence.ActorDefinitionVersionHelper;
import io.airbyte.config.persistence.StatePersistence;
import io.airbyte.config.persistence.helper.GenerationBumper;
import io.airbyte.data.exceptions.ConfigNotFoundException;
import io.airbyte.data.services.ConnectionService;
import io.airbyte.data.services.DestinationService;
import io.airbyte.data.services.StreamAttemptMetadata;
import io.airbyte.data.services.StreamAttemptMetadataService;
import io.airbyte.featureflag.Connection;
import io.airbyte.featureflag.EnableResumableFullRefresh;
import io.airbyte.featureflag.FeatureFlagClient;
import io.airbyte.metrics.lib.OssMetricsRegistry;
import io.airbyte.persistence.job.JobPersistence;
import io.airbyte.validation.json.JsonValidationException;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AttemptHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@Singleton
public class AttemptHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(AttemptHandler.class);
  private final JobPersistence jobPersistence;
  private final StatePersistence statePersistence;

  private final JobConverter jobConverter;
  private final JobCreationAndStatusUpdateHelper jobCreationAndStatusUpdateHelper;
  private final Path workspaceRoot;
  private final FeatureFlagClient featureFlagClient;
  private final GenerationBumper generationBumper;
  private final ConnectionService connectionService;
  private final DestinationService destinationService;
  private final ActorDefinitionVersionHelper actorDefinitionVersionHelper;
  private final StreamAttemptMetadataService streamAttemptMetadataService;
  private final ApiPojoConverters apiPojoConverters;

  public AttemptHandler(final JobPersistence jobPersistence,
                        final StatePersistence statePersistence,
                        final JobConverter jobConverter,
                        final FeatureFlagClient featureFlagClient,
                        final JobCreationAndStatusUpdateHelper jobCreationAndStatusUpdateHelper,
                        @Named("workspaceRoot") final Path workspaceRoot,
                        final GenerationBumper generationBumper,
                        final ConnectionService connectionService,
                        final DestinationService destinationService,
                        final ActorDefinitionVersionHelper actorDefinitionVersionHelper,
                        final StreamAttemptMetadataService streamAttemptMetadataService,
                        final ApiPojoConverters apiPojoConverters) {
    this.jobPersistence = jobPersistence;
    this.statePersistence = statePersistence;
    this.jobConverter = jobConverter;
    this.jobCreationAndStatusUpdateHelper = jobCreationAndStatusUpdateHelper;
    this.featureFlagClient = featureFlagClient;
    this.workspaceRoot = workspaceRoot;
    this.generationBumper = generationBumper;
    this.connectionService = connectionService;
    this.destinationService = destinationService;
    this.actorDefinitionVersionHelper = actorDefinitionVersionHelper;
    this.streamAttemptMetadataService = streamAttemptMetadataService;
    this.apiPojoConverters = apiPojoConverters;
  }

  public CreateNewAttemptNumberResponse createNewAttemptNumber(final long jobId)
      throws IOException, JsonValidationException, ConfigNotFoundException, io.airbyte.config.persistence.ConfigNotFoundException {
    final Job job;
    try {
      job = jobPersistence.getJob(jobId);
    } catch (final RuntimeException e) {
      throw new UnprocessableContentException(String.format("Could not find jobId: %s", jobId), e);
    }

    final Path jobRoot = TemporalUtils.getJobRoot(workspaceRoot, String.valueOf(jobId), job.getAttemptsCount());
    final Path logFilePath = jobRoot.resolve(DEFAULT_LOG_FILENAME);
    final int persistedAttemptNumber = jobPersistence.createAttempt(jobId, logFilePath);

    final UUID connectionId = UUID.fromString(job.getScope());
    final StandardSync standardSync = connectionService.getStandardSync(connectionId);
    final StandardDestinationDefinition standardDestinationDefinition =
        destinationService.getDestinationDefinitionFromDestination(standardSync.getDestinationId());
    final DestinationConnection destinationConnection = destinationService.getDestinationConnection(standardSync.getDestinationId());
    final ActorDefinitionVersion destinationVersion =
        actorDefinitionVersionHelper.getDestinationVersion(standardDestinationDefinition, destinationConnection.getWorkspaceId(),
            destinationConnection.getDestinationId());
    final boolean supportRefreshes = destinationVersion.getSupportsRefreshes();

    // Action done in the first attempt
    if (persistedAttemptNumber == 0) {
      updateGenerationAndStateForFirstAttempt(job, connectionId, supportRefreshes);
    } else {
      updateGenerationAndStateForSubsequentAttempts(job, supportRefreshes);
    }

    jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.ATTEMPT_CREATED_BY_RELEASE_STAGE, job);
    jobCreationAndStatusUpdateHelper.emitAttemptCreatedEvent(job, persistedAttemptNumber);

    return new CreateNewAttemptNumberResponse().attemptNumber(persistedAttemptNumber);
  }

  @VisibleForTesting
  void updateGenerationAndStateForSubsequentAttempts(final Job job, final boolean supportRefreshes) throws IOException {
    // We cannot easily do this in a transaction as the attempt and state tables are in separate logical
    // databases.
    final boolean enableResumableFullRefresh = featureFlagClient.boolVariation(EnableResumableFullRefresh.INSTANCE, new Connection(job.getScope()));
    final boolean evaluateResumableFlag = enableResumableFullRefresh && supportRefreshes;
    final var stateToClear = getFullRefreshStreamsToClear(new JobConfigProxy(job.getConfig()).getConfiguredCatalog(),
        job.getId(),
        evaluateResumableFlag);

    generationBumper.updateGenerationForStreams(UUID.fromString(job.getScope()), job.getId(), List.of(), stateToClear);
    if (!stateToClear.isEmpty()) {
      statePersistence.bulkDelete(UUID.fromString(job.getScope()), stateToClear);
    }
  }

  @VisibleForTesting
  void updateGenerationAndStateForFirstAttempt(final Job job, final UUID connectionId, final boolean supportRefreshes) throws IOException {
    if (job.getConfigType() == JobConfig.ConfigType.REFRESH) {
      if (!supportRefreshes) {
        throw new IllegalStateException("Trying to create a refresh attempt for a destination which doesn't support refreshes");
      }
      final Set<StreamDescriptor> streamToReset = getFullRefresh(job.getConfig().getRefresh().getConfiguredAirbyteCatalog(), true);
      streamToReset.addAll(job.getConfig().getRefresh().getStreamsToRefresh().stream().map(RefreshStream::getStreamDescriptor)
          .collect(Collectors.toSet()));
      generationBumper.updateGenerationForStreams(connectionId, job.getId(), List.of(), streamToReset);

      statePersistence.bulkDelete(UUID.fromString(job.getScope()), streamToReset);
    } else if (job.getConfigType() == JobConfig.ConfigType.SYNC) {
      final Set<StreamDescriptor> fullRefreshes = getFullRefresh(job.getConfig().getSync().getConfiguredAirbyteCatalog(), true);
      generationBumper.updateGenerationForStreams(connectionId, job.getId(), List.of(), fullRefreshes);

      statePersistence.bulkDelete(UUID.fromString(job.getScope()), fullRefreshes);
    } else if (job.getConfigType() == JobConfig.ConfigType.CLEAR || job.getConfigType() == JobConfig.ConfigType.RESET_CONNECTION) {
      final Set<StreamDescriptor> resetStreams = Set.copyOf(job.getConfig().getResetConnection().getResetSourceConfiguration().getStreamsToReset());

      generationBumper.updateGenerationForStreams(connectionId, job.getId(), List.of(), resetStreams);
      statePersistence.bulkDelete(UUID.fromString(job.getScope()), resetStreams);
    }
  }

  @VisibleForTesting
  Set<StreamDescriptor> getFullRefresh(final ConfiguredAirbyteCatalog catalog, final boolean supportResumableFullRefresh) {
    if (!supportResumableFullRefresh) {
      return Set.of();
    }

    return catalog.getStreams().stream()
        .filter(stream -> stream.getSyncMode() == SyncMode.FULL_REFRESH)
        .map(stream -> new StreamDescriptor()
            .withName(stream.getStream().getName())
            .withNamespace(stream.getStream().getNamespace()))
        .collect(Collectors.toSet());
  }

  @VisibleForTesting
  Set<StreamDescriptor> getFullRefreshStreamsToClear(final ConfiguredAirbyteCatalog catalog, final long id, final boolean excludeResumableStreams) {
    if (catalog == null) {
      throw new BadRequestException("Missing configured catalog for job: " + id);
    }
    final var configuredStreams = catalog.getStreams();
    if (configuredStreams == null) {
      throw new BadRequestException("Missing configured catalog stream for job: " + id);
    }

    return configuredStreams.stream()
        .filter(s -> {
          if (s.getSyncMode().equals(SyncMode.FULL_REFRESH)) {
            if (excludeResumableStreams) {
              return s.getStream().isResumable() == null || !s.getStream().isResumable();
            } else {
              return true;
            }
          } else {
            return false;
          }
        })
        .map(s -> new StreamDescriptor().withName(s.getStream().getName()).withNamespace(s.getStream().getNamespace()))
        .collect(Collectors.toSet());
  }

  public AttemptInfoRead getAttemptForJob(final long jobId, final int attemptNo) throws IOException {
    final Optional<AttemptInfoRead> read = jobPersistence.getAttemptForJob(jobId, attemptNo)
        .map(jobConverter::getAttemptInfoRead);

    if (read.isEmpty()) {
      throw new IdNotFoundKnownException(
          String.format("Could not find attempt for job_id: %d and attempt no: %d", jobId, attemptNo),
          String.format("%d_%d", jobId, attemptNo));
    }

    return read.get();
  }

  public AttemptStats getAttemptCombinedStats(final long jobId, final int attemptNo) throws IOException {
    final SyncStats stats = jobPersistence.getAttemptCombinedStats(jobId, attemptNo);

    if (stats == null) {
      throw new IdNotFoundKnownException(
          String.format("Could not find attempt stats for job_id: %d and attempt no: %d", jobId, attemptNo),
          String.format("%d_%d", jobId, attemptNo));
    }

    return new AttemptStats()
        .recordsEmitted(stats.getRecordsEmitted())
        .bytesEmitted(stats.getBytesEmitted())
        .bytesCommitted(stats.getBytesCommitted())
        .recordsCommitted(stats.getRecordsCommitted())
        .estimatedRecords(stats.getEstimatedRecords())
        .estimatedBytes(stats.getEstimatedBytes());
  }

  public InternalOperationResult saveStats(final SaveStatsRequestBody requestBody) {
    try {
      final var stats = requestBody.getStats();
      final var streamStats = requestBody.getStreamStats().stream()
          .map(s -> new StreamSyncStats()
              .withStreamName(s.getStreamName())
              .withStreamNamespace(s.getStreamNamespace())
              .withStats(new SyncStats()
                  .withBytesEmitted(s.getStats().getBytesEmitted())
                  .withRecordsEmitted(s.getStats().getRecordsEmitted())
                  .withBytesCommitted(s.getStats().getBytesCommitted())
                  .withRecordsCommitted(s.getStats().getRecordsCommitted())
                  .withEstimatedBytes(s.getStats().getEstimatedBytes())
                  .withEstimatedRecords(s.getStats().getEstimatedRecords())))
          .collect(Collectors.toList());

      jobPersistence.writeStats(requestBody.getJobId(), requestBody.getAttemptNumber(),
          stats.getEstimatedRecords(), stats.getEstimatedBytes(),
          stats.getRecordsEmitted(), stats.getBytesEmitted(),
          stats.getRecordsCommitted(), stats.getBytesCommitted(),
          requestBody.getConnectionId(),
          streamStats);

    } catch (final IOException ioe) {
      LOGGER.error("IOException when setting temporal workflow in attempt;", ioe);
      return new InternalOperationResult().succeeded(false);
    }

    return new InternalOperationResult().succeeded(true);
  }

  public InternalOperationResult saveStreamMetadata(final SaveStreamAttemptMetadataRequestBody requestBody) {
    try {
      streamAttemptMetadataService.upsertStreamAttemptMetadata(
          requestBody.getJobId(),
          requestBody.getAttemptNumber(),
          requestBody.getStreamMetadata().stream().map(
              (s) -> new StreamAttemptMetadata(
                  s.getStreamName(),
                  s.getStreamNamespace(),
                  s.getWasBackfilled(),
                  s.getWasResumed()))
              .toList());
      return new InternalOperationResult().succeeded(true);
    } catch (final Exception e) {
      LOGGER.error("failed to save steam metadata for job:{} attempt:{}", requestBody.getJobId(), requestBody.getAttemptNumber(), e);
      return new InternalOperationResult().succeeded(false);
    }
  }

  public InternalOperationResult saveSyncConfig(final SaveAttemptSyncConfigRequestBody requestBody) {
    try {
      jobPersistence.writeAttemptSyncConfig(
          requestBody.getJobId(),
          requestBody.getAttemptNumber(),
          apiPojoConverters.attemptSyncConfigToInternal(requestBody.getSyncConfig()));
    } catch (final IOException ioe) {
      LOGGER.error("IOException when saving AttemptSyncConfig for attempt;", ioe);
      return new InternalOperationResult().succeeded(false);
    }
    return new InternalOperationResult().succeeded(true);
  }

  @SuppressWarnings("PMD")
  public void failAttempt(final int attemptNumber, final long jobId, final Object rawFailureSummary, final Object rawSyncOutput)
      throws IOException {
    AttemptFailureSummary failureSummary = null;
    if (rawFailureSummary != null) {
      try {
        failureSummary = Jsons.convertValue(rawFailureSummary, AttemptFailureSummary.class);
      } catch (final Exception e) {
        throw new BadRequestException("Unable to parse failureSummary.", e);
      }
    }
    StandardSyncOutput output = null;
    if (rawSyncOutput != null) {
      try {
        output = Jsons.convertValue(rawSyncOutput, StandardSyncOutput.class);
      } catch (final Exception e) {
        throw new BadRequestException("Unable to parse standardSyncOutput.", e);
      }
    }

    jobCreationAndStatusUpdateHelper.traceFailures(failureSummary);

    jobPersistence.failAttempt(jobId, attemptNumber);
    jobPersistence.writeAttemptFailureSummary(jobId, attemptNumber, failureSummary);

    if (output != null) {
      final JobOutput jobOutput = new JobOutput().withSync(output);
      jobPersistence.writeOutput(jobId, attemptNumber, jobOutput);
    }

    final Job job = jobPersistence.getJob(jobId);
    jobCreationAndStatusUpdateHelper.emitJobToReleaseStagesMetric(OssMetricsRegistry.ATTEMPT_FAILED_BY_RELEASE_STAGE, job);
    jobCreationAndStatusUpdateHelper.emitAttemptCompletedEventIfAttemptPresent(job);
    jobCreationAndStatusUpdateHelper.trackFailures(failureSummary);
  }

}
