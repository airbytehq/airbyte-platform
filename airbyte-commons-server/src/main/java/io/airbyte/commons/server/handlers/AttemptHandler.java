/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.server.handlers;

import io.airbyte.api.model.generated.AttemptStats;
import io.airbyte.api.model.generated.InternalOperationResult;
import io.airbyte.api.model.generated.SaveAttemptSyncConfigRequestBody;
import io.airbyte.api.model.generated.SaveStatsRequestBody;
import io.airbyte.api.model.generated.SetWorkflowInAttemptRequestBody;
import io.airbyte.commons.server.converters.ApiPojoConverters;
import io.airbyte.commons.server.errors.IdNotFoundKnownException;
import io.airbyte.config.StreamSyncStats;
import io.airbyte.config.SyncStats;
import io.airbyte.persistence.job.JobPersistence;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AttemptHandler. Javadocs suppressed because api docs should be used as source of truth.
 */
@SuppressWarnings("MissingJavadocMethod")
@Singleton
public class AttemptHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(AttemptHandler.class);

  private final JobPersistence jobPersistence;

  public AttemptHandler(final JobPersistence jobPersistence) {
    this.jobPersistence = jobPersistence;
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

  public InternalOperationResult setWorkflowInAttempt(final SetWorkflowInAttemptRequestBody requestBody) {
    try {
      jobPersistence.setAttemptTemporalWorkflowInfo(requestBody.getJobId(),
          requestBody.getAttemptNumber(), requestBody.getWorkflowId(), requestBody.getProcessingTaskQueue());
    } catch (final IOException ioe) {
      LOGGER.error("IOException when setting temporal workflow in attempt;", ioe);
      return new InternalOperationResult().succeeded(false);
    }
    return new InternalOperationResult().succeeded(true);
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
          streamStats);

    } catch (final IOException ioe) {
      LOGGER.error("IOException when setting temporal workflow in attempt;", ioe);
      return new InternalOperationResult().succeeded(false);
    }

    return new InternalOperationResult().succeeded(true);
  }

  public InternalOperationResult saveSyncConfig(final SaveAttemptSyncConfigRequestBody requestBody) {
    try {
      jobPersistence.writeAttemptSyncConfig(
          requestBody.getJobId(),
          requestBody.getAttemptNumber(),
          ApiPojoConverters.attemptSyncConfigToInternal(requestBody.getSyncConfig()));
    } catch (final IOException ioe) {
      LOGGER.error("IOException when saving AttemptSyncConfig for attempt;", ioe);
      return new InternalOperationResult().succeeded(false);
    }
    return new InternalOperationResult().succeeded(true);
  }

}
