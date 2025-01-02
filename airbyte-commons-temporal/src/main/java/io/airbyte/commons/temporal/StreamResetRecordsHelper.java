/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal;

import io.airbyte.commons.temporal.exception.RetryableException;
import io.airbyte.config.Job;
import io.airbyte.config.JobConfig.ConfigType;
import io.airbyte.config.StreamDescriptor;
import io.airbyte.config.persistence.StreamResetPersistence;
import io.airbyte.persistence.job.JobPersistence;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that provides methods for dealing with stream reset records.
 */
@Singleton
public class StreamResetRecordsHelper {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final JobPersistence jobPersistence;
  private final StreamResetPersistence streamResetPersistence;

  public StreamResetRecordsHelper(final JobPersistence jobPersistence, final StreamResetPersistence streamResetPersistence) {
    this.jobPersistence = jobPersistence;
    this.streamResetPersistence = streamResetPersistence;
  }

  /**
   * Deletes all stream reset records related to the provided job and connection.
   *
   * @param jobId The job ID.
   * @param connectionId the connection ID.
   */
  public void deleteStreamResetRecordsForJob(final Long jobId, final UUID connectionId) {
    if (jobId == null) {
      log.info("deleteStreamResetRecordsForJob was called with a null job id; returning.");
      return;
    }

    try {
      final Job job = jobPersistence.getJob(jobId);
      final ConfigType configType = job.getConfig().getConfigType();
      if (!ConfigType.RESET_CONNECTION.equals(configType)) {
        log.info("deleteStreamResetRecordsForJob was called for job {} with config type {}. Returning, as config type is not {}.",
            jobId,
            configType,
            ConfigType.RESET_CONNECTION);
        return;
      }

      final List<StreamDescriptor> resetStreams = job.getConfig().getResetConnection().getResetSourceConfiguration().getStreamsToReset();
      log.info("Deleting the following streams for reset job {} from the stream_reset table: {}", jobId, resetStreams);
      streamResetPersistence.deleteStreamResets(connectionId, resetStreams);
    } catch (final IOException e) {
      throw new RetryableException(e);
    }
  }

}
