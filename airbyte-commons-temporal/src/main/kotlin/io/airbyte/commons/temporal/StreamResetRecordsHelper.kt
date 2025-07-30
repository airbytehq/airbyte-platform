/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.commons.temporal

import io.airbyte.commons.temporal.exception.RetryableException
import io.airbyte.config.JobConfig.ConfigType
import io.airbyte.config.persistence.StreamResetPersistence
import io.airbyte.persistence.job.JobPersistence
import io.github.oshai.kotlinlogging.KotlinLogging
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID

/**
 * Helper class that provides methods for dealing with stream reset records.
 */
@Singleton
class StreamResetRecordsHelper(
  private val jobPersistence: JobPersistence,
  private val streamResetPersistence: StreamResetPersistence,
) {
  /**
   * Deletes all stream reset records related to the provided job and connection.
   *
   * @param jobId The job ID.
   * @param connectionId the connection ID.
   */
  fun deleteStreamResetRecordsForJob(
    jobId: Long?,
    connectionId: UUID?,
  ) {
    if (jobId == null) {
      log.info("deleteStreamResetRecordsForJob was called with a null job id; returning.")
      return
    }

    try {
      val job = jobPersistence.getJob(jobId)
      val configType = job.config.configType
      if (ConfigType.RESET_CONNECTION != configType) {
        log.info(
          "deleteStreamResetRecordsForJob was called for job {} with config type {}. Returning, as config type is not {}.",
          jobId,
          configType,
          ConfigType.RESET_CONNECTION,
        )
        return
      }

      val resetStreams = job.config.resetConnection.resetSourceConfiguration.streamsToReset
      log.info("Deleting the following streams for reset job {} from the stream_reset table: {}", jobId, resetStreams)
      streamResetPersistence.deleteStreamResets(connectionId, resetStreams)
    } catch (e: IOException) {
      throw RetryableException(e)
    }
  }

  companion object {
    private val log = KotlinLogging.logger {}
  }
}
