/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.api.model.generated.JobRead
import io.airbyte.api.model.generated.JobStatus
import io.airbyte.api.model.generated.JobWithAttemptsRead
import io.airbyte.public_api.model.generated.JobResponse
import io.airbyte.public_api.model.generated.JobStatusEnum
import io.airbyte.public_api.model.generated.JobTypeEnum
import io.airbyte.server.apis.publicapi.mappers.JobsResponseMapper.ALLOWED_CONFIG_TYPES
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.util.UUID

/**
 * Mappers that help convert models from the config api to models from the public api.
 */
object JobResponseMapper {
  private val TERMINAL_JOB_STATUS = setOf(JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.SUCCEEDED)
  private val UTC = ZoneId.of("UTC")

  /**
   * Converts a JobInfoRead object from the config api to a JobResponse object.
   *
   * @param jobInfoRead Output of a job create/get from config api
   * @return JobResponse Response object which contains job id, status, and job type
   */
  fun from(jobInfoRead: JobInfoRead): JobResponse {
    val jobResponse: JobResponse = jobResponseFromJobReadMinusSyncedData(jobInfoRead.job)
    if (jobInfoRead.attempts.size > 0) {
      val lastAttempt = jobInfoRead.attempts[jobInfoRead.attempts.size - 1]
      jobResponse.setBytesSynced(lastAttempt.attempt.totalStats?.bytesCommitted)
      jobResponse.setRowsSynced(lastAttempt.attempt.totalStats?.recordsCommitted)
    }
    return jobResponse
  }

  /**
   * Converts a JobWithAttemptsRead object from the config api to a JobResponse object.
   *
   * @param jobWithAttemptsRead Output of a job get with attempts from config api
   * @return JobResponse Response object which contains job id, status, and job type
   */
  fun from(jobWithAttemptsRead: JobWithAttemptsRead): JobResponse {
    val jobResponse: JobResponse = jobResponseFromJobReadMinusSyncedData(jobWithAttemptsRead.job)
    if (jobWithAttemptsRead.attempts != null && jobWithAttemptsRead.attempts!!.size > 0) {
      val lastAttempt = jobWithAttemptsRead.attempts!![jobWithAttemptsRead.attempts!!.size - 1]

      jobResponse.setBytesSynced(lastAttempt.totalStats?.bytesCommitted)
      jobResponse.setRowsSynced(lastAttempt.totalStats?.recordsCommitted)
    }
    return jobResponse
  }

  /**
   * Converts a JobRead object from the config api to a JobResponse object.
   */
  private fun jobResponseFromJobReadMinusSyncedData(jobRead: JobRead?): JobResponse {
    val jobResponse = JobResponse()
    jobResponse.setJobId(jobRead!!.id)
    jobResponse.setStatus(JobStatusEnum.fromValue(jobRead.status.toString()))
    jobResponse.setConnectionId(UUID.fromString(jobRead.configId))
    when (jobRead.configType) {
      JobConfigType.SYNC -> jobResponse.setJobType(JobTypeEnum.SYNC)
      JobConfigType.RESET_CONNECTION -> jobResponse.setJobType(JobTypeEnum.RESET)
      else -> {
        assert(ALLOWED_CONFIG_TYPES.contains(jobRead.configType))
      }
    }
    // set to string for now since the jax-rs response entity turns offsetdatetime into epoch seconds
    jobResponse.setStartTime(OffsetDateTime.ofInstant(Instant.ofEpochSecond(jobRead.createdAt), UTC).toString())
    if (TERMINAL_JOB_STATUS.contains(jobRead.status)) {
      jobResponse.setLastUpdatedAt(OffsetDateTime.ofInstant(Instant.ofEpochSecond(jobRead.updatedAt), UTC).toString())
    }

    // duration is ISO_8601 formatted https://en.wikipedia.org/wiki/ISO_8601#Durations
    if (jobRead.status != JobStatus.PENDING) {
      jobResponse.setDuration(Duration.ofSeconds(jobRead.updatedAt - jobRead.createdAt).toString())
    }
    return jobResponse
  }
}
