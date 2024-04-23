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
    return fromJobRead(jobInfoRead.job)
  }

  /**
   * Converts a JobWithAttemptsRead object from the config api to a JobResponse object.
   *
   * @param jobWithAttemptsRead Output of a job get with attempts from config api
   * @return JobResponse Response object which contains job id, status, and job type
   */
  fun from(jobWithAttemptsRead: JobWithAttemptsRead): JobResponse {
    return fromJobRead(jobWithAttemptsRead.job)
  }

  /**
   * Converts a JobRead object from the config api to a JobResponse object.
   */
  private fun fromJobRead(jobRead: JobRead?): JobResponse {
    val jobResponse = JobResponse()
    jobResponse.jobId = jobRead!!.id
    jobResponse.status = JobStatusEnum.fromValue(jobRead.status.toString())
    jobResponse.connectionId = UUID.fromString(jobRead.configId)
    when (jobRead.configType) {
      JobConfigType.SYNC -> jobResponse.jobType = JobTypeEnum.SYNC
      JobConfigType.RESET_CONNECTION -> jobResponse.jobType = JobTypeEnum.RESET
      else -> {
        assert(ALLOWED_CONFIG_TYPES.contains(jobRead.configType))
      }
    }
    // set to string for now since the jax-rs response entity turns offsetdatetime into epoch seconds
    jobResponse.startTime = OffsetDateTime.ofInstant(Instant.ofEpochSecond(jobRead.createdAt), UTC).toString()
    if (TERMINAL_JOB_STATUS.contains(jobRead.status)) {
      jobResponse.lastUpdatedAt = OffsetDateTime.ofInstant(Instant.ofEpochSecond(jobRead.updatedAt), UTC).toString()
    }

    // duration is ISO_8601 formatted https://en.wikipedia.org/wiki/ISO_8601#Durations
    if (jobRead.status != JobStatus.PENDING) {
      jobResponse.duration = Duration.ofSeconds(jobRead.updatedAt - jobRead.createdAt).toString()
    }

    jobResponse.bytesSynced = jobRead.aggregatedStats?.bytesCommitted
    jobResponse.rowsSynced = jobRead.aggregatedStats?.recordsCommitted
    return jobResponse
  }
}
