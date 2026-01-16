/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.apis.publicapi.mappers

import io.airbyte.api.model.generated.JobConfigType
import io.airbyte.api.model.generated.JobInfoRead
import io.airbyte.api.model.generated.JobRead
import io.airbyte.api.model.generated.JobStatus
import io.airbyte.api.model.generated.JobWithAttemptsRead
import io.airbyte.publicApi.server.generated.models.JobResponse
import io.airbyte.publicApi.server.generated.models.JobStatusEnum
import io.airbyte.publicApi.server.generated.models.JobTypeEnum
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId

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
  fun from(jobInfoRead: JobInfoRead): JobResponse = fromJobRead(jobInfoRead.job)

  /**
   * Converts a JobWithAttemptsRead object from the config api to a JobResponse object.
   *
   * @param jobWithAttemptsRead Output of a job get with attempts from config api
   * @return JobResponse Response object which contains job id, status, and job type
   */
  fun from(jobWithAttemptsRead: JobWithAttemptsRead): JobResponse = fromJobRead(jobWithAttemptsRead.job)

  /**
   * Converts a JobRead object from the config api to a JobResponse object.
   */
  private fun fromJobRead(jobRead: JobRead): JobResponse =
    JobResponse(
      jobId = jobRead.id,
      status = JobStatusEnum.valueOf(jobRead.status.toString().uppercase()),
      connectionId = jobRead.configId,
      jobType =
        when (jobRead.configType) {
          JobConfigType.SYNC -> JobTypeEnum.SYNC
          JobConfigType.RESET_CONNECTION -> JobTypeEnum.RESET
          JobConfigType.CLEAR -> JobTypeEnum.CLEAR
          JobConfigType.REFRESH -> JobTypeEnum.REFRESH
          else -> {
            throw IllegalArgumentException("Unknown job type ${jobRead.configType}")
          }
        },
      // set to string for now since the jax-rs response entity turns offsetdatetime into epoch seconds
      startTime = OffsetDateTime.ofInstant(Instant.ofEpochSecond(jobRead.createdAt), UTC).toString(),
      lastUpdatedAt =
        if (TERMINAL_JOB_STATUS.contains(jobRead.status)) {
          OffsetDateTime.ofInstant(Instant.ofEpochSecond(jobRead.updatedAt), UTC).toString()
        } else {
          null
        },
      // duration is ISO_8601 formatted https://en.wikipedia.org/wiki/ISO_8601#Durations
      duration =
        if (jobRead.status != JobStatus.PENDING) {
          Duration.ofSeconds(jobRead.updatedAt - jobRead.createdAt).toString()
        } else {
          null
        },
      bytesSynced = jobRead.aggregatedStats?.bytesCommitted,
      rowsSynced = jobRead.aggregatedStats?.recordsCommitted,
    )
}
