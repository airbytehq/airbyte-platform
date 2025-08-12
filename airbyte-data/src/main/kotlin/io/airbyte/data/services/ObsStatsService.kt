/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.data.services

import io.airbyte.data.ConfigNotFoundException
import io.airbyte.data.repositories.ObsJobsStatsRepository
import io.airbyte.data.repositories.ObsStreamStatsRepository
import io.airbyte.data.repositories.entities.ObsJobsStats
import io.airbyte.data.repositories.entities.ObsStreamStats
import io.micronaut.data.exceptions.DataAccessException
import jakarta.inject.Singleton

@Singleton
class ObsStatsService(
  private val obsJobsStatsRepository: ObsJobsStatsRepository,
  private val obsStreamStatsRepository: ObsStreamStatsRepository,
) {
  fun getJobStats(jobId: Long): ObsJobsStats =
    obsJobsStatsRepository.findById(jobId).orElseThrow { ConfigNotFoundException("jobId", jobId.toString()) }

  fun saveJobsStats(jobStats: ObsJobsStats): ObsJobsStats =
    try {
      obsJobsStatsRepository.save(jobStats)
    } catch (e: DataAccessException) {
      // Because micronaut data doesn't have upsert, retry using updated on DataAccessException
      obsJobsStatsRepository.update(jobStats)
    }

  fun getJobStreamStats(jobId: Long): List<ObsStreamStats> = obsStreamStatsRepository.findByJobId(jobId)

  fun saveStreamStats(stats: List<ObsStreamStats>): List<ObsStreamStats> =
    try {
      obsStreamStatsRepository.saveAll(stats)
    } catch (e: DataAccessException) {
      obsStreamStatsRepository.updateAll(stats)
    }
}
