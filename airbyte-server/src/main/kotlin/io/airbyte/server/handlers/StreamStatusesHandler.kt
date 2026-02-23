/*
 * Copyright (c) 2020-2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.server.handlers

import io.airbyte.api.model.generated.ConnectionIdRequestBody
import io.airbyte.api.model.generated.ConnectionSyncResultRead
import io.airbyte.api.model.generated.ConnectionUptimeHistoryRequestBody
import io.airbyte.api.model.generated.JobStatus
import io.airbyte.api.model.generated.JobSyncResultRead
import io.airbyte.api.model.generated.StreamStatusCreateRequestBody
import io.airbyte.api.model.generated.StreamStatusIncompleteRunCause
import io.airbyte.api.model.generated.StreamStatusListRequestBody
import io.airbyte.api.model.generated.StreamStatusRead
import io.airbyte.api.model.generated.StreamStatusReadList
import io.airbyte.api.model.generated.StreamStatusRunState
import io.airbyte.api.model.generated.StreamStatusUpdateRequestBody
import io.airbyte.commons.server.handlers.JobHistoryHandler
import io.airbyte.commons.server.handlers.helpers.StatsAggregationHelper
import io.airbyte.config.Job
import io.airbyte.featureflag.Connection
import io.airbyte.featureflag.FeatureFlagClient
import io.airbyte.featureflag.UseOptimizedStreamStatusQuery
import io.airbyte.featureflag.UseReadReplicaForStreamStatus
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.server.handlers.apidomainmapping.StreamStatusesMapper
import io.airbyte.server.repositories.StreamStatusesReadRepository
import io.airbyte.server.repositories.StreamStatusesRepository
import io.airbyte.server.repositories.domain.StreamStatus
import jakarta.inject.Singleton
import java.io.IOException
import java.util.UUID

/**
 * Interface layer between the API and Persistence layers.
 */
@Singleton
open class StreamStatusesHandler(
  private val repo: StreamStatusesRepository,
  private val replicaRepo: StreamStatusesReadRepository,
  val mapper: StreamStatusesMapper,
  private val jobHistoryHandler: JobHistoryHandler,
  private val jobPersistence: JobPersistence,
  private val featureFlagClient: FeatureFlagClient,
) {
  fun createStreamStatus(req: StreamStatusCreateRequestBody): StreamStatusRead {
    val model = mapper.map(req)

    val saved = repo.save(model)

    return mapper.map(saved)
  }

  fun updateStreamStatus(req: StreamStatusUpdateRequestBody): StreamStatusRead {
    val model = mapper.map(req)

    val saved = repo.update(model)

    return mapper.map(saved)
  }

  fun listStreamStatus(req: StreamStatusListRequestBody): StreamStatusReadList {
    val filters = mapper.map(req)
    val useReplica = featureFlagClient.boolVariation(UseReadReplicaForStreamStatus, Connection(req.connectionId))

    val page =
      if (useReplica) {
        replicaRepo.findAllFiltered(filters)
      } else {
        repo.findAllFiltered(filters)
      }

    val apiList =
      page.content
        .map { domain -> mapper.map(domain!!) }

    return StreamStatusReadList().streamStatuses(apiList)
  }

  fun listStreamStatusPerRunState(req: ConnectionIdRequestBody): StreamStatusReadList {
    val useOptimized = featureFlagClient.boolVariation(UseOptimizedStreamStatusQuery, Connection(req.connectionId))
    val useReplica = featureFlagClient.boolVariation(UseReadReplicaForStreamStatus, Connection(req.connectionId))

    val streamStatuses: List<StreamStatus> =
      if (useOptimized) {
        // Optimized: same query but with recency filter (last 100 jobs)
        // Reduces query time from 2+ min to ~18 sec for high-volume connections
        if (useReplica) {
          replicaRepo.findAllPerRunStateByConnectionIdWithRecentJobsFilter(req.connectionId, RECENT_JOBS_LIMIT)
        } else {
          repo.findAllPerRunStateByConnectionIdWithRecentJobsFilter(req.connectionId, RECENT_JOBS_LIMIT)
        }
      } else {
        if (useReplica) {
          replicaRepo.findAllPerRunStateByConnectionId(req.connectionId)
        } else {
          repo.findAllPerRunStateByConnectionId(req.connectionId)
        }
      }

    val apiList = streamStatuses.map { domain -> mapper.map(domain) }
    return StreamStatusReadList().streamStatuses(apiList)
  }

  /**
   * Determines whether to use the read replica based on the feature flag.
   */
  private fun shouldUseReplica(connectionId: UUID): Boolean = featureFlagClient.boolVariation(UseReadReplicaForStreamStatus, Connection(connectionId))

  companion object {
    const val RECENT_JOBS_LIMIT = 100
  }

  fun mapStreamStatusToSyncReadResult(streamStatus: StreamStatusRead): ConnectionSyncResultRead {
    val jobStatus =
      if (streamStatus.runState == StreamStatusRunState.COMPLETE) {
        JobStatus.SUCCEEDED
      } else {
        if (streamStatus.incompleteRunCause == StreamStatusIncompleteRunCause.CANCELED) {
          JobStatus.CANCELLED
        } else {
          JobStatus.FAILED
        }
      }

    val result = ConnectionSyncResultRead()
    result.status = jobStatus
    result.streamName = streamStatus.streamName
    result.streamNamespace = streamStatus.streamNamespace
    return result
  }

  /**
   * Get the uptime history for a specific connection over the last X jobs.
   *
   * @param req the request body
   * @return list of JobSyncResultReads.
   */
  fun getConnectionUptimeHistory(req: ConnectionUptimeHistoryRequestBody): List<JobSyncResultRead> {
    val useReplica = shouldUseReplica(req.connectionId)

    val streamStatuses: List<StreamStatusRead> =
      if (useReplica) {
        replicaRepo
          .findLastAttemptsOfLastXJobsForConnection(req.connectionId, req.numberOfJobs)
          ?.map { domain -> mapper.map(domain!!) }
          ?: emptyList()
      } else {
        repo
          .findLastAttemptsOfLastXJobsForConnection(req.connectionId, req.numberOfJobs)
          ?.map { domain -> mapper.map(domain!!) }
          ?: emptyList()
      }

    val jobIdToStreamStatuses = streamStatuses.groupBy { it.jobId }

    val result: MutableList<JobSyncResultRead> = ArrayList()

    val jobs: List<Job>
    try {
      jobs = jobPersistence.listJobsLight(HashSet(jobIdToStreamStatuses.keys))
    } catch (e: IOException) {
      throw RuntimeException(e)
    }

    val jobIdToJobRead = StatsAggregationHelper.getJobIdToJobWithAttemptsReadMap(jobs, jobPersistence)

    jobIdToStreamStatuses.forEach { (jobId: Long?, statuses: List<StreamStatusRead>) ->
      val job = jobIdToJobRead[jobId]!!.job
      val aggregatedStats = job.aggregatedStats
      val jobResult =
        JobSyncResultRead()
          .jobId(jobId)
          .configType(job.configType)
          .jobCreatedAt(job.createdAt)
          .jobUpdatedAt(job.updatedAt)
          .streamStatuses(statuses.map { streamStatus -> mapStreamStatusToSyncReadResult(streamStatus) })
          .bytesEmitted(aggregatedStats.bytesEmitted)!!
          .bytesCommitted(aggregatedStats.bytesCommitted)
          .recordsEmitted(aggregatedStats.recordsEmitted)
          .recordsCommitted(aggregatedStats.recordsCommitted)
          .recordsRejected(aggregatedStats.recordsRejected)
      result.add(jobResult)
    }

    return result
  }
}
