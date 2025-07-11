/*
 * Copyright (c) 2020-2025 Airbyte, Inc., all rights reserved.
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
import io.airbyte.persistence.job.JobPersistence
import io.airbyte.server.handlers.apidomainmapping.StreamStatusesMapper
import io.airbyte.server.repositories.StreamStatusesRepository
import jakarta.inject.Singleton
import java.io.IOException

/**
 * Interface layer between the API and Persistence layers.
 */
@Singleton
open class StreamStatusesHandler(
  val repo: StreamStatusesRepository,
  val mapper: StreamStatusesMapper,
  private val jobHistoryHandler: JobHistoryHandler,
  private val jobPersistence: JobPersistence,
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

    val page = repo.findAllFiltered(filters)

    val apiList =
      page.content
        .map { domain -> mapper.map(domain!!) }

    return StreamStatusReadList().streamStatuses(apiList)
  }

  fun listStreamStatusPerRunState(req: ConnectionIdRequestBody): StreamStatusReadList {
    val apiList =
      repo
        .findAllPerRunStateByConnectionId(req.connectionId)
        .map { domain -> mapper.map(domain) }

    return StreamStatusReadList().streamStatuses(apiList)
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
    val streamStatuses: List<StreamStatusRead> =
      repo
        .findLastAttemptsOfLastXJobsForConnection(req.connectionId, req.numberOfJobs)
        ?.map { domain -> mapper.map(domain!!) }
        ?: emptyList()

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
